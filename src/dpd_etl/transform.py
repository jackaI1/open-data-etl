"""Transform: schema enforcement, cleaning, dedup, SCD2 status history, enriched dimension."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .schemas import SCHEMAS


def get_spark(app: str = "dpd-etl") -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName(app)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, raw_dir: Path, name: str) -> DataFrame:
    """Read one headerless DPD file with its enforced schema.

    FAILFAST: a row whose field count/quoting does not match the schema aborts the run
    instead of silently producing shifted columns.
    """
    return (
        spark.read.schema(SCHEMAS[name])
        .option("header", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .csv(str(Path(raw_dir) / f"{name}.txt"))
    )


def _clean_str(col: str) -> F.Column:
    """Trim; turn empty strings into NULL; normalise internal whitespace."""
    c = F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")
    return F.when(c == "", F.lit(None)).otherwise(c)


def _parse_dpd_date(col: str) -> F.Column:
    # DPD dates look like 22-MAR-2025; Spark's MMM parsing wants Title case.
    return F.to_date(F.initcap(F.lower(F.trim(F.col(col)))), "dd-MMM-yyyy")


def clean_drugs(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("drug_code").cast("int").alias("drug_code"),
        _clean_str("product_categorization").alias("product_categorization"),
        F.upper(_clean_str("class")).alias("drug_class"),
        _clean_str("din").alias("din"),
        F.upper(_clean_str("brand_name")).alias("brand_name"),
        _clean_str("descriptor").alias("descriptor"),
        (F.upper(F.trim(F.col("pediatric_flag"))) == "Y").alias("is_pediatric"),
        F.col("number_of_ais").cast("int").alias("number_of_ais"),
        _parse_dpd_date("last_update_date").alias("last_update_date"),
    ).where(F.col("drug_code").isNotNull())
    # Deduplicate: keep the most recently updated record per drug_code (ties: lowest DIN).
    w = Window.partitionBy("drug_code").orderBy(
        F.col("last_update_date").desc_nulls_last(),
        F.col("din").asc_nulls_last(),
        F.to_json(F.struct(*df.columns)).asc(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def clean_status(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("drug_code").cast("int").alias("drug_code"),
        F.upper(_clean_str("status")).alias("status"),
        _parse_dpd_date("history_date").alias("history_date"),
        (F.upper(F.trim(F.col("current_status_flag"))) == "Y").alias("source_is_current"),
    ).where(F.col("drug_code").isNotNull())
    return df.groupBy("drug_code", "status", "history_date").agg(
        F.max("source_is_current").alias("source_is_current")
    )


def build_status_scd2(status: DataFrame) -> DataFrame:
    """Slowly-changing (type 2) status history.

    Each status event becomes a version valid from its history_date until the next
    event for the same drug (valid_to NULL for the open version, is_current = true).
    """
    w = Window.partitionBy("drug_code").orderBy(
        F.col("history_date").asc_nulls_first(),
        F.col("source_is_current").asc_nulls_first(),
        F.col("status"),
    )
    per_drug = Window.partitionBy("drug_code")
    return (
        status.withColumn("valid_from", F.col("history_date"))
        .withColumn("valid_to", F.lead("history_date").over(w))
        .withColumn("is_current", F.row_number().over(w) == F.count("*").over(per_drug))
        .drop("history_date")
    )


def clean_companies(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("drug_code").cast("int").alias("drug_code"),
        _clean_str("company_code").alias("company_code"),
        F.upper(_clean_str("company_name")).alias("company_name"),
        F.upper(_clean_str("province")).alias("company_province"),
        (F.upper(F.trim(F.col("company_type"))) == "DIN_OWNER").alias("_is_owner"),
    ).where(F.col("drug_code").isNotNull())
    w = Window.partitionBy("drug_code").orderBy(
        F.col("_is_owner").desc_nulls_last(),
        F.col("company_name").asc_nulls_last(),
        F.col("company_code").asc_nulls_last(),
        F.col("company_province").asc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "_is_owner")


def clean_ingredients(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("drug_code").cast("int").alias("drug_code"),
        F.upper(_clean_str("ingredient")).alias("ingredient"),
        _clean_str("strength").alias("strength"),
        F.upper(_clean_str("strength_unit")).alias("strength_unit"),
    ).where(F.col("drug_code").isNotNull() & F.col("ingredient").isNotNull())
    return df.dropDuplicates(["drug_code", "ingredient", "strength", "strength_unit"])


def clean_atc(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("drug_code").cast("int").alias("drug_code"),
        F.upper(_clean_str("atc_number")).alias("atc_code"),
        F.upper(_clean_str("atc_name")).alias("atc_name"),
    ).where(F.col("drug_code").isNotNull())
    w = Window.partitionBy("drug_code").orderBy(
        F.col("atc_code").asc_nulls_last(), F.col("atc_name").asc_nulls_last()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def build_drug_products(
    drugs: DataFrame,
    status_scd2: DataFrame,
    companies: DataFrame,
    atc: DataFrame,
    ingredients: DataFrame,
    snapshot_date: str,
) -> DataFrame:
    """One row per drug product, enriched with current status, company, ATC group."""
    cur = status_scd2.where("is_current").select(
        "drug_code",
        F.col("status").alias("current_status"),
        F.col("valid_from").alias("current_status_since"),
    )
    first_approved = (
        status_scd2.where(F.col("status") == "APPROVED")
        .groupBy("drug_code")
        .agg(F.min("valid_from").alias("first_approved_date"))
    )
    ing = ingredients.groupBy("drug_code").agg(
        F.concat_ws("; ", F.sort_array(F.collect_set("ingredient"))).alias("active_ingredients")
    )
    return (
        drugs.join(cur, "drug_code", "left")
        .join(first_approved, "drug_code", "left")
        .join(companies, "drug_code", "left")
        .join(atc, "drug_code", "left")
        .join(ing, "drug_code", "left")
        .withColumn("atc_group", F.substring("atc_code", 1, 1))
        .withColumn("snapshot_date", F.lit(snapshot_date).cast("date"))
    )
