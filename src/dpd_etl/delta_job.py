"""A parameterized Delta Lake sink for the same checked Health Canada snapshot."""

from __future__ import annotations

import argparse
import json
from datetime import date
from importlib.resources import files
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import FIXTURE_THRESHOLDS, Thresholds
from .detection import detect_strength_anomalies
from .monitoring import Monitor
from .snapshot import checked_snapshot


def valid_date(value: str) -> str:
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError("snapshot_date must use YYYY-MM-DD")
    return value


def get_delta_spark() -> SparkSession:
    """Create a fresh local session; managed notebooks use their supplied session."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]").appName("dpd-delta-notebook")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # A small local job does not need Delta's default 50 snapshot tasks.
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def table_path(root: str, table: str) -> str:
    return f"{root.rstrip('/')}/{table}"


def write_snapshot(frame: DataFrame, path: str, snapshot_date: str) -> None:
    """Atomically replace just one date, including when its new result is empty."""
    valid_date(snapshot_date)
    (
        frame.write.format("delta").mode("overwrite")
        .option("partitionOverwriteMode", "static")
        .option("replaceWhere", f"snapshot_date = DATE '{snapshot_date}'")
        .partitionBy("snapshot_date").save(path)
    )


def previous_product_count(spark: SparkSession, root: str, snapshot_date: str) -> int | None:
    from delta.tables import DeltaTable

    path = table_path(root, "drug_products")
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    products = spark.read.format("delta").load(path)
    earlier = products.where(F.col("snapshot_date") < F.lit(snapshot_date).cast("date"))
    latest = earlier.agg(F.max("snapshot_date")).first()[0]
    return earlier.where(F.col("snapshot_date") == latest).count() if latest else None


def register_views(spark: SparkSession, delta_root: str) -> None:
    """Session SQL views can be rebuilt from saved Delta tables without source files."""
    for name in ("drug_products", "strength_anomalies"):
        spark.read.format("delta").load(table_path(delta_root, name)).createOrReplaceTempView(
            f"dpd_{name}"
        )
    sql = files("dpd_etl").joinpath("sql/flagged_strengths.sql").read_text()
    spark.sql(sql)


def run_delta(
    spark: SparkSession, *, raw_dir: str, delta_root: str, snapshot_date: str,
    report_dir: str, fixture: bool = False,
) -> dict:
    """Use a caller-owned session, so this also runs inside a Databricks notebook.

    Writes are atomic per table, not across tables. Serialize writers, and rerun
    the date if interrupted between writes before accepting its review results.
    """
    valid_date(snapshot_date)
    if not delta_root.strip():
        raise ValueError("delta_root cannot be empty")
    monitor = Monitor.from_env()
    reports = Path(report_dir)
    summary_path = reports / f"delta_{snapshot_date}.json"
    # A rejected rerun must not leave an old success marker for this date.
    summary_path.unlink(missing_ok=True)
    with monitor.command("delta"), checked_snapshot(
        spark, Path(raw_dir), snapshot_date, FIXTURE_THRESHOLDS if fixture else Thresholds(),
        previous_product_count(spark, delta_root, snapshot_date),
        reports / f"dq_{snapshot_date}.json", monitor,
    ) as (products, _, ingredients):
        strengths = detect_strength_anomalies(ingredients).withColumn(
            "snapshot_date", F.lit(snapshot_date).cast("date")
        ).cache()
        try:
            write_snapshot(products, table_path(delta_root, "drug_products"), snapshot_date)
            write_snapshot(strengths, table_path(delta_root, "strength_anomalies"), snapshot_date)
            register_views(spark, delta_root)
            summary = {
                "snapshot_date": snapshot_date, "products": products.count(),
                "ingredient_strengths": strengths.count(),
                "flagged_strengths": spark.table("flagged_strengths").where(
                    F.col("snapshot_date") == F.lit(snapshot_date).cast("date")
                ).count(),
                "delta_root": delta_root,
            }
            monitor.products(summary["products"])
            pending = summary_path.with_suffix(".json.tmp")
            pending.write_text(json.dumps(summary, indent=2) + "\n")
            pending.replace(summary_path)
            return summary
        finally:
            strengths.unpersist()


def parameters(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", default="data/raw/files")
    parser.add_argument("--delta-root", default="data/delta")
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--report-dir", default="data/reports")
    parser.add_argument("--fixture", action="store_true")
    args = parser.parse_args(argv)
    try:
        valid_date(args.snapshot_date)
    except ValueError:
        parser.error("--snapshot-date must be a valid YYYY-MM-DD date")
    return vars(args)
