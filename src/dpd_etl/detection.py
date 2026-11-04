"""Non-destructive duplicate review and ingredient-strength anomaly detection.

All pair generation and scoring use Spark expressions, with no driver-side catalogue
collection or Python UDF. These outputs suggest records to review, never medical advice.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LINK_THRESHOLD = 0.90
MAX_BLOCK_SIZE = 200
MIN_PEERS = 8
Z_THRESHOLD = 3.5


def _name(column):
    return F.regexp_replace(F.upper(F.coalesce(column, F.lit(""))), "[^A-Z0-9]", "")


def product_context(ingredients: DataFrame, forms: DataFrame, routes: DataFrame) -> DataFrame:
    """Require complete ingredient/strength, form and route sets for fuzzy linkage."""
    complete = F.lit(True)
    fields = ["ingredient", "strength", "strength_unit"]
    for field in fields:
        complete = complete & F.col(field).isNotNull() & (F.trim(F.col(field)) != "")
    ing = ingredients.groupBy("drug_code").agg(
        F.sort_array(F.collect_set(F.to_json(F.struct(*fields)))).alias("ingredients"),
        F.min(complete.cast("int")).alias("complete"),
    )
    ing = ing.withColumn("complete", F.col("complete") == 1)
    for source, column in [(forms, "pharm_form_code"), (routes, "route_code")]:
        part = source.groupBy("drug_code").agg(
            F.sort_array(F.collect_set(F.trim(column))).alias(column),
            F.min((F.col(column).isNotNull() & (F.trim(column) != "")).cast("int")).alias(
                "_part_complete"
            ),
        )
        ing = ing.join(part, "drug_code", "left")
        ing = ing.withColumn(
            "complete",
            F.col("complete") & (F.size(column) > 0) & (F.col("_part_complete") == 1),
        ).drop("_part_complete")
    return ing.select(
        "drug_code",
        F.when(
            F.col("complete"), F.to_json(F.struct("ingredients", "pharm_form_code", "route_code"))
        ).alias("context"),
    )


def link_products(products: DataFrame, context: DataFrame, max_block_size=MAX_BLOCK_SIZE):
    """Return scored candidate pairs and block diagnostics; distinct DINs never link.

    Blocking restricts comparisons to a shared valid DIN or complete product context
    plus a three-character brand prefix. Overlarge blocks are reported and skipped.
    """
    if max_block_size < 2:
        raise ValueError("max_block_size must be at least 2")
    records = (
        products.join(context, "drug_code", "left")
        .select(
            "drug_code",
            "drug_class",
            "context",
            _name(F.col("brand_name")).alias("brand"),
            _name(F.col("company_name")).alias("company"),
            F.when(F.col("din").rlike(r"^[0-9]{8}$"), F.col("din")).alias("din"),
        )
        .withColumn(
            "profile",
            F.when(
                F.col("context").isNotNull() & F.col("drug_class").isNotNull(),
                F.sha2(F.to_json(F.struct("context", "drug_class")), 256),
            ),
        )
    )
    members = records.select(
        "drug_code",
        F.explode(
            F.array(
                F.when(F.col("din").isNotNull(), F.concat(F.lit("din:"), "din")),
                F.when(
                    F.col("profile").isNotNull() & (F.length("brand") >= 3),
                    F.concat(F.lit("profile:"), "profile", F.lit(":"), F.substring("brand", 1, 3)),
                ),
            )
        ).alias("block_key"),
    ).where("block_key is not null")
    blocks = members.groupBy("block_key").count().withColumnRenamed("count", "block_size")
    blocks = blocks.withColumn("skipped", F.col("block_size") > max_block_size)
    eligible = members.join(blocks.where("not skipped"), "block_key").select(
        "block_key", "drug_code"
    )
    pairs = (
        eligible.alias("a")
        .join(
            eligible.alias("b"),
            (F.col("a.block_key") == F.col("b.block_key"))
            & (F.col("a.drug_code") < F.col("b.drug_code")),
        )
        .select(F.col("a.drug_code").alias("left_id"), F.col("b.drug_code").alias("right_id"))
        .distinct()
    )
    for side in ["left", "right"]:
        renamed = records.select(
            *[F.col(c).alias(f"{side}_{'id' if c == 'drug_code' else c}") for c in records.columns]
        )
        pairs = pairs.join(renamed, f"{side}_id")
    for field in ["brand", "company"]:
        a, b = F.col(f"left_{field}"), F.col(f"right_{field}")
        pairs = pairs.withColumn(
            f"{field}_similarity",
            F.when(
                (F.length(a) > 0) & (F.length(b) > 0),
                1 - F.levenshtein(a, b) / F.greatest(F.length(a), F.length(b)),
            ).otherwise(0.0),
        )
    same_din = F.coalesce(F.col("left_din") == F.col("right_din"), F.lit(False))
    conflict = F.coalesce(F.col("left_din") != F.col("right_din"), F.lit(False))
    same_profile = F.coalesce(F.col("left_profile") == F.col("right_profile"), F.lit(False))
    pairs = pairs.withColumn(
        "score",
        F.when(same_din, 1.0).otherwise(
            0.8 * F.col("brand_similarity") + 0.2 * F.col("company_similarity")
        ),
    )
    fuzzy = (
        same_profile & (F.col("score") >= LINK_THRESHOLD) & (F.col("company_similarity") >= 0.85)
    )
    return pairs.select(
        "left_id",
        "right_id",
        "left_din",
        "right_din",
        "brand_similarity",
        "company_similarity",
        "score",
        (same_din | (~conflict & fuzzy)).alias("is_match"),
        F.when(same_din, "exact_din")
        .when(conflict, "conflicting_din")
        .when(fuzzy, "fuzzy_match")
        .otherwise("insufficient_similarity")
        .alias("reason"),
    ), blocks


def detect_strength_anomalies(ingredients: DataFrame) -> DataFrame:
    """Modified z-scores within ingredient/unit peers; unsupported values stay visible."""
    keys = ["ingredient", "strength_unit"]
    numeric = F.when(
        F.col("strength").rlike(r"^[+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$"),
        F.col("strength").cast("double"),
    )
    base = ingredients.withColumn("numeric_strength", numeric)
    valid = base.where(
        (F.col("numeric_strength") > 0)
        & (F.col("numeric_strength") < float("inf"))
        & F.col("ingredient").isNotNull()
        & F.col("strength_unit").isNotNull()
        & (F.trim("strength_unit") != "")
    )
    stats = valid.groupBy(*keys).agg(
        F.countDistinct("drug_code").alias("peer_count"),
        F.median("numeric_strength").alias("peer_median"),
    )
    deviations = valid.join(stats, keys).withColumn(
        "deviation", F.abs(F.col("numeric_strength") - F.col("peer_median"))
    )
    mad = deviations.groupBy(*keys).agg(F.median("deviation").alias("peer_mad"))
    result = base.join(stats.join(mad, keys), keys, "left")
    supported = (F.col("numeric_strength") > 0) & (F.col("numeric_strength") < float("inf"))
    result = result.withColumn(
        "reason",
        F.when(~F.coalesce(supported, F.lit(False)), "unsupported_strength")
        .when(F.col("peer_count").isNull() | (F.col("peer_count") < MIN_PEERS), "too_few_peers")
        .when(F.col("peer_mad") == 0, "zero_mad")
        .otherwise("scored"),
    )
    return (
        result.withColumn(
            "robust_z",
            F.when(
                F.col("reason") == "scored",
                0.6745 * (F.col("numeric_strength") - F.col("peer_median")) / F.col("peer_mad"),
            ),
        )
        .withColumn("is_evaluated", F.col("reason") == "scored")
        .withColumn("is_anomaly", F.coalesce(F.abs("robust_z") > Z_THRESHOLD, F.lit(False)))
    )
