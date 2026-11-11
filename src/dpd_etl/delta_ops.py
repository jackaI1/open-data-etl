"""Monthly Delta operations: keep the raw files, merge each release, compare versions.

Each release is merged into one `current_products` table keyed by drug code, so a
monthly run changes only the rows that changed. Delta keeps every committed version,
so two releases can be compared with `versionAsOf` instead of reprocessing files.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .delta_job import table_path, valid_date

CURRENT_TABLE = "current_products"
BRONZE_FILES_TABLE = "bronze_raw_files"
KEY = "drug_code"
META_COLUMNS = ("row_hash", "active", "first_snapshot_date", "last_changed_snapshot_date")


def _delta_table(spark: SparkSession, path: str):
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, path) if DeltaTable.isDeltaTable(spark, path) else None


def _schema_map(frame: DataFrame) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in frame.schema.fields}


def assert_same_schema(spark: SparkSession, path: str, incoming: DataFrame, ignore=()) -> None:
    """Reject a release whose columns were added, removed or retyped.

    Delta already blocks some incompatible writes, but it does not explain them.
    This check names the column so an unexpected upstream change is easy to find.
    """
    table = _delta_table(spark, path)
    if table is None:
        return
    expected = {k: v for k, v in _schema_map(table.toDF()).items() if k not in ignore}
    actual = {k: v for k, v in _schema_map(incoming).items() if k not in ignore}
    added = sorted(set(actual) - set(expected))
    removed = sorted(set(expected) - set(actual))
    changed = sorted(k for k in set(actual) & set(expected) if actual[k] != expected[k])
    if added or removed or changed:
        raise ValueError(
            f"schema changed for {path}: added={added} removed={removed} "
            f"retyped={[f'{k} {expected[k]} -> {actual[k]}' for k in changed]}"
        )


def check_release_order(spark: SparkSession, delta_root: str, snapshot_date: str) -> None:
    """Refuse to merge an older release over a newer one already loaded."""
    valid_date(snapshot_date)
    path = table_path(delta_root, BRONZE_FILES_TABLE)
    if _delta_table(spark, path) is None:
        return
    latest = spark.read.format("delta").load(path).agg(F.max("snapshot_date")).first()[0]
    if latest and snapshot_date < latest.isoformat():
        raise ValueError(
            f"release {snapshot_date} is older than loaded release {latest}; "
            "rerun the newest release or compare versions with versionAsOf"
        )


def load_raw_files(spark: SparkSession, raw_dir: Path, delta_root: str, snapshot_date: str) -> int:
    """Keep each downloaded file byte for byte, with its checksum and load time."""
    valid_date(snapshot_date)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for file in sorted(Path(raw_dir).glob("*.txt")):
        content = file.read_bytes()
        rows.append((
            snapshot_date, file.name, hashlib.sha256(content).hexdigest(), len(content),
            content.count(b"\n"), loaded_at, content,
        ))
    schema = StructType([
        StructField("snapshot_date", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("sha256", StringType(), False),
        StructField("bytes", LongType(), False),
        StructField("line_count", LongType(), False),
        StructField("loaded_at", TimestampType(), False),
        StructField("content", BinaryType(), False),
    ])
    frame = spark.createDataFrame(rows, schema).withColumn(
        "snapshot_date", F.col("snapshot_date").cast("date")
    )
    from .delta_job import write_snapshot

    write_snapshot(frame, table_path(delta_root, BRONZE_FILES_TABLE), snapshot_date)
    return len(rows)


def merge_release(spark: SparkSession, delta_root: str, products: DataFrame, snapshot_date: str) -> dict:
    """MERGE one release into `current_products`, touching only changed rows.

    A product whose contents changed is updated. A new product is inserted. A product
    missing from the release is kept but marked inactive, so history stays queryable.
    Returns the counts Delta reports for this commit.
    """
    from delta.tables import DeltaTable

    valid_date(snapshot_date)
    path = table_path(delta_root, CURRENT_TABLE)
    day = F.lit(snapshot_date).cast("date")
    payload = [c for c in products.columns if c not in ("snapshot_date", *META_COLUMNS)]
    hash_input = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in payload]
    source = (
        products.select(*payload)
        .withColumn("row_hash", F.sha2(F.concat_ws("\u001f", *hash_input), 256))
        .withColumn("active", F.lit(True))
        .withColumn("first_snapshot_date", day)
        .withColumn("last_changed_snapshot_date", day)
    )
    if _delta_table(spark, path) is None:
        source.write.format("delta").mode("overwrite").save(path)
        return {"num_inserted_rows": source.count(), "num_updated_rows": 0,
                "num_deactivated_rows": 0}
    assert_same_schema(spark, path, source, ignore=META_COLUMNS)
    table = DeltaTable.forPath(spark, path)
    # Active products the release no longer lists; the merge below marks these inactive.
    dropped = table.toDF().where(F.col("active")).join(
        source.select(KEY), KEY, "left_anti"
    ).count()
    columns = [c for c in source.columns if c not in ("first_snapshot_date", KEY)]
    changed_set = {c: f"s.{c}" for c in columns}
    (
        table.alias("t")
        .merge(source.alias("s"), f"t.{KEY} = s.{KEY}")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash OR t.active = false",
            set={**changed_set, "active": "true"},
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(
            condition="t.active = true",
            set={"active": "false", "last_changed_snapshot_date": f"DATE '{snapshot_date}'"},
        )
        .execute()
    )
    # Delta 3.x returns nothing from execute(); the commit's counts are in table history.
    metrics = table.history(1).select("operationMetrics").first()[0]
    return {"num_inserted_rows": int(metrics.get("numTargetRowsInserted", 0)),
            "num_updated_rows": int(metrics.get("numTargetRowsMatchedUpdated", 0)),
            "num_deactivated_rows": dropped}


def compare_versions(spark: SparkSession, delta_root: str, old_version: int, new_version: int) -> dict:
    """Compare two committed versions of the current catalogue with Delta time travel."""
    path = table_path(delta_root, CURRENT_TABLE)
    old = spark.read.format("delta").option("versionAsOf", old_version).load(path)
    new = spark.read.format("delta").option("versionAsOf", new_version).load(path)
    o = old.select(KEY, "row_hash", "active").withColumnRenamed("row_hash", "old_hash").withColumnRenamed("active", "old_active")
    n = new.select(KEY, "row_hash", "active").withColumnRenamed("row_hash", "new_hash").withColumnRenamed("active", "new_active")
    joined = o.join(n, KEY, "full_outer")
    return {
        "old_version": old_version,
        "new_version": new_version,
        "added": joined.where(F.col("old_hash").isNull()).count(),
        "removed": joined.where(F.col("new_hash").isNull()).count(),
        "changed": joined.where(
            F.col("old_hash").isNotNull() & F.col("new_hash").isNotNull()
            & (F.col("old_hash") != F.col("new_hash"))
        ).count(),
        "deactivated": joined.where(F.col("old_active") & ~F.col("new_active")).count(),
    }


def table_versions(spark: SparkSession, delta_root: str) -> list[int]:
    """Versions of the current table, oldest first, for choosing a comparison."""
    table = _delta_table(spark, table_path(delta_root, CURRENT_TABLE))
    if table is None:
        return []
    return sorted(int(r.version) for r in table.history().select("version").collect())


def main(argv: list[str] | None = None, spark: SparkSession | None = None) -> int:
    """Command line for version comparison: list saved versions, or diff two of them.

    A caller that already has a session passes it in and keeps ownership of it.
    """
    import argparse
    import json

    from .delta_job import get_delta_spark

    parser = argparse.ArgumentParser(description="Compare two saved versions of current_products.")
    parser.add_argument("--delta-root", required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("versions", help="list the saved versions, oldest first")
    cmp_parser = sub.add_parser("compare", help="count changes between two versions")
    cmp_parser.add_argument("--old-version", type=int, required=True)
    cmp_parser.add_argument("--new-version", type=int, required=True)
    args = parser.parse_args(argv)
    owned = spark is None
    spark = spark or get_delta_spark()
    try:
        if args.command == "versions":
            print(json.dumps(table_versions(spark, args.delta_root)))
        else:
            diff = compare_versions(spark, args.delta_root, args.old_version, args.new_version)
            print(json.dumps(diff, indent=2))
    finally:
        if owned:
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
