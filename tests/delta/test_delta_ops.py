"""Real Delta MERGE, time travel, schema checks and raw-file bronze on local Spark."""

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("delta.tables", reason="install .[delta] for real Delta integration tests")

from pyspark.sql import functions as F

from dpd_etl.delta_job import get_delta_spark, table_path
from dpd_etl.delta_ops import (
    CURRENT_TABLE,
    assert_same_schema,
    check_release_order,
    compare_versions,
    load_raw_files,
    merge_release,
    table_versions,
)
from dpd_etl.lakehouse import run_lakehouse

DAY1, DAY2 = "2026-11-01", "2026-11-02"
RAW_FILES = 7


@pytest.fixture(scope="module")
def spark():
    session = get_delta_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def release(spark, rows):
    return spark.createDataFrame(rows, "drug_code int, brand_name string, dosage_form string")


BASE = [(1, "ALPHA", "TABLET"), (2, "BETA", "CAPSULE"), (3, "GAMMA", "TABLET")]


def test_merge_touches_only_changed_rows_and_time_travel_compares_releases(spark, tmp_path):
    root = str(tmp_path / "delta")
    first = merge_release(spark, root, release(spark, BASE), DAY1)
    assert first == {"num_inserted_rows": 3, "num_updated_rows": 0, "num_deactivated_rows": 0}
    # Release two: product 2 changed, product 3 was dropped, product 4 is new.
    second_rows = [(1, "ALPHA", "TABLET"), (2, "BETA PLUS", "CAPSULE"), (4, "DELTA", "TABLET")]
    second = merge_release(spark, root, release(spark, second_rows), DAY2)
    assert second["num_inserted_rows"] == 1
    assert second["num_updated_rows"] == 1
    assert second["num_deactivated_rows"] == 1
    assert table_versions(spark, root) == [0, 1]
    diff = compare_versions(spark, root, 0, 1)
    assert diff == {"old_version": 0, "new_version": 1, "added": 1, "removed": 0,
                    "changed": 1, "deactivated": 1}
    # Product 3 stays in the table as inactive, so its history is still queryable.
    current = spark.read.format("delta").load(table_path(root, CURRENT_TABLE))
    assert current.where("drug_code = 3").first().active is False
    assert current.where("active").count() == 3
    # Rerunning the same release changes nothing and writes no new rows.
    again = merge_release(spark, root, release(spark, second_rows), DAY2)
    assert again["num_inserted_rows"] == 0 and again["num_updated_rows"] == 0
    assert again["num_deactivated_rows"] == 0
    assert current.count() == spark.read.format("delta").load(table_path(root, CURRENT_TABLE)).count()


def test_changed_column_is_rejected_and_table_keeps_its_version(spark, tmp_path):
    root = str(tmp_path / "delta")
    merge_release(spark, root, release(spark, BASE), DAY1)
    versions_before = table_versions(spark, root)
    retyped = spark.createDataFrame(
        [(1, 100, "TABLET")], "drug_code int, brand_name int, dosage_form string"
    )
    with pytest.raises(ValueError, match="retyped=.*brand_name"):
        merge_release(spark, root, retyped, DAY2)
    renamed = spark.createDataFrame([(1, "A", "T")], "drug_code int, brand string, dosage_form string")
    with pytest.raises(ValueError, match="removed=.*brand_name"):
        assert_same_schema(spark, table_path(root, CURRENT_TABLE), renamed,
                           ignore=("row_hash", "active", "first_snapshot_date", "last_changed_snapshot_date"))
    assert table_versions(spark, root) == versions_before


def test_raw_files_are_kept_byte_for_byte_with_checksums(spark, tmp_path):
    root = str(tmp_path / "delta")
    raw = Path(__file__).resolve().parents[1] / "fixtures" / "raw"
    assert load_raw_files(spark, raw, root, DAY1) == RAW_FILES
    rows = spark.read.format("delta").load(table_path(root, "bronze_raw_files")).orderBy("file_name").collect()
    assert len(rows) == RAW_FILES
    drug = next(r for r in rows if r.file_name == "drug.txt")
    assert drug.content == (raw / "drug.txt").read_bytes()
    assert drug.sha256 == hashlib.sha256((raw / "drug.txt").read_bytes()).hexdigest()
    assert drug.bytes == len(drug.content)
    assert drug.loaded_at is not None


def test_older_release_cannot_replace_newer_one(spark, tmp_path):
    root = str(tmp_path / "delta")
    raw = Path(__file__).resolve().parents[1] / "fixtures" / "raw"
    load_raw_files(spark, raw, root, DAY2)
    check_release_order(spark, root, DAY2)  # the same date is allowed
    with pytest.raises(ValueError, match="older than loaded release"):
        check_release_order(spark, root, DAY1)


def test_lakehouse_run_merges_current_products_and_rerun_is_a_no_op(spark, raw_dir, tmp_path):
    args = {"raw_dir": str(raw_dir), "delta_root": str(tmp_path / "delta"), "snapshot_date": DAY1,
            "report_dir": str(tmp_path / "reports"), "fixture": True, "as_of": date(2026, 11, 2)}
    run_lakehouse(spark, **args)
    current = spark.read.format("delta").load(table_path(args["delta_root"], CURRENT_TABLE))
    assert current.count() == 60 and current.where("active").count() == 60
    first = json.loads((tmp_path / "reports" / f"merge_{DAY1}.json").read_text())
    assert first["num_inserted_rows"] == 60
    run_lakehouse(spark, **args)
    second = json.loads((tmp_path / "reports" / f"merge_{DAY1}.json").read_text())
    assert second["num_inserted_rows"] == 0 and second["num_updated_rows"] == 0
    bronze = spark.read.format("delta").load(table_path(args["delta_root"], "bronze_raw_files"))
    assert bronze.where(F.col("snapshot_date") == F.lit(DAY1).cast("date")).count() == RAW_FILES


def test_command_line_lists_versions_and_compares_them(spark, tmp_path, capsys):
    from dpd_etl.delta_ops import main as ops_main

    root = str(tmp_path / "delta")
    merge_release(spark, root, release(spark, BASE), DAY1)
    merge_release(spark, root, release(spark, [(1, "ALPHA", "TABLET"), (2, "BETA PLUS", "CAPSULE")]), DAY2)
    assert ops_main(["--delta-root", root, "versions"], spark=spark) == 0
    assert json.loads(capsys.readouterr().out) == [0, 1]
    assert ops_main(["--delta-root", root, "compare", "--old-version", "0", "--new-version", "1"],
                    spark=spark) == 0
    diff = json.loads(capsys.readouterr().out)
    assert diff["changed"] == 1 and diff["deactivated"] == 1 and diff["added"] == 0
    # The caller's session stays open after the command finishes.
    assert spark.range(1).count() == 1
