"""Real local Spark + Delta transactions; no mock storage or fake Delta writer."""

import csv
import json
import runpy
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

pytest.importorskip("delta", reason="install .[delta] for the Delta integration suite")

from delta.tables import DeltaTable

from dpd_etl.delta_job import (
    get_delta_spark,
    parameters,
    register_views,
    run_delta,
    write_snapshot,
)
from dpd_etl.quality import DataQualityError
from dpd_etl.transform import clean_ingredients, read_raw

DAY = "2026-11-01"
NEXT = "2026-11-02"
NOTEBOOK = Path(__file__).resolve().parents[2] / "notebooks/health_canada_delta.py"


@pytest.fixture(scope="module")
def delta_spark():
    spark = get_delta_spark()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def add_known_outlier(raw_dir):
    """Keep the public product keys, but inject a clearly labelled test strength group."""
    path = raw_dir / "ingred.txt"
    with path.open(newline="") as handle:
        rows = list(csv.reader(handle))
    used = set()
    for row in rows:
        if row[0] not in used and len(used) < 12:
            row[2] = "DELTA TEST INGREDIENT"
            row[4] = str(1000 if len(used) == 11 else 90 + 2 * len(used))
            row[5] = "MG"
            used.add(row[0])
    assert len(used) == 12
    with path.open("w", newline="") as handle:
        csv.writer(handle, quoting=csv.QUOTE_ALL).writerows(rows)


class Widgets:
    """Only the parameter transport is simulated; the notebook runs real Spark."""
    def __init__(self, values):
        self.values = values

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


def test_notebook_writes_real_delta_and_sql_survives_reruns(delta_spark, raw_dir, tmp_path):
    spark = delta_spark
    original = (raw_dir / "ingred.txt").read_text()
    add_known_outlier(raw_dir)
    root, reports = tmp_path / "delta", tmp_path / "reports"
    params = {
        "raw_dir": str(raw_dir), "delta_root": str(root), "snapshot_date": DAY,
        "report_dir": str(reports), "fixture": "true",
    }
    utils = type("Utilities", (), {"widgets": Widgets(params)})()
    state = runpy.run_path(str(NOTEBOOK), init_globals={"spark": spark, "dbutils": utils})
    assert state["summary"]["products"] == 60
    assert state["summary"]["flagged_strengths"] == 1
    assert not state["owns_spark"]  # Managed notebook must leave its supplied session alive.
    for table in ("drug_products", "strength_anomalies"):
        assert DeltaTable.isDeltaTable(spark, str(root / table))
        assert (root / table / "_delta_log" / "00000000000000000000.json").exists()
    products = spark.read.format("delta").load(str(root / "drug_products"))
    assert products.schema["snapshot_date"].dataType.simpleString() == "date"
    assert products.select("drug_code").distinct().count() == 60
    saved = spark.read.format("delta").load(str(root / "strength_anomalies"))
    expected = clean_ingredients(read_raw(spark, raw_dir, "ingred"))
    columns = expected.columns
    assert saved.select(columns).exceptAll(expected).count() == 0
    assert expected.exceptAll(saved.select(columns)).count() == 0
    flag = spark.table("flagged_strengths").first()
    assert flag.ingredient == "DELTA TEST INGREDIENT"
    assert flag.strength == "1000" and flag.peer_count == 12
    assert flag.brand_name and flag.robust_z > 3.5

    # Repeat the same date, append a date, then clear the old date's flags.
    kwargs = {key: value for key, value in params.items() if key != "fixture"}
    run_delta(spark, **kwargs, fixture=True)
    run_delta(spark, **{**kwargs, "snapshot_date": NEXT}, fixture=True)
    (raw_dir / "ingred.txt").write_text(original)
    run_delta(spark, **kwargs, fixture=True)
    register_views(spark, str(root))
    assert {str(r.snapshot_date): r["count"] for r in spark.table("dpd_drug_products")
            .groupBy("snapshot_date").count().collect()} == {DAY: 60, NEXT: 60}
    assert spark.table("flagged_strengths").where(f"snapshot_date = '{DAY}'").count() == 0
    assert spark.table("flagged_strengths").where(f"snapshot_date = '{NEXT}'").count() == 1
    history = DeltaTable.forPath(spark, str(root / "strength_anomalies")).history()
    assert history.count() == 4
    old = spark.read.format("delta").option("versionAsOf", 0).load(str(root / "strength_anomalies"))
    assert old.where("is_anomaly").count() == 1
    # Rebuild the SQL view in another session using only saved Delta tables.
    reader = spark.newSession()
    register_views(reader, str(root))
    assert reader.table("flagged_strengths").count() == 1
    summary = json.loads((reports / f"delta_{DAY}.json").read_text())
    assert summary["flagged_strengths"] == 0

    # A rejected rerun leaves the committed versions untouched.
    drug = raw_dir / "drug.txt"
    drug.write_text("".join(drug.read_text().splitlines(True)[:30]))
    assert (reports / f"delta_{NEXT}.json").exists()
    with pytest.raises(DataQualityError, match="row_drop_vs_previous_snapshot"):
        run_delta(spark, **{**kwargs, "snapshot_date": NEXT}, fixture=True)
    assert DeltaTable.forPath(spark, str(root / "drug_products")).history().count() == 4
    assert not (reports / f"delta_{NEXT}.json").exists()


def test_empty_replacement_clears_only_requested_date(delta_spark, tmp_path):
    path = str(tmp_path / "empty-delta")
    frame = delta_spark.createDataFrame([(1, date.fromisoformat(DAY))], "id int, snapshot_date date")
    write_snapshot(frame, path, DAY)
    write_snapshot(frame.withColumn("snapshot_date", F.lit(NEXT).cast("date")), path, NEXT)
    write_snapshot(frame.limit(0), path, DAY)
    rows = delta_spark.read.format("delta").load(path).collect()
    assert [(r.id, str(r.snapshot_date)) for r in rows] == [(1, NEXT)]
    with pytest.raises(Exception, match="replaceWhere|constraint|written out"):
        write_snapshot(frame, path, NEXT)  # Date mismatch must never erase a different date.
    assert delta_spark.read.format("delta").load(path).collect() == rows


def test_quality_failure_writes_report_but_no_delta(delta_spark, raw_dir, tmp_path):
    drug = raw_dir / "drug.txt"
    drug.write_text("".join(drug.read_text().splitlines(True)[:5]))
    with pytest.raises(DataQualityError):
        run_delta(delta_spark, raw_dir=str(raw_dir), delta_root=str(tmp_path / "delta"),
                  snapshot_date=DAY, report_dir=str(tmp_path / "reports"), fixture=True)
    assert not (tmp_path / "delta").exists()
    assert (tmp_path / "reports" / f"dq_{DAY}.json").exists()


@pytest.mark.parametrize("value", ["2026-02-30", "20261101", "2026-11-01' OR true"])
def test_bad_date_stops_before_spark(value):
    with pytest.raises(SystemExit) as error:
        parameters(["--snapshot-date", value])
    assert error.value.code == 2
