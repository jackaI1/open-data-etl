"""Execute the exported metrics, and check the complete dashboard reference graph."""

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from dpd_etl.dashboard import publish

ROOT = Path(__file__).resolve().parents[1]


def load_script(path):
    spec = importlib.util.spec_from_file_location("dashboard_seed", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def seeded(tmp_path):
    database = tmp_path / "catalogue.db"
    load_script(ROOT / "scripts/seed_dashboard.py").seed(database)
    return database


def test_export_columns_metrics_and_graph_are_executable(seeded):
    assets = ROOT / "superset/assets"
    database = yaml.safe_load((assets / "databases/catalogue.yaml").read_text())
    assert database["sqlalchemy_uri"] == "sqlite:////app/catalogue/catalogue.db"
    datasets = {}
    expected_metrics = {
        "runs": {"products": 9, "flagged_products": 3, "flags": 3, "matches": 3, "duration": 10.5},
        "products": {"product_count": 9, "strength_flags": 3, "flagged_share": 1 / 3},
        "pairs": {"pair_count": 3, "suggested_matches": 3}}
    with sqlite3.connect(seeded) as con:
        for path in sorted((assets / "datasets").rglob("*.yaml")):
            ds = yaml.safe_load(path.read_text())
            assert ds["database_uuid"] == database["uuid"]
            datasets[ds["uuid"]] = ds
            columns = {r[1] for r in con.execute(f'PRAGMA table_info({ds["table_name"]})')}
            assert {c["column_name"] for c in ds["columns"]} == columns
            assert ds["main_dttm_col"] in columns
            for metric in ds["metrics"]:
                value = con.execute(
                    f'SELECT {metric["expression"]} FROM {ds["table_name"]}').fetchone()[0]
                assert value == pytest.approx(expected_metrics[ds["table_name"]][metric["metric_name"]])
        for path in (assets / "charts").glob("*.yaml"):
            chart = yaml.safe_load(path.read_text())
            ds = datasets[chart["dataset_uuid"]]
            metrics = {m["metric_name"] for m in ds["metrics"]}
            assert set(chart["params"]["metrics"]) <= metrics
            assert set(chart["params"]["groupby"]) <= {c["column_name"] for c in ds["columns"]}
    dashboard = yaml.safe_load((assets / "dashboards/catalogue.yaml").read_text())
    charts = {yaml.safe_load(p.read_text())["uuid"] for p in (assets / "charts").glob("*.yaml")}
    placed = {v["meta"]["uuid"] for v in dashboard["position"].values()
              if isinstance(v, dict) and v.get("type") == "CHART"}
    assert placed == charts and len(charts) == 11
    filters = dashboard["metadata"]["native_filter_configuration"]
    assert len(filters) == 3
    for filter_ in filters:
        for target in filter_["targets"]:
            if "datasetUuid" in target:
                assert target["column"]["name"] in {
                    c["column_name"] for c in datasets[target["datasetUuid"]]["columns"]}


def test_example_metrics_dates_filters_and_review_status(seeded):
    with sqlite3.connect(seeded) as con:
        assert con.execute("SELECT sum(rows_processed),sum(flagged_records),sum(duplicate_pairs),"
                           "avg(duration_seconds) FROM runs").fetchone() == (9, 3, 3, 10.5)
        assert con.execute("SELECT snapshot_date,flagged_strengths FROM runs ORDER BY snapshot_date"
                           ).fetchall() == [("2026-11-01", 2), ("2026-11-02", 1), ("2026-11-03", 0)]
        assert con.execute("SELECT 1.0*sum(flagged_record)/count(*) FROM products "
                           "WHERE snapshot_date='2026-11-01' AND drug_class='Human'"
                           ).fetchone()[0] == 1.0
        assert con.execute("SELECT review_status,count(*) FROM pairs GROUP BY review_status "
                           "ORDER BY review_status").fetchall() == [
                               ("confirmed", 1), ("dismissed", 1), ("pending", 1)]
        assert con.execute("SELECT DISTINCT source_kind FROM runs").fetchall() == [("example",)]
    load_script(ROOT / "scripts/seed_dashboard.py").seed(seeded)
    with sqlite3.connect(seeded) as con:
        assert con.execute("SELECT count(*) FROM products").fetchone()[0] == 9


def test_real_pipeline_output_round_trip(tmp_path):
    """CI creates this sample with real Spark before running the lightweight checks."""
    data = Path("data/fixture")
    if not (data / "reports/run_2026-11-01.json").exists():
        pytest.skip("make fixture SNAPSHOT_DATE=2026-11-01 first; required in CI")
    expected = publish(data, "2026-11-01", tmp_path / "real.db")
    with sqlite3.connect(tmp_path / "real.db") as con:
        assert con.execute("SELECT count(*) FROM products").fetchone()[0] == 60
        assert con.execute("SELECT sum(flagged_strengths) FROM products").fetchone()[0] == expected["flagged_strengths"]
        assert (con.execute("SELECT sum(is_match) FROM pairs").fetchone()[0] or 0) == expected["duplicate_pairs"]
        assert con.execute("SELECT duration_seconds FROM runs").fetchone()[0] > 0


def test_failed_receipt_does_not_replace_existing_date(seeded, tmp_path):
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports/run_2026-11-01.json").write_text(json.dumps({}))
    (tmp_path / "reports/dq_2026-11-01.json").write_text('[{"passed":false}]')
    with pytest.raises(ValueError, match="pass quality"):
        publish(tmp_path, "2026-11-01", seeded)
    with sqlite3.connect(seeded) as con:
        assert con.execute("SELECT count(*) FROM runs").fetchone()[0] == 3
