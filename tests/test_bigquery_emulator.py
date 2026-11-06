"""Run the shipped SQL and SDK uploads against goccy/bigquery-emulator v0.8.1.

The emulator lacks partition decorators. This TEST-ONLY adapter implements their
replacement effect with DELETE + an SDK append load; production never uses it.
Production WRITE_TRUNCATE configuration is asserted in test_bigquery.py.
"""

import copy
import os
import socket
import subprocess
import time
from pathlib import Path

import duckdb
import pytest
from google.api_core.exceptions import Conflict, NotFound
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery as bq
from test_bigquery import DAY, PROJECT, write_partition

from dpd_etl.bigquery import FIELDS, load_snapshot
from dpd_etl.bigquery_review import review_query


@pytest.fixture(scope="module")
def emulator(tmp_path_factory):
    binary = os.environ.get("BIGQUERY_EMULATOR_BIN")
    if not binary:
        pytest.skip("set BIGQUERY_EMULATOR_BIN; CI always runs this integration suite")
    with socket.socket() as reserve, socket.socket() as grpc_reserve:
        reserve.bind(("127.0.0.1", 0))
        grpc_reserve.bind(("127.0.0.1", 0))
        port, grpc_port = reserve.getsockname()[1], grpc_reserve.getsockname()[1]
    log = tmp_path_factory.mktemp("bq-server") / "emulator.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            [binary, f"--project={PROJECT}", "--host=127.0.0.1", f"--port={port}",
             f"--grpc-port={grpc_port}"], stdout=output, stderr=output,
            env=dict(os.environ, TZ="UTC"),
        )
        print(f"Owned emulator pid={process.pid} started={time.time()} "
              f"ports={port},{grpc_port}; cleanup=kill -TERM {process.pid}, then wait")
        try:
            for _ in range(200):
                if process.poll() is not None:
                    pytest.fail(f"emulator exited: {log.read_text()[-2000:]}")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        break
                except OSError:
                    time.sleep(.05)
            else:
                pytest.fail("emulator did not become ready")
            with bq.Client(project=PROJECT, credentials=AnonymousCredentials(),
                           client_options={"api_endpoint": f"http://127.0.0.1:{port}"}) as client:
                yield PartitionAdapter(client)
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            assert process.poll() is not None
            for owned_port in (port, grpc_port):
                with socket.socket() as check:
                    assert check.connect_ex(("127.0.0.1", owned_port)) != 0
            print(f"Stopped emulator pid={process.pid}; "
                  f"verified ports={port},{grpc_port} closed")


class PartitionAdapter:
    def __init__(self, client):
        self.client = client

    def __getattr__(self, key):
        return getattr(self.client, key)

    def query(self, sql, *, job_config=None, **kwargs):
        if job_config is not None:
            # v0.8.1's Go decoder requires REST parameter values to be strings.
            # Preserve the SQL and declared BigQuery types; normalize only wire JSON.
            api = job_config.to_api_repr()
            for parameter in api.get("query", {}).get("queryParameters", []):
                value = parameter["parameterValue"].get("value")
                if isinstance(value, (int, float)):
                    parameter["parameterValue"]["value"] = str(value)
            job_config = bq.QueryJobConfig.from_api_repr(api)
        return self.client.query(sql, job_config=job_config, retry=None, timeout=30, **kwargs)

    def create_dataset(self, dataset, *, exists_ok=False):
        # v0.8.1 reports existing resources as HTTP 500 instead of 409.
        try:
            actual = self.client.get_dataset(dataset)
        except NotFound:
            return self.client.create_dataset(dataset)
        if not exists_ok:
            raise Conflict("dataset already exists")
        return actual

    def create_table(self, table, *, exists_ok=False):
        try:
            actual = self.client.get_table(table)
        except NotFound:
            return self.client.create_table(table)
        if not exists_ok:
            raise Conflict("table already exists")
        return actual

    def update_table(self, table, fields):
        # PATCH updates view metadata but not its executable definition in v0.8.1.
        assert fields == ["view_query", "view_use_legacy_sql"]
        self.client.delete_table(table)
        return self.client.create_table(table)

    def load_table_from_file(self, file, destination, *, job_config, **kwargs):
        base, day = destination.split("$")
        snapshot = f"{day[:4]}-{day[4:6]}-{day[6:]}"
        self.client.query(
            f"DELETE FROM `{base}` WHERE snapshot_date = DATE '{snapshot}'"
        ).result(timeout=30)
        config = copy.deepcopy(job_config)
        config.write_disposition = "WRITE_APPEND"
        job = self.client.load_table_from_file(file, base, job_config=config, **kwargs)
        job.result(timeout=30)
        # v0.8.1 uploads ISO DATE strings without the engine's internal DATE encoding.
        # SQL writes normalize them. Touch only this job's newly inserted generation.
        self.client.query(
            f"UPDATE `{base}` SET snapshot_date = DATE '{snapshot}' "
            f"WHERE CAST(snapshot_date AS STRING) = '{snapshot}'"
        ).result(timeout=30)
        return job


def view_rows(client, dataset, snapshot=DAY):
    return [dict(row) for row in client.query(
        f"SELECT * FROM `{PROJECT}.{dataset}.reviewer_summary_{snapshot.replace('-', '')}` "
        "ORDER BY drug_code"
    ).result(timeout=30)]


def test_real_pipeline_snapshot_upload_rerun_and_history(emulator, tmp_path):
    from dpd_etl.config import FIXTURE_THRESHOLDS
    from dpd_etl.pipeline import run

    run(Path("tests/fixtures/raw"), tmp_path, DAY, FIXTURE_THRESHOLDS)
    root = tmp_path / "parquet"
    result = load_snapshot(emulator, root, PROJECT, "pipeline", DAY)
    rows = view_rows(emulator, "pipeline")
    assert len(rows) == 60, [dict(r) for r in emulator.query(
        f"SELECT drug_code, snapshot_date, load_id FROM `{PROJECT}.pipeline.drug_products` "
        "LIMIT 2").result()]
    assert result["rows"]["drug_products"] == 60
    from dpd_etl.bigquery_health import health_checks
    from dpd_etl.summary import accepted_summary
    expected_summary = accepted_summary(tmp_path, DAY)
    actual = health_checks(emulator, PROJECT, "pipeline", DAY, expected=expected_summary)
    assert actual["rows_processed"] == 60
    assert actual["flagged_strengths"] == expected_summary["flagged_strengths"]
    with duckdb.connect() as con:
        expected = con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE is_anomaly",
            [str(root / "strength_anomalies" / f"snapshot_date={DAY}" / "*.parquet")],
        ).fetchone()[0]
    assert sum(row["flagged_strengths"] for row in rows) == expected
    load_snapshot(emulator, root, PROJECT, "pipeline", DAY)
    assert view_rows(emulator, "pipeline") == rows
    # Reuse the pipeline's cleaned output for a second dated snapshot.
    import shutil
    for table in FIELDS:
        shutil.copytree(root / table / f"snapshot_date={DAY}",
                        root / table / "snapshot_date=2026-11-08")
    load_snapshot(emulator, root, PROJECT, "pipeline", "2026-11-08")
    assert len(view_rows(emulator, "pipeline", "2026-11-08")) == 60
    load_snapshot(emulator, root, PROJECT, "pipeline", DAY)
    assert view_rows(emulator, "pipeline") == rows
    assert len(view_rows(emulator, "pipeline", "2026-11-08")) == 60
    review = review_query(emulator, PROJECT, "pipeline", DAY)
    assert all(r["flagged_strengths"] or r["unscored_strengths"] for r in review["rows"])


def test_multiple_children_nulls_empty_flags_and_corrected_rerun(emulator, tmp_path):
    products = [{"drug_code": n, "din": f"{n:08d}", "brand_name": f"SAMPLE {n}"}
                for n in (1, 2)]
    write_partition(tmp_path, "drug_products", products)
    write_partition(tmp_path, "drug_ingredients", [
        {"drug_code": 1, "ingredient": "A", "strength": "5", "strength_unit": "MG"},
        {"drug_code": 1, "ingredient": "B", "strength": "10", "strength_unit": "MG"},
    ])
    write_partition(tmp_path, "strength_anomalies", [
        {"drug_code": 1, "ingredient": "A", "is_anomaly": True, "is_evaluated": True},
        {"drug_code": 1, "ingredient": "B", "is_anomaly": False, "is_evaluated": False},
    ])
    write_partition(tmp_path, "product_links", [])
    load_snapshot(emulator, tmp_path, PROJECT, "edge_cases", DAY)
    rows = view_rows(emulator, "edge_cases")
    assert rows[0]["ingredient_rows"] == 2
    assert rows[0]["flagged_strengths"] == 1  # A raw child join would produce 2.
    assert rows[0]["unscored_strengths"] == 1
    assert rows[0]["din"] == "00000001"
    assert rows[1]["ingredient_rows"] == rows[1]["flagged_strengths"] == 0
    assert rows[1]["company_name"] is None
    class FailIngredients:
        def __getattr__(self, name):
            return getattr(emulator, name)

        def load_table_from_file(self, file, target, **kw):
            if ".drug_ingredients$" in target:
                raise RuntimeError("interrupted upload")
            return emulator.load_table_from_file(file, target, **kw)

    with pytest.raises(RuntimeError, match="interrupted"):
        load_snapshot(FailIngredients(), tmp_path, PROJECT, "edge_cases", DAY)
    # The older view cannot mix the newly loaded products with older flags.
    assert view_rows(emulator, "edge_cases") == []
    load_snapshot(emulator, tmp_path, PROJECT, "edge_cases", DAY)
    assert view_rows(emulator, "edge_cases") == rows
    # An empty replacement must clear the previous flags for this date.
    write_partition(tmp_path, "strength_anomalies", [])
    load_snapshot(emulator, tmp_path, PROJECT, "edge_cases", DAY)
    cleared = view_rows(emulator, "edge_cases")
    assert len(cleared) == 2
    assert sum(row["flagged_strengths"] for row in cleared) == 0
    # Restore the original inputs after a correction. Jobs must not replay old IDs.
    write_partition(tmp_path, "strength_anomalies", [
        {"drug_code": 1, "ingredient": "A", "is_anomaly": True, "is_evaluated": True},
    ])
    load_snapshot(emulator, tmp_path, PROJECT, "edge_cases", DAY)
    assert view_rows(emulator, "edge_cases")[0]["flagged_strengths"] == 1


def test_health_parameter_date_flags_null_rates_pairs_and_isolation(emulator, tmp_path):
    from dpd_etl.bigquery_health import health_checks

    write_partition(tmp_path, "drug_products", [
        {"drug_code": 1, "din": "00000001", "brand_name": "A", "company_name": "Maker"},
        {"drug_code": 2, "din": None, "brand_name": None, "company_name": None}])
    write_partition(tmp_path, "drug_ingredients", [{"drug_code": 1}, {"drug_code": 1}])
    write_partition(tmp_path, "strength_anomalies", [
        {"drug_code": 1, "is_anomaly": True, "is_evaluated": True,
         "numeric_strength": 100, "peer_median": 10, "peer_mad": 1,
         "peer_count": 10, "robust_z": 60.705},
        {"drug_code": 1, "is_anomaly": False, "is_evaluated": False}])
    write_partition(tmp_path, "product_links", [
        {"left_id": 1, "right_id": 2, "is_match": True, "reason": "exact_din"}])
    load_snapshot(emulator, tmp_path, PROJECT, "health_edges", DAY)
    actual = health_checks(emulator, PROJECT, "health_edges", DAY)
    assert actual == {"rows_processed": 2, "ingredient_rows": 2, "strength_rows": 2,
        "flagged_strengths": 1, "flagged_records": 1, "recomputed_strength_flags": 1,
        "candidate_pairs": 1, "duplicate_pairs": 1, "recomputed_duplicate_pairs": 1,
        "null_rate_din": 0.5, "null_rate_brand_name": 0.5, "null_rate_company_name": 0.5}
    with pytest.raises(ValueError, match="no products"):
        health_checks(emulator, PROJECT, "health_edges", "2026-11-02")
    # Replacing the date clears the old flags and pairs while keeping product rows.
    write_partition(tmp_path, "strength_anomalies", [])
    write_partition(tmp_path, "product_links", [])
    load_snapshot(emulator, tmp_path, PROJECT, "health_edges", DAY)
    cleared = health_checks(emulator, PROJECT, "health_edges", DAY)
    assert cleared["flagged_strengths"] == cleared["duplicate_pairs"] == 0
