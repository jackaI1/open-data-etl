"""Cloud boundaries are mocked; integration still runs real Spark and DuckDB."""

import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import duckdb
import pytest
from google.api_core.exceptions import Forbidden, NotFound, PreconditionFailed
from google.cloud import batch_v1

from dpd_etl import gcp_batch
from dpd_etl.extract import EXPECTED_FILES
from dpd_etl.quality import DataQualityError
from dpd_etl.report_service import create_app

DATE = "2026-11-01"


@pytest.fixture
def cloud(raw_dir):
    objects = {f"raw/{name}": (raw_dir / name).read_bytes() for name in EXPECTED_FILES}
    client = Mock()
    bucket = client.bucket.return_value

    def blob(key):
        item = Mock(generation=1)

        def read(**kwargs):
            if key not in objects:
                raise NotFound(key)
            return objects[key]

        def download(filename, **kwargs):
            Path(filename).write_bytes(read())

        def upload(value, **kwargs):
            assert kwargs["if_generation_match"] == 0
            if key in objects:
                raise PreconditionFailed(key)
            objects[key] = value.encode() if isinstance(value, str) else value

        item.download_as_bytes.side_effect = read
        item.download_to_filename.side_effect = download
        item.upload_from_string.side_effect = upload
        item.upload_from_filename.side_effect = lambda filename, **kw: upload(
            Path(filename).read_bytes(), **kw)
        return item

    bucket.blob.side_effect = blob
    return client, objects


def execute(client, **kwargs):
    return gcp_batch.execute(bucket_name="test-bucket", raw_prefix="raw", snapshot_date=DATE,
                             run_id="test-run", fixture=True, client=client, **kwargs)


def pretend_pipeline(raw, output, snapshot, thresholds):
    assert all((raw / name).exists() for name in EXPECTED_FILES)
    reports = output / "reports"
    reports.mkdir(parents=True)
    (reports / f"dq_{snapshot}.json").write_text(json.dumps([
        {"name": "row_count_min", "passed": True, "observed": 60, "limit": ">= 20"}
    ]))
    (output / "warehouse").mkdir(exist_ok=True)
    (output / "warehouse" / "dpd.duckdb").write_bytes(b"database")


def test_publish_reserves_name_and_commits_last(cloud, monkeypatch):
    client, objects = cloud
    monkeypatch.setattr(gcp_batch, "run", pretend_pipeline)
    assert execute(client) == 0
    assert list(objects)[-1] == f"runs/{DATE}/test-run/complete.json"
    manifest = json.loads(objects[list(objects)[-1]])
    assert objects[manifest["warehouse"]] == b"database"
    report = json.loads(objects[f"runs/{DATE}/test-run/report.json"])
    assert len(report["inputs"]) == 7
    assert all(len(v["sha256"]) == 64 for v in report["inputs"].values())
    with pytest.raises(PreconditionFailed):
        execute(client)


@pytest.mark.parametrize("error,expected", [(DataQualityError("bad"), 2), (RuntimeError("bad"), 1)])
def test_rejection_and_crash_never_publish_accepted_outputs(cloud, monkeypatch, error, expected):
    client, objects = cloud

    def fail(*args):
        pretend_pipeline(*args)
        raise error

    monkeypatch.setattr(gcp_batch, "run", fail)
    if expected == 2:
        assert execute(client) == 2
    else:
        with pytest.raises(RuntimeError):
            execute(client)
    assert not any("/outputs/" in k or k.endswith("/complete.json") for k in objects)
    report = json.loads(objects[f"runs/{DATE}/test-run/report.json"])
    assert report["status"] == ("rejected" if expected == 2 else "failed")


def test_partial_upload_has_no_completion_record(cloud, monkeypatch):
    client, objects = cloud
    monkeypatch.setattr(gcp_batch, "run", pretend_pipeline)
    original = client.bucket.return_value.blob.side_effect

    def blob(key):
        result = original(key)
        if "/outputs/" in key:
            result.upload_from_filename.side_effect = Forbidden("no write")
        return result

    client.bucket.return_value.blob.side_effect = blob
    with pytest.raises(Forbidden):
        execute(client)
    assert not any(k.endswith("/complete.json") for k in objects)
    assert json.loads(objects[f"runs/{DATE}/test-run/report.json"])["status"] == "failed"


@pytest.mark.parametrize("value", ["../escape", "2026-02-30", "2026-1-1"])
def test_bad_dates_fail_before_storage(value):
    client = Mock()
    with pytest.raises(ValueError):
        gcp_batch.execute(bucket_name="x", raw_prefix="raw", snapshot_date=value,
                          run_id="test", client=client)
    client.bucket.assert_not_called()


def test_baseline_must_be_earlier(cloud):
    client, objects = cloud
    key = f"runs/{DATE}/previous/complete.json"
    objects[key] = json.dumps({"snapshot_date": DATE}).encode()
    with pytest.raises(ValueError, match="earlier"):
        execute(client, baseline=key)


def test_report_html_json_and_escaping(cloud, monkeypatch):
    client, objects = cloud
    monkeypatch.setattr(gcp_batch, "run", pretend_pipeline)
    execute(client)
    key = f"runs/{DATE}/test-run/report.json"
    payload = json.loads(objects[key])
    payload["checks"][0]["name"] = "<script>alert(1)</script>"
    objects[key] = json.dumps(payload).encode()
    http = create_app(client, "test-bucket").test_client()
    response = http.get(f"/reports/{DATE}/test-run")
    assert response.status_code == 200
    assert b"&lt;script&gt;" in response.data and b"<script>" not in response.data
    assert response.headers["Cache-Control"] == "no-store"
    assert http.get(f"/reports/{DATE}/test-run.json").json == payload
    assert http.get(f"/reports/{DATE}/missing").status_code == 404
    assert http.get("/reports/not-a-date/test-run").status_code == 400
    assert http.get("/healthz").json == {"status": "ok"}


def test_report_storage_failure_is_bounded():
    client = Mock()
    client.bucket.return_value.blob.return_value.download_as_bytes.side_effect = Forbidden("private")
    response = create_app(client, "test").test_client().get(f"/reports/{DATE}/test-run")
    assert response.status_code == 503
    assert b"private" not in response.data


def test_batch_config_matches_google_schema():
    spec = importlib.util.spec_from_file_location("render", "infra/gcp/render.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    config = module.batch_config("sample-project", "us-central1", "sample-bucket",
                                  "example/batch:sha", DATE, "test-run", "raw", True,
                                  "runs/2026-10-01/old/complete.json")
    job = batch_v1.Job.from_json(json.dumps(config))  # Reject unknown fields and wrong types.
    task = job.task_groups[0].task_spec
    assert task.max_retry_count == 0
    assert task.runnables[0].container.commands[-2:] == [
        "--baseline", "runs/2026-10-01/old/complete.json"]
    assert job.allocation_policy.service_account.email.startswith("dpd-batch@")
    assert job.allocation_policy.network.network_interfaces[0].subnetwork.endswith("/dpd-batch")


def test_real_cloud_pipeline_and_previous_snapshot_gate(cloud, tmp_path):
    client, objects = cloud
    assert execute(client) == 0
    manifest_key = f"runs/{DATE}/test-run/complete.json"
    manifest = json.loads(objects[manifest_key])
    warehouse = tmp_path / "published.duckdb"
    warehouse.write_bytes(objects[manifest["warehouse"]])
    with duckdb.connect(str(warehouse), read_only=True) as con:
        assert con.execute("select count(*) from drug_products").fetchone()[0] == 60
    assert any("drug_products/snapshot_date=" in key for key in manifest["objects"])
    # The cloud job must restore its baseline, or this 50% drop would be missed.
    original_drugs = objects["raw/drug.txt"]
    objects["raw/drug.txt"] = b"\n".join(original_drugs.splitlines()[:30]) + b"\n"
    assert gcp_batch.execute(bucket_name="test-bucket", raw_prefix="raw",
                             snapshot_date="2026-11-08", run_id="drop", baseline=manifest_key,
                             fixture=True, client=client) == 2
    prefix = "runs/2026-11-08/drop/"
    report = json.loads(objects[prefix + "report.json"])
    assert report["status"] == "rejected"
    assert any(c["name"] == "row_drop_vs_previous_snapshot" and not c["passed"]
               for c in report["checks"])
    assert prefix + "complete.json" not in objects
    assert objects[manifest["warehouse"]] == warehouse.read_bytes()
    http = create_app(client, "test-bucket").test_client()
    assert b"FAIL" in http.get("/reports/2026-11-08/drop").data
    objects["raw/drug.txt"] = original_drugs
    assert gcp_batch.execute(bucket_name="test-bucket", raw_prefix="raw",
                             snapshot_date="2026-11-08", run_id="recovered", baseline=manifest_key,
                             fixture=True, client=client) == 0
    key = "runs/2026-11-08/recovered/complete.json"
    accepted = json.loads(objects[key])
    warehouse.write_bytes(objects[accepted["warehouse"]])
    with duckdb.connect(str(warehouse), read_only=True) as con:
        assert con.execute("select count(*) from drug_products").fetchone()[0] == 120
        assert con.execute("select count(distinct snapshot_date) from drug_products").fetchone()[0] == 2
