"""Use the Functions SDK's registered timer and real Azurite storage in CI."""

import importlib.util
import json
import os
from contextlib import suppress
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest

func = pytest.importorskip("azure.functions")
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("summary_function", ROOT / "azure_summary/function_app.py")
function_app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(function_app)
FUNCTIONS = function_app.app.get_functions()


def registered(name):
    matches = [f for f in FUNCTIONS if f.get_function_name() == name]
    assert len(matches) == 1, name
    return matches[0]


def test_real_functions_registration_and_invocation(monkeypatch):
    assert len(FUNCTIONS) == 3
    registered_timer = registered("daily_quality_summary")
    binding = json.loads(registered_timer.get_function_json())["bindings"][0]
    assert binding["type"] == "timerTrigger"
    assert binding["schedule"] == "%DPD_SUMMARY_SCHEDULE%"
    assert binding["useMonitor"] is True and binding["runOnStartup"] is False
    write = Mock()
    monkeypatch.setattr(function_app, "write_summary", write)
    registered_timer.get_user_function()(Mock(spec=func.TimerRequest, past_due=False))
    write.assert_called_once_with()

    archive_binding = json.loads(registered("daily_release_archive").get_function_json())["bindings"][0]
    assert archive_binding["schedule"] == "%DPD_ARCHIVE_SCHEDULE%"
    assert archive_binding["runOnStartup"] is False


def test_quality_timer_is_registered_with_its_own_schedule():
    binding = json.loads(registered("daily_quality_check").get_function_json())["bindings"][0]
    assert binding["type"] == "timerTrigger"
    assert binding["schedule"] == "%DPD_QUALITY_SCHEDULE%"
    assert binding["runOnStartup"] is False


def write_quality_report(data_dir, checks):
    reports = data_dir / "reports"
    reports.mkdir(parents=True)
    (reports / "dq_2026-11-01.json").write_text(json.dumps(checks))


def test_quality_check_logs_each_result_and_passes(tmp_path, monkeypatch, caplog):
    checks = [
        {"name": "row_count_min", "passed": True, "observed": 60, "limit": ">= 1"},
        {"name": "drug_code_unique", "passed": True, "observed": 0, "limit": "== 0 duplicates"},
    ]
    write_quality_report(tmp_path, checks)
    monkeypatch.setenv("DPD_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DPD_SNAPSHOT_DATE", "2026-11-01")
    with caplog.at_level("INFO"):
        assert function_app.check_quality() == checks
    assert [r.getMessage() for r in caplog.records if "[dq]" in r.getMessage()] == [
        "[dq] PASS row_count_min: observed 60, limit >= 1",
        "[dq] PASS drug_code_unique: observed 0, limit == 0 duplicates",
    ]


def test_quality_check_raises_and_names_each_failed_check(tmp_path, monkeypatch, caplog):
    write_quality_report(tmp_path, [
        {"name": "row_count_min", "passed": True, "observed": 60, "limit": ">= 1"},
        {"name": "null_ratio_din", "passed": False, "observed": 0.4, "limit": "<= 0.1"},
        {"name": "orphan_ratio_status", "passed": False, "observed": 0.2, "limit": "<= 0.0"},
    ])
    monkeypatch.setenv("DPD_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DPD_SNAPSHOT_DATE", "2026-11-01")
    with caplog.at_level("INFO"), pytest.raises(RuntimeError) as failure:
        function_app.check_quality()
    assert str(failure.value) == "data-quality checks failed: null_ratio_din, orphan_ratio_status"
    assert "[dq] FAIL null_ratio_din: observed 0.4, limit <= 0.1" in caplog.text
    assert "[dq] PASS row_count_min" in caplog.text
    assert "ERROR" in caplog.text and "data-quality checks failed" in caplog.text


def test_quality_check_rejects_a_missing_or_empty_report(tmp_path, monkeypatch):
    monkeypatch.setenv("DPD_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DPD_SNAPSHOT_DATE", "2026-11-01")
    with pytest.raises(FileNotFoundError):
        function_app.check_quality()
    write_quality_report(tmp_path, [])
    with pytest.raises(ValueError, match="no quality checks"):
        function_app.check_quality()


def test_rejected_snapshot_never_writes(monkeypatch):
    monkeypatch.setenv("DPD_DATA_DIR", "/unused")
    monkeypatch.setenv("DPD_SNAPSHOT_DATE", "2026-11-01")
    monkeypatch.setattr(function_app, "accepted_summary", Mock(side_effect=ValueError("rejected")))
    storage = Mock()
    monkeypatch.setattr(function_app, "BlobServiceClient", storage)
    with pytest.raises(ValueError, match="rejected"):
        function_app.write_summary()
    assert not storage.mock_calls


def test_timer_writes_replaces_and_reads_real_azurite(monkeypatch):
    connection = os.environ.get("AZURITE_CONNECTION_STRING")
    if not connection:
        pytest.skip("Azurite integration is required by the azure-summary CI job")
    from dpd_etl.summary import accepted_summary

    data = ROOT / "data/fixture"
    expected = accepted_summary(data, "2026-11-01")
    container = "test-" + uuid4().hex
    monkeypatch.setenv("AzureWebJobsStorage", connection)
    monkeypatch.setenv("DPD_DATA_DIR", str(data))
    monkeypatch.setenv("DPD_SNAPSHOT_DATE", "2026-11-01")
    monkeypatch.setenv("DPD_SUMMARY_CONTAINER", container)
    with BlobServiceClient.from_connection_string(connection) as service:
        client = service.get_container_client(container)
        try:
            timer = registered("daily_quality_summary")
            for past_due in (False, True):
                timer.get_user_function()(Mock(spec=func.TimerRequest, past_due=past_due))
            blobs = list(client.list_blobs())
            assert [b.name for b in blobs] == ["2026-11-01/summary.json"]
            blob = client.get_blob_client(blobs[0].name)
            assert json.loads(blob.download_blob().readall()) == expected
            assert blob.get_blob_properties().content_settings.content_type == "application/json"
        finally:
            with suppress(ResourceNotFoundError):
                client.delete_container()
