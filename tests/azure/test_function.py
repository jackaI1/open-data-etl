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
    assert len(FUNCTIONS) == 2
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
