import socket
import subprocess
import zipfile
from unittest.mock import patch

import pytest

from dpd_etl.monitoring import Monitor
from dpd_etl.pipeline import main
from dpd_etl.quality import Check, DataQualityError


def test_disabled_does_not_construct_client(monkeypatch):
    monkeypatch.setenv("DD_DOGSTATSD_PORT", "not-a-port")
    with patch("dpd_etl.monitoring.DogStatsd", side_effect=AssertionError("unexpected client")):
        monitor = Monitor.from_env()
        with monitor.command("transform"):
            monitor.quality([Check("row_count_min", True, 60, ">=20")])
            monitor.products(60)


def test_real_client_emits_safe_tags_and_closes_socket(monitoring_packets, monkeypatch):
    monkeypatch.setenv("DATADOG_TAGS", "owner:private-value,path:/private/file")
    monkeypatch.setenv("DD_SERVICE", "injected-service")
    monkeypatch.setenv("DD_ENTITY_ID", "private-entity")
    monitor = Monitor.from_env()
    with monitor.command("transform"):
        monitor.quality([Check("row_count_min", True, 60, ">=20"),
                         Check("private-unknown-name", True, 1, "secret")])
    packets = monitoring_packets()
    assert len(packets) == 6
    assert all("service:open-data-etl" in p and "dataset:marketed-dpd" in p and
               "env:test" in p and "version:test" in p for p in packets)
    assert not any(word in "\n".join(packets) for word in
                   ["private", "secret", "injected", "/file", "snapshot"])
    assert monitor.client.socket is None


@pytest.mark.parametrize("exception,outcome", [
    (DataQualityError("private record"), "quality_failed"),
    (OSError("private path"), "error"),
    (KeyboardInterrupt(), "error"),
])
def test_failure_is_reraised_and_reported(monitoring_packets, exception, outcome):
    monitor = Monitor.from_env()
    with pytest.raises(type(exception)) as caught, monitor.command("transform"):
        raise exception
    assert caught.value is exception
    packets = monitoring_packets()
    assert any(f"outcome:{outcome}" in p for p in packets)
    assert any(p.startswith("_sc|dpd_etl.run|2|") for p in packets)
    assert "private" not in "\n".join(packets)
    assert monitor.client.socket is None


def test_quality_observed_includes_zero_and_fraction(monitoring_packets):
    monitor = Monitor.from_env()
    with monitor.command("transform"):
        monitor.quality([Check("null_ratio_din", False, 0.25, "<=0.02"),
                         Check("drug_code_unique", True, 0, "==0")])
    packets = monitoring_packets()
    assert any(p.startswith("dpd_etl.quality.observed:0.25|g|") for p in packets)
    assert any(p.startswith("dpd_etl.quality.failed:0|g|") for p in packets)


def test_transport_failure_does_not_replace_pipeline_exception(monitoring_packets, caplog):
    monitor = Monitor.from_env()
    with (
        patch.object(monitor.client, "_send", side_effect=OSError("secret endpoint")),
        pytest.raises(DataQualityError, match="original failure"),
        monitor.command("transform"),
    ):
        monitor.products(60)
        raise DataQualityError("original failure")
    assert len(caplog.records) == 1
    assert "secret" not in caplog.text
    assert monitor.client.socket is None


def test_unavailable_udp_receiver_keeps_success(monkeypatch):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    monkeypatch.setenv("DPD_DATADOG_ENABLED", "true")
    monkeypatch.setenv("DD_AGENT_HOST", "127.0.0.1")
    monkeypatch.setenv("DD_DOGSTATSD_PORT", str(port))
    monitor = Monitor.from_env()
    with monitor.command("extract"):
        pass
    assert monitor.client.socket is None


@pytest.mark.parametrize("key,value", [
    ("DPD_DATADOG_ENABLED", "yes"), ("DD_DOGSTATSD_PORT", "bad"),
    ("DD_DOGSTATSD_PORT", "0"), ("DD_DOGSTATSD_PORT", "65536"),
    ("DD_ENV", "local,owner:secret"), ("DD_VERSION", "v1\nforged:1|c"),
    ("DD_EXTERNAL_ENV", "private-external-data"),
])
def test_invalid_config_is_rejected_before_running(monkeypatch, tmp_path, key, value):
    monkeypatch.setenv("DPD_DATADOG_ENABLED", "true")
    monkeypatch.setenv(key, value)
    with pytest.raises(SystemExit) as caught:
        main(["extract", "--data-dir", str(tmp_path / "out")])
    assert caught.value.code == 2
    assert not (tmp_path / "out").exists()


def test_extract_success_is_monitored(monitoring_packets, tmp_path):
    from dpd_etl.extract import EXPECTED_FILES

    archive = tmp_path / "sample.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        for name in EXPECTED_FILES:
            zipped.writestr(name, '"1","sample"\n')
    assert main(["extract", "--url", archive.as_uri(),
                 "--data-dir", str(tmp_path / "out")]) == 0
    assert (tmp_path / "out/raw/manifest.json").is_file()
    packets = monitoring_packets()
    assert any("command:extract" in p and "outcome:success" in p for p in packets)
    assert any(p.startswith("_sc|dpd_etl.run|0|") for p in packets)


def test_extract_error_is_monitored_without_swallowing(monitoring_packets, tmp_path):
    # A local missing file exercises curl's real error path, without Internet access.
    with pytest.raises(subprocess.CalledProcessError):
        main(["extract", "--url", (tmp_path / "missing.zip").as_uri(),
              "--data-dir", str(tmp_path / "out")])
    packets = monitoring_packets()
    assert any("command:extract" in p and "outcome:error" in p for p in packets)
    assert any(p.startswith("_sc|dpd_etl.run|2|") for p in packets)
