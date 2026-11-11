"""Daily release archive: gate on accepted runs, skip unchanged copies, refuse changed ones."""

import importlib.util
import os
import shutil
from contextlib import suppress
from pathlib import Path
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest

from dpd_etl import archive
from dpd_etl.archive import ReleaseMismatch

ROOT = Path(__file__).resolve().parents[2]
DAY = "2026-11-01"
RAW = ROOT / "tests" / "fixtures" / "raw"
ACCEPTED = {"flagged_strengths": 0, "duplicate_pairs": 0}


def _release(tmp_path):
    raw = tmp_path / "raw"
    shutil.copytree(RAW, raw)
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "run.json").write_text('{"snapshot_date": "2026-11-01"}\n')
    return raw, outputs


def _manifest_for(raw, outputs):
    files = []
    for part, root in (("raw", raw), ("outputs", outputs)):
        for file in sorted(p for p in root.rglob("*") if p.is_file()):
            files.append({"part": part, "path": file.relative_to(root).as_posix(),
                          "bytes": file.stat().st_size, "sha256": archive.sha256_file(file)})
    return {"snapshot_date": DAY, "file_count": len(files),
            "total_bytes": sum(f["bytes"] for f in files), "files": files}


def test_rejected_snapshot_archives_nothing(monkeypatch, tmp_path):
    raw, outputs = _release(tmp_path)
    monkeypatch.setattr(archive, "accepted_summary",
                        Mock(side_effect=ValueError("snapshot must pass quality checks")))
    container = Mock()
    with pytest.raises(ValueError, match="quality checks"):
        archive.archive_accepted_release(container, tmp_path, DAY, raw_dir=raw, output_dir=outputs)
    container.get_blob_client.assert_not_called()


def test_unchanged_archive_is_skipped_not_uploaded(monkeypatch, tmp_path):
    raw, outputs = _release(tmp_path)
    monkeypatch.setattr(archive, "accepted_summary", Mock(return_value=dict(ACCEPTED)))
    monkeypatch.setattr(archive, "restore_manifest", Mock(return_value=_manifest_for(raw, outputs)))
    container = Mock()
    container.get_blob_client.return_value.exists.return_value = True
    result = archive.archive_accepted_release(container, tmp_path, DAY, raw_dir=raw, output_dir=outputs)
    assert result["status"] == "already_archived"
    container.get_blob_client.return_value.upload_blob.assert_not_called()


def test_changed_local_file_after_archive_is_refused(monkeypatch, tmp_path):
    raw, outputs = _release(tmp_path)
    manifest = _manifest_for(raw, outputs)
    (outputs / "run.json").write_text('{"snapshot_date": "changed"}\n')
    monkeypatch.setattr(archive, "accepted_summary", Mock(return_value=dict(ACCEPTED)))
    monkeypatch.setattr(archive, "restore_manifest", Mock(return_value=manifest))
    container = Mock()
    container.get_blob_client.return_value.exists.return_value = True
    with pytest.raises(ReleaseMismatch, match="outputs/run.json differs"):
        archive.archive_accepted_release(container, tmp_path, DAY, raw_dir=raw, output_dir=outputs)
    container.get_blob_client.return_value.upload_blob.assert_not_called()


def test_new_snapshot_is_uploaded_once(monkeypatch, tmp_path):
    raw, outputs = _release(tmp_path)
    upload = Mock(return_value=_manifest_for(raw, outputs))
    monkeypatch.setattr(archive, "accepted_summary", Mock(return_value=dict(ACCEPTED, flagged_strengths=2)))
    monkeypatch.setattr(archive, "upload_release", upload)
    container = Mock()
    container.get_blob_client.return_value.exists.return_value = False
    result = archive.archive_accepted_release(container, tmp_path, DAY, raw_dir=raw, output_dir=outputs)
    assert result["status"] == "archived" and result["flagged_strengths"] == 2
    upload.assert_called_once()


def test_timer_reads_settings_and_uses_the_archive_gate(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("function_app_under_test",
                                                  ROOT / "azure_summary" / "function_app.py")
    function_app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(function_app)
    seen = {}

    def fake_archive(container, data_dir, snapshot, *, raw_dir, output_dir):
        seen.update(data_dir=data_dir, snapshot=snapshot, raw_dir=raw_dir, output_dir=output_dir)
        return {"status": "archived"}

    monkeypatch.setattr(function_app.archive, "archive_accepted_release", fake_archive)
    monkeypatch.setattr(function_app.archive, "container_client", MagicMock())
    monkeypatch.setenv("DPD_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DPD_SNAPSHOT_DATE", DAY)
    monkeypatch.setenv("DPD_ARCHIVE_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("DPD_ARCHIVE_CONNECTION_STRING", "UseDevelopmentStorage=true")
    monkeypatch.delenv("DPD_RAW_DIR", raising=False)
    assert function_app.archive_release() == {"status": "archived"}
    assert seen["snapshot"] == DAY
    assert seen["raw_dir"] == tmp_path / "data" / "raw" / "files"
    assert seen["output_dir"] == tmp_path / "out"


AZURITE = os.environ.get("AZURITE_CONNECTION_STRING", "")


@pytest.mark.skipif(not AZURITE, reason="set AZURITE_CONNECTION_STRING for Azurite")
def test_real_archive_runs_twice_on_azurite_and_detects_tampering(tmp_path):
    from azure.storage.blob import BlobServiceClient

    data = ROOT / "data" / "fixture"
    if not (data / "reports" / f"run_{DAY}.json").exists():
        pytest.skip("run `make fixture SNAPSHOT_DATE=2026-11-01` first")
    outputs = tmp_path / "outputs"
    shutil.copytree(data, outputs)
    name = "dpd-test-" + uuid4().hex[:12]
    service = BlobServiceClient.from_connection_string(AZURITE)
    try:
        with archive.container_client(AZURITE, name) as container:
            archive.ensure_container(container)
            first = archive.archive_accepted_release(container, data, DAY, raw_dir=RAW, output_dir=outputs)
            again = archive.archive_accepted_release(container, data, DAY, raw_dir=RAW, output_dir=outputs)
            assert first["status"] == "archived" and again["status"] == "already_archived"
            assert [r["snapshot_date"] for r in archive.list_releases(container)] == [DAY]
            (outputs / "reports" / f"run_{DAY}.json").write_text("{}\n")
            with pytest.raises(ReleaseMismatch):
                archive.archive_accepted_release(container, data, DAY, raw_dir=RAW, output_dir=outputs)
    finally:
        with suppress(Exception):
            service.delete_container(name)
        service.close()
