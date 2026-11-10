"""Archive, list and restore monthly releases against the real Azurite emulator."""

import json
import os
import shutil
from pathlib import Path
from uuid import uuid4

import pytest

pytest.importorskip("azure.storage.blob")
from azure.storage.blob import BlobServiceClient

from dpd_etl import archive
from dpd_etl.archive import ReleaseMismatch

RAW = Path(__file__).resolve().parents[1] / "fixtures" / "raw"
DAY = "2026-11-01"
CONNECTION = os.environ.get("AZURITE_CONNECTION_STRING", "")

pytestmark = pytest.mark.skipif(not CONNECTION, reason="set AZURITE_CONNECTION_STRING for Azurite")


@pytest.fixture()
def container():
    """A fresh container per test, always deleted, so reruns never see old blobs."""
    name = f"dpd-test-{uuid4().hex[:12]}"
    service = BlobServiceClient.from_connection_string(CONNECTION)
    client = service.create_container(name)
    try:
        yield client
    finally:
        service.delete_container(name)
        service.close()


@pytest.fixture()
def release_dirs(tmp_path):
    raw = tmp_path / "raw"
    shutil.copytree(RAW, raw)
    outputs = tmp_path / "outputs" / "reports"
    outputs.mkdir(parents=True)
    (outputs / f"lakehouse_{DAY}.json").write_text(json.dumps({"snapshot_date": DAY}) + "\n")
    return raw, tmp_path / "outputs"


def test_upload_lists_and_restores_every_byte(container, release_dirs, tmp_path):
    raw, outputs = release_dirs
    manifest = archive.upload_release(container, DAY, raw_dir=raw, output_dir=outputs)
    assert manifest["file_count"] == 8 and manifest["total_bytes"] > 0
    listed = archive.list_releases(container)
    assert [r["snapshot_date"] for r in listed] == [DAY]
    assert listed[0]["file_count"] == 8

    dest = tmp_path / "restored"
    restored = archive.restore_release(container, DAY, dest)
    assert restored["file_count"] == 8
    pairs = [(source, dest / "raw" / source.relative_to(raw)) for source in raw.rglob("*.txt")]
    pairs += [(source, dest / "outputs" / source.relative_to(outputs))
              for source in outputs.rglob("*.json")]
    assert len(pairs) == 8
    for source, target in pairs:
        assert target.read_bytes() == source.read_bytes()


def test_rerun_with_same_files_is_safe_and_changed_files_are_rejected(container, release_dirs):
    raw, _ = release_dirs
    archive.upload_release(container, DAY, raw_dir=raw)
    again = archive.upload_release(container, DAY, raw_dir=raw)
    assert again["file_count"] == 7
    (raw / "drug.txt").write_bytes((raw / "drug.txt").read_bytes() + b"\n")
    with pytest.raises(ReleaseMismatch, match="already stored with other content"):
        archive.upload_release(container, DAY, raw_dir=raw)


def test_restore_refuses_to_overwrite_and_detects_tampered_blobs(container, release_dirs, tmp_path):
    raw, _ = release_dirs
    archive.upload_release(container, DAY, raw_dir=raw)
    dest = tmp_path / "restored"
    (dest / "raw").mkdir(parents=True)
    (dest / "raw" / "drug.txt").write_text("local copy")
    with pytest.raises(FileExistsError):
        archive.restore_release(container, DAY, dest)
    (dest / "raw" / "drug.txt").unlink()
    # Simulate a stored blob that no longer matches its manifest.
    container.get_blob_client(f"{DAY}/raw/drug.txt").upload_blob(b"tampered", overwrite=True)
    with pytest.raises(ReleaseMismatch, match="checksum failed"):
        archive.restore_release(container, DAY, dest)
    assert not (dest / "raw" / "drug.txt").exists()


def test_release_without_manifest_is_not_listed(container):
    container.get_blob_client(f"{DAY}/raw/drug.txt").upload_blob(b"half uploaded", overwrite=True)
    assert archive.list_releases(container) == []


def test_upload_needs_an_input_folder(container):
    with pytest.raises(ValueError, match="raw_dir, output_dir"):
        archive.upload_release(container, DAY)


def test_command_line_lists_releases(container, release_dirs, monkeypatch, capsys):
    raw, _ = release_dirs
    archive.upload_release(container, DAY, raw_dir=raw)
    monkeypatch.setenv("DPD_ARCHIVE_CONNECTION_STRING", CONNECTION)
    monkeypatch.setenv("DPD_ARCHIVE_CONTAINER", container.container_name)
    assert archive.main(["list"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed[0]["snapshot_date"] == DAY


def test_cli_upload_creates_the_container_on_first_use(release_dirs, monkeypatch, capsys):
    raw, outputs = release_dirs
    name = f"dpd-test-{uuid4().hex[:12]}"
    service = BlobServiceClient.from_connection_string(CONNECTION)
    monkeypatch.setenv("DPD_ARCHIVE_CONNECTION_STRING", CONNECTION)
    try:
        assert archive.main(["--container", name, "upload", "--snapshot-date", DAY,
                             "--raw-dir", str(raw), "--output-dir", str(outputs)]) == 0
        uploaded = json.loads(capsys.readouterr().out)
        assert uploaded["file_count"] == 8
        assert archive.main(["--container", name, "list"]) == 0
        assert json.loads(capsys.readouterr().out)[0]["file_count"] == 8
    finally:
        service.delete_container(name)
        service.close()
