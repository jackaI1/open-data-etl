import json
import zipfile

import pytest

from dpd_etl.extract import EXPECTED_FILES, extract, sha256_of


def make_zip(path, names=EXPECTED_FILES):
    with zipfile.ZipFile(path, "w") as zf:
        for n in names:
            zf.writestr(n, '"1","x"\n')
    return path


def test_extract_records_checksum_and_unpacks(tmp_path):
    src = make_zip(tmp_path / "src.zip")
    out = extract(tmp_path / "raw", url=src.as_uri())
    assert {p.name for p in out.iterdir()} == EXPECTED_FILES
    manifest = json.loads((tmp_path / "raw" / "manifest.json").read_text())
    assert manifest["sha256"] == sha256_of(src)
    assert manifest["unchanged_since_previous"] is False


def test_second_download_with_same_checksum_is_flagged_unchanged(tmp_path):
    src = make_zip(tmp_path / "src.zip")
    extract(tmp_path / "raw", url=src.as_uri())
    extract(tmp_path / "raw", url=src.as_uri())
    manifest = json.loads((tmp_path / "raw" / "manifest.json").read_text())
    assert manifest["unchanged_since_previous"] is True


def test_checksum_mismatch_fails(tmp_path):
    src = make_zip(tmp_path / "src.zip")
    with pytest.raises(ValueError, match="checksum mismatch"):
        extract(tmp_path / "raw", url=src.as_uri(), expected_sha256="0" * 64)


def test_zip_missing_expected_file_fails(tmp_path):
    src = make_zip(tmp_path / "src.zip", names={"drug.txt"})
    with pytest.raises(ValueError, match="missing expected files"):
        extract(tmp_path / "raw", url=src.as_uri())


def test_unchanged_download_repairs_missing_extracted_file(tmp_path):
    src = make_zip(tmp_path / "src.zip")
    out = extract(tmp_path / "raw", url=src.as_uri())
    (out / "drug.txt").unlink()
    extract(tmp_path / "raw", url=src.as_uri())
    assert (out / "drug.txt").read_text() == '"1","x"\n'


def test_truncated_http_download_preserves_previous_archive_and_removes_partial(tmp_path):
    import subprocess
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from dpd_etl.extract import download

    class TruncatedResponse(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "100")
            self.end_headers()
            self.wfile.write(b"short")
            self.close_connection = True

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), TruncatedResponse)
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    dest = tmp_path / "archive.zip"
    dest.write_bytes(b"previous successful archive")
    try:
        with pytest.raises(subprocess.CalledProcessError):
            download(f"http://127.0.0.1:{server.server_port}/archive.zip", dest, timeout=5)
        assert dest.read_bytes() == b"previous successful archive"
        assert not dest.with_suffix(".part").exists()
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
    assert not worker.is_alive()
