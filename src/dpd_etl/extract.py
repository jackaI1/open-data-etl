"""Extract: download the DPD bulk zip, verify/record its checksum, unpack it.

A manifest (JSON) records url, sha256, size and download time. If the checksum is
identical to the previous download the unpack step is skipped, so scheduled runs
are cheap when Health Canada has not republished.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from .config import DPD_URL

EXPECTED_FILES = {
    "drug.txt",
    "status.txt",
    "comp.txt",
    "ingred.txt",
    "ther.txt",
    "form.txt",
    "route.txt",
}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: Path, timeout: int = 120) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    # Canada's CDN timed out with urllib's TLS client during live validation.
    # curl also rejects truncated responses before the previous archive is replaced.
    try:
        subprocess.run(
            [
                "curl",
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                "--proto",
                "=https,http,file",
                "--proto-redir",
                "=https,http",
                "--connect-timeout",
                "30",
                "--max-time",
                str(timeout),
                "--output",
                str(tmp),
                "--url",
                url,
            ],
            check=True,
            timeout=timeout + 5,
        )
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)
    return dest


def unpack(zip_path: Path, out_dir: Path) -> list[str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        bad = zf.testzip()
        if bad:
            raise ValueError(f"corrupt member in {zip_path}: {bad}")
        names = set(zf.namelist())
        missing = EXPECTED_FILES - names
        if missing:
            raise ValueError(f"zip is missing expected files: {sorted(missing)}")
        for name in EXPECTED_FILES:
            # Extract by fixed name only, never trusting archive paths.
            (out_dir / name).write_bytes(zf.read(name))
    return sorted(EXPECTED_FILES)


def extract(raw_root: Path, url: str = DPD_URL, expected_sha256: str | None = None) -> Path:
    """Download + verify + unpack. Returns the directory holding the .txt files."""
    zip_path = raw_root / "allfiles.zip"
    manifest_path = raw_root / "manifest.json"
    previous = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}

    download(url, zip_path)
    digest = sha256_of(zip_path)
    if expected_sha256 and digest != expected_sha256:
        raise ValueError(f"checksum mismatch: expected {expected_sha256}, got {digest}")

    unchanged = previous.get("sha256") == digest and all(
        (raw_root / "files" / name).is_file() for name in EXPECTED_FILES
    )
    if not unchanged:
        unpack(zip_path, raw_root / "files")
    manifest = {
        "url": url,
        "sha256": digest,
        "bytes": zip_path.stat().st_size,
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "unchanged_since_previous": unchanged,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"[extract] sha256={digest[:12]}… bytes={manifest['bytes']} unchanged={unchanged}")
    return raw_root / "files"
