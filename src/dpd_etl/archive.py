"""Archive each monthly release to Azure Blob Storage with checksums, and restore it.

A release is the raw downloaded files plus any output folders for one snapshot date.
Every file is stored under `<snapshot_date>/<part>/<relative path>` with its SHA-256.
A `<snapshot_date>/MANIFEST.json` file is written last, so a release counts as stored
only after every file uploaded. Restore checks each file against the manifest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .elastic import validate_date as valid_date  # light import: the Functions image has no PySpark
from .summary import accepted_summary

MANIFEST = "MANIFEST.json"
DEFAULT_CONTAINER = "dpd-releases"


class ReleaseMismatch(ValueError):
    """Raised when a stored file differs from the local copy or the manifest."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def container_client(connection_string: str, container: str = DEFAULT_CONTAINER):
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient.from_connection_string(connection_string)
    return service.get_container_client(container)


def ensure_container(container) -> None:
    """Create the container on first upload; an existing one is fine."""
    from azure.core.exceptions import ResourceExistsError

    try:
        container.create_container()
    except ResourceExistsError:
        pass


def _blob_name(snapshot_date: str, part: str, relative: str) -> str:
    return f"{snapshot_date}/{part}/{relative}"


def upload_release(
    container, snapshot_date: str, *, raw_dir: Path | None = None, output_dir: Path | None = None,
) -> dict:
    """Upload one release. Re-running it is safe when the files are unchanged."""
    valid_date(snapshot_date)
    sources = {"raw": raw_dir, "outputs": output_dir}
    if not any(sources.values()):
        raise ValueError("give raw_dir, output_dir or both")
    if container.get_blob_client(f"{snapshot_date}/{MANIFEST}").exists():
        existing = restore_manifest(container, snapshot_date)
        stored = {(f["part"], f["path"]): f["sha256"] for f in existing["files"]}
    else:
        stored = {}
    entries = []
    for part, folder in sources.items():
        if folder is None:
            continue
        root = Path(folder)
        if not root.is_dir():
            raise ValueError(f"{part} folder does not exist: {root}")
        for file in sorted(p for p in root.rglob("*") if p.is_file()):
            relative = file.relative_to(root).as_posix()
            digest = sha256_file(file)
            if (part, relative) in stored and stored[(part, relative)] != digest:
                raise ReleaseMismatch(f"{snapshot_date}/{part}/{relative} already stored with other content")
            blob = container.get_blob_client(_blob_name(snapshot_date, part, relative))
            with file.open("rb") as handle:
                blob.upload_blob(handle, overwrite=True, metadata={"sha256": digest})
            entries.append({"part": part, "path": relative, "bytes": file.stat().st_size,
                            "sha256": digest})
    if not entries:
        raise ValueError("no files found to archive")
    manifest = {
        "snapshot_date": snapshot_date,
        "uploaded_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "file_count": len(entries),
        "total_bytes": sum(e["bytes"] for e in entries),
        "files": entries,
    }
    container.get_blob_client(f"{snapshot_date}/{MANIFEST}").upload_blob(
        json.dumps(manifest, indent=2).encode(), overwrite=True,
        content_settings=_json_settings(),
    )
    return manifest


def _json_settings():
    from azure.storage.blob import ContentSettings

    return ContentSettings(content_type="application/json")


def restore_manifest(container, snapshot_date: str) -> dict:
    valid_date(snapshot_date)
    blob = container.get_blob_client(f"{snapshot_date}/{MANIFEST}")
    return json.loads(blob.download_blob().readall())


def verify_local_copy(manifest: dict, raw_dir: Path | None, output_dir: Path | None) -> None:
    """Refuse to continue when a local file no longer matches the archived checksum."""
    roots = {"raw": raw_dir, "outputs": output_dir}
    for entry in manifest["files"]:
        root = roots.get(entry["part"])
        if root is None:
            continue
        path = Path(root) / entry["path"]
        if not path.is_file() or sha256_file(path) != entry["sha256"]:
            raise ReleaseMismatch(
                f"local {entry['part']}/{entry['path']} differs from the archived "
                f"{manifest['snapshot_date']} release"
            )


def archive_accepted_release(
    container, data_dir: Path, snapshot_date: str, *, raw_dir: Path, output_dir: Path,
) -> dict:
    """Archive one snapshot only after its accepted-run checks pass.

    `accepted_summary` rejects a snapshot whose quality checks failed, whose run receipt
    disagrees with the database, or whose duplicate-pair and strength counts changed.
    A release that is already archived is not uploaded again; its local files must still
    match the stored checksums, otherwise the call raises instead of skipping silently.
    """
    summary = accepted_summary(Path(data_dir), snapshot_date)
    ensure_container(container)
    if container.get_blob_client(f"{snapshot_date}/{MANIFEST}").exists():
        manifest = restore_manifest(container, snapshot_date)
        verify_local_copy(manifest, raw_dir, output_dir)
        status = "already_archived"
    else:
        manifest = upload_release(container, snapshot_date, raw_dir=Path(raw_dir),
                                  output_dir=Path(output_dir))
        status = "archived"
    return {
        "status": status,
        "snapshot_date": snapshot_date,
        "file_count": manifest["file_count"],
        "total_bytes": manifest["total_bytes"],
        "flagged_strengths": summary["flagged_strengths"],
        "duplicate_pairs": summary["duplicate_pairs"],
    }


def list_releases(container) -> list[dict]:
    """Every release whose manifest finished uploading, newest first."""
    releases = []
    for item in container.list_blobs(name_starts_with=None):
        parts = item.name.split("/")
        if len(parts) == 2 and parts[1] == MANIFEST:
            manifest = restore_manifest(container, parts[0])
            releases.append({
                "snapshot_date": manifest["snapshot_date"],
                "file_count": manifest["file_count"],
                "total_bytes": manifest["total_bytes"],
                "uploaded_at": manifest["uploaded_at"],
            })
    return sorted(releases, key=lambda r: r["snapshot_date"], reverse=True)


def restore_release(container, snapshot_date: str, dest: Path) -> dict:
    """Download a release into dest/<part>/ and verify every checksum.

    Refuses to write over an existing file, so a restore never silently changes local data.
    A file that fails its checksum is deleted and the restore stops.
    """
    manifest = restore_manifest(container, snapshot_date)
    dest = Path(dest)
    targets = [(f, dest / f["part"] / f["path"]) for f in manifest["files"]]
    for _, target in targets:
        if target.exists():
            raise FileExistsError(f"refusing to overwrite {target}")
    for entry, target in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        blob = container.get_blob_client(_blob_name(snapshot_date, entry["part"], entry["path"]))
        with target.open("wb") as handle:
            blob.download_blob().readinto(handle)
        digest = sha256_file(target)
        if digest != entry["sha256"] or target.stat().st_size != entry["bytes"]:
            target.unlink()
            raise ReleaseMismatch(f"checksum failed for {snapshot_date}/{entry['part']}/{entry['path']}")
    return manifest


def _connection_string() -> str:
    value = os.environ.get("DPD_ARCHIVE_CONNECTION_STRING", "")
    if not value.strip():
        raise SystemExit("Set DPD_ARCHIVE_CONNECTION_STRING to a storage connection string")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", default=os.environ.get("DPD_ARCHIVE_CONTAINER", DEFAULT_CONTAINER))
    sub = parser.add_subparsers(dest="command", required=True)
    up = sub.add_parser("upload", help="upload one monthly release")
    up.add_argument("--snapshot-date", required=True)
    up.add_argument("--raw-dir")
    up.add_argument("--output-dir")
    sub.add_parser("list", help="list stored releases")
    rs = sub.add_parser("restore", help="download one release and verify checksums")
    rs.add_argument("--snapshot-date", required=True)
    rs.add_argument("--dest", required=True)
    args = parser.parse_args(argv)
    container = container_client(_connection_string(), args.container)
    if args.command == "upload":
        ensure_container(container)
        manifest = upload_release(
            container, args.snapshot_date,
            raw_dir=Path(args.raw_dir) if args.raw_dir else None,
            output_dir=Path(args.output_dir) if args.output_dir else None,
        )
        print(json.dumps({k: v for k, v in manifest.items() if k != "files"}, indent=2))
    elif args.command == "list":
        print(json.dumps(list_releases(container), indent=2))
    else:
        manifest = restore_release(container, args.snapshot_date, Path(args.dest))
        print(json.dumps({"snapshot_date": manifest["snapshot_date"],
                          "restored_files": manifest["file_count"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
