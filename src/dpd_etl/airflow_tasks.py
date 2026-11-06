"""Filesystem boundaries for the Airflow DAG; no Airflow imports or parse-time IO."""
from __future__ import annotations

import csv
import json
import shutil
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .config import DPD_URL, FIXTURE_THRESHOLDS, Thresholds
from .extract import download, sha256_of, unpack


def partition_dir(logical_date: str, root: Path) -> Path:
    try:
        if date.fromisoformat(logical_date).isoformat() != logical_date:
            raise ValueError
    except ValueError as exc:
        raise ValueError("logical date must be a valid YYYY-MM-DD date") from exc
    return Path(root) / f"snapshot_date={logical_date}"


def _begin(logical_date: str, root: Path) -> Path:
    partition = partition_dir(logical_date, root)
    # A failed retry must never leave a previous attempt looking accepted.
    (partition / "complete.json").unlink(missing_ok=True)
    return partition


def _manifest(partition: Path) -> dict:
    raw = partition / "raw"
    manifest = json.loads((raw / "manifest.json").read_text())
    if sha256_of(raw / "allfiles.zip") != manifest["sha256"]:
        raise ValueError("pinned archive checksum mismatch; restore the original archive")
    return manifest


def download_partition(
    logical_date: str, root: Path, source_dir: Path | None = None, url: str = DPD_URL,
) -> None:
    """Pin one archive per date. Retries never fetch a different upstream version.

    source_dir contains YYYY-MM-DD.zip files. Historical dates must use these
    saved archives, never today's download labelled as historical data.
    """
    partition = _begin(logical_date, root)
    if (partition / "raw").exists():
        _manifest(partition)
        return
    source = Path(source_dir) / f"{logical_date}.zip" if source_dir is not None else None
    if source is not None and not source.is_file():
        raise FileNotFoundError(f"missing historical archive: {source}")
    today = datetime.now(timezone.utc).date()
    if source is None and date.fromisoformat(logical_date) not in (today, today - timedelta(days=1)):
        raise FileNotFoundError("a historical archive YYYY-MM-DD.zip is required for this date")
    partition.mkdir(parents=True, exist_ok=True)
    # Directory rename commits archive + checksum together; interrupted downloads
    # cannot be mistaken for pinned input on a later attempt.
    with tempfile.TemporaryDirectory(prefix=".download-", dir=partition) as staging:
        raw = Path(staging) / "raw"
        raw.mkdir()
        target = raw / "allfiles.zip"
        if source is not None:
            shutil.copyfile(source, target)
        else:
            download(url, target)
        (raw / "manifest.json").write_text(json.dumps({
            "logical_date": logical_date,
            "sha256": sha256_of(target),
            "bytes": target.stat().st_size,
        }, indent=2) + "\n")
        raw.rename(partition / "raw")


def validate_partition(logical_date: str, root: Path) -> None:
    """Verify pinned bytes, safe ZIP members and every source row's field count."""
    from .schemas import SCHEMAS

    partition = _begin(logical_date, root)
    _manifest(partition)
    raw = partition / "raw"
    unpack(raw / "allfiles.zip", raw / "files")
    for name, schema in SCHEMAS.items():
        count = 0
        with (raw / "files" / f"{name}.txt").open(encoding="utf-8", newline="") as fh:
            for count, row in enumerate(csv.reader(fh, strict=True), 1):
                if len(row) != len(schema):
                    raise ValueError(f"{name}.txt row {count}: expected {len(schema)} columns")
        if not count:
            raise ValueError(f"{name}.txt is empty")


def transform_partition(logical_date: str, root: Path, fixture: bool = False) -> None:
    """Reuse all existing cleaning, detection and pre-publication quality gates."""
    from .pipeline import run

    partition = _begin(logical_date, root)
    # Restore validated source files even when this task alone is retried.
    validate_partition(logical_date, root)
    run(partition / "raw/files", partition / "output", logical_date,
        FIXTURE_THRESHOLDS if fixture else Thresholds())


def quality_check_partition(logical_date: str, root: Path) -> None:
    """Check persisted outputs before atomically accepting the date's snapshot."""
    import duckdb

    from .pipeline import TABLES

    partition = _begin(logical_date, root)
    manifest = _manifest(partition)
    output = partition / "output"
    checks = json.loads((output / "reports" / f"dq_{logical_date}.json").read_text())
    required = {
        "row_count_min", "raw_vs_deduped_rows", "drug_code_unique",
        "null_ratio_brand_name", "null_ratio_din", "null_ratio_last_update_date",
        "null_ratio_company_name", "null_ratio_current_status", "orphan_ratio_status",
        "orphan_ratio_ingredients", "null_ratio_status_valid_from",
        "scd2_single_current_version", "scd2_valid_intervals",
    }
    names = [check["name"] for check in checks]
    if (not required.issubset(names) or len(names) != len(set(names))
            or not all(check["passed"] is True for check in checks)):
        raise ValueError("quality report contains failed or missing checks")
    counts = {}
    with duckdb.connect(str(output / "warehouse/dpd.duckdb"), read_only=True) as con:
        for table in TABLES:
            # TABLES is an internal allowlist, never a user-supplied identifier.
            count, wrong_date = con.execute(
                f'SELECT count(*), count(*) FILTER (WHERE snapshot_date IS NULL '
                f'OR snapshot_date != CAST(? AS DATE)) FROM "{table}"', [logical_date],
            ).fetchone()
            if wrong_date or (table == "drug_products" and count == 0):
                raise ValueError(f"invalid persisted partition for {table}")
            parquet = str(output / "parquet" / table / f"snapshot_date={logical_date}/*.parquet")
            con.execute(
                'CREATE OR REPLACE TEMP TABLE persisted AS SELECT *, CAST(? AS DATE) AS '
                'snapshot_date FROM read_parquet(?, hive_partitioning=false)',
                [logical_date, parquet],
            )
            differences = con.execute(
                f'SELECT count(*) FROM ((SELECT * FROM "{table}" EXCEPT ALL '
                f'SELECT * FROM persisted) UNION ALL (SELECT * FROM persisted '
                f'EXCEPT ALL SELECT * FROM "{table}"))'
            ).fetchone()[0]
            if differences:
                raise ValueError(f"database and Parquet differ for {table}")
            counts[table] = count
    record = {"logical_date": logical_date, "source_sha256": manifest["sha256"], "rows": counts}
    pending = partition / ".complete.json.tmp"
    pending.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    pending.replace(partition / "complete.json")
