"""Stage GCS inputs locally and publish an isolated, auditable Batch attempt."""

from __future__ import annotations

import argparse
import json
import re
import tempfile
from datetime import date
from pathlib import Path
from uuid import uuid4

from google.cloud import storage

from .config import FIXTURE_THRESHOLDS, Thresholds
from .extract import EXPECTED_FILES, sha256_of
from .pipeline import run
from .quality import DataQualityError


def valid_date(value: str) -> str:
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError("snapshot date must be YYYY-MM-DD")
    return value


def valid_run(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", value):
        raise ValueError("run ID must be 1-63 lowercase letters, digits or hyphens")
    return value


def execute(*, bucket_name: str, raw_prefix: str, snapshot_date: str, run_id: str,
            baseline: str | None = None, fixture: bool = False, client=None) -> int:
    valid_date(snapshot_date)
    valid_run(run_id)
    bucket = (client or storage.Client()).bucket(bucket_name)
    prefix = f"runs/{snapshot_date}/{run_id}"
    # Reserve the name before doing expensive work. Duplicate IDs fail rather than overwrite.
    bucket.blob(f"{prefix}/started.json").upload_from_string(
        json.dumps({"snapshot_date": snapshot_date, "run_id": run_id}),
        content_type="application/json", if_generation_match=0,
    )
    with tempfile.TemporaryDirectory(prefix="dpd-batch-") as directory:
        root = Path(directory)
        raw = root / "raw"
        raw.mkdir()
        output = root / "output"
        inputs = {}
        for name in sorted(EXPECTED_FILES):
            blob = bucket.blob(f"{raw_prefix.rstrip('/')}/{name}")
            # Pin each download to its generation and retain both generation and digest.
            blob.reload(timeout=60)
            blob.download_to_filename(str(raw / name), if_generation_match=blob.generation,
                                      timeout=120)
            inputs[name] = {"generation": str(blob.generation), "sha256": sha256_of(raw / name)}
        if baseline:
            if not baseline.startswith("runs/") or not baseline.endswith("/complete.json"):
                raise ValueError("baseline must be a runs/.../complete.json object in this bucket")
            previous = json.loads(bucket.blob(baseline).download_as_bytes(timeout=60))
            if previous["snapshot_date"] >= snapshot_date:
                raise ValueError("baseline must be from an earlier snapshot date")
            warehouse = output / "warehouse" / "dpd.duckdb"
            warehouse.parent.mkdir(parents=True)
            bucket.blob(previous["warehouse"]).download_to_filename(str(warehouse), timeout=120)
        status, code = "complete", 0
        objects = []
        try:
            run(raw, output, snapshot_date, FIXTURE_THRESHOLDS if fixture else Thresholds())
            for path in sorted(output.rglob("*")):
                if path.is_file() and not path.name.startswith("."):
                    key = f"{prefix}/outputs/{path.relative_to(output).as_posix()}"
                    bucket.blob(key).upload_from_filename(
                        str(path), if_generation_match=0, timeout=300
                    )
                    objects.append(key)
        except DataQualityError:
            status, code = "rejected", 2
        except Exception:
            status, code = "failed", 1
            raise
        finally:
            report_file = output / "reports" / f"dq_{snapshot_date}.json"
            report = {
                "snapshot_date": snapshot_date, "run_id": run_id, "status": status,
                "fixture": fixture, "baseline": baseline, "inputs": inputs,
                "checks": json.loads(report_file.read_text()) if report_file.exists() else [],
            }
            bucket.blob(f"{prefix}/report.json").upload_from_string(
                json.dumps(report, indent=2) + "\n", content_type="application/json",
                if_generation_match=0,
            )
        if code:
            return code
        # Completion is the commit point. A partial upload is never an accepted snapshot.
        bucket.blob(f"{prefix}/complete.json").upload_from_string(
            json.dumps({"snapshot_date": snapshot_date, "run_id": run_id, "objects": objects,
                        "warehouse": f"{prefix}/outputs/warehouse/dpd.duckdb"}),
            content_type="application/json", if_generation_match=0,
        )
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--raw-prefix", required=True)
    parser.add_argument("--snapshot-date", required=True, type=valid_date)
    parser.add_argument("--run-id", default=uuid4().hex, type=valid_run)
    parser.add_argument("--baseline")
    parser.add_argument("--fixture", action="store_true")
    args = parser.parse_args(argv)
    return execute(bucket_name=args.bucket, raw_prefix=args.raw_prefix,
                   snapshot_date=args.snapshot_date, run_id=args.run_id,
                   baseline=args.baseline, fixture=args.fixture)


if __name__ == "__main__":
    raise SystemExit(main())
