"""Publish accepted DuckDB snapshots to Elasticsearch through its bulk HTTP API."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from datetime import date
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import duckdb

# Deliberate projection: source schema changes cannot silently create new ES fields.
COLUMNS = {
    "product": (
        "drug_products",
        ("drug_code, din, brand_name, company_name, current_status, active_ingredients, atc_group"),
    ),
    "strength": (
        "strength_anomalies",
        (
            "drug_code, ingredient, strength, strength_unit, "
            "numeric_strength, robust_z, is_anomaly, reason"
        ),
    ),
    "duplicate_pair": ("product_links", "left_id, right_id, score, is_match, reason"),
}


def validate_date(value: str) -> str:
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError("snapshot date must be YYYY-MM-DD")
    return value


def mapping() -> dict:
    keywords = [
        "kind",
        "drug_code",
        "din",
        "company_name",
        "current_status",
        "atc_group",
        "ingredient",
        "strength",
        "strength_unit",
        "reason",
        "left_id",
        "right_id",
        "check_name",
        "limit",
    ]
    doubles = ["numeric_strength", "robust_z", "score", "observed"]
    counts = [
        "product_count",
        "flagged_strength_count",
        "duplicate_pair_count",
        "failed_check_count",
    ]
    fields = {key: {"type": "keyword"} for key in keywords}
    fields.update({key: {"type": "double"} for key in doubles})
    fields.update({key: {"type": "long"} for key in counts})
    fields.update({key: {"type": "boolean"} for key in ("is_anomaly", "is_match", "passed")})
    fields.update({key: {"type": "text"} for key in ("brand_name", "active_ingredients")})
    fields["snapshot_date"] = {"type": "date", "format": "strict_date"}
    return {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"dynamic": "strict", "properties": fields},
    }


def snapshot_documents(db: Path, report: Path, snapshot_date: str):
    """Read only accepted rows; bounded cursor batches avoid collecting the catalogue."""
    validate_date(snapshot_date)
    checks = json.loads(report.read_text())
    if not checks or not all(c.get("passed") is True for c in checks):
        raise ValueError("snapshot must pass all quality checks")
    with duckdb.connect(str(db), read_only=True) as con:
        n = con.execute(
            "SELECT count(*) FROM drug_products WHERE snapshot_date = ?", [snapshot_date]
        ).fetchone()[0]
        if not n:
            raise ValueError("snapshot has no products")
        for kind, (table, columns) in COLUMNS.items():
            cursor = con.execute(
                f"SELECT {columns} FROM {table} WHERE snapshot_date = ? ORDER BY ALL",
                [snapshot_date],
            )
            names = [c[0] for c in cursor.description]
            while rows := cursor.fetchmany(500):
                for row in rows:
                    doc = dict(zip(names, row))
                    for key, value in doc.items():
                        if isinstance(value, float) and not math.isfinite(value):
                            doc[key] = None
                    yield dict(doc, kind=kind, snapshot_date=snapshot_date)
        for check in checks:
            yield {
                "kind": "quality_check",
                "snapshot_date": snapshot_date,
                "check_name": check["name"],
                "passed": check["passed"],
                "observed": check["observed"],
                "limit": check["limit"],
            }
        flagged = con.execute(
            "SELECT count(*) FROM strength_anomalies WHERE snapshot_date = ? AND is_anomaly",
            [snapshot_date],
        ).fetchone()[0]
        pairs = con.execute(
            "SELECT count(*) FROM product_links WHERE snapshot_date = ? AND is_match",
            [snapshot_date],
        ).fetchone()[0]
        yield {
            "kind": "summary",
            "snapshot_date": snapshot_date,
            "product_count": n,
            "flagged_strength_count": flagged,
            "duplicate_pair_count": pairs,
            "failed_check_count": 0,
        }


class Elastic:
    def __init__(self, url: str, api_key: str | None = None):
        self.url = url.rstrip("/")
        self.api_key = api_key

    def request(self, method: str, path: str, body=None, *, missing_ok=False):
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"ApiKey {self.api_key}"
        if isinstance(body, bytes):
            data = body
            headers["Content-Type"] = "application/x-ndjson"
        else:
            data = json.dumps(body, allow_nan=False).encode() if body is not None else None
        request = Request(self.url + path, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=60) as response:
                return json.load(response)
        except HTTPError as exc:
            if missing_ok and exc.code == 404:
                return {}
            # Avoid logging request headers, credentials, server details or documents.
            raise RuntimeError(f"Elasticsearch {method} failed (HTTP {exc.code})") from None


def export_snapshot(
    db: Path,
    report: Path,
    snapshot_date: str,
    *,
    url="http://127.0.0.1:19200",
    prefix="dpd",
    batch_size=500,
    api_key: str | None = None,
) -> dict:
    """Stage then atomically publish one date. Serialize writers of the same date."""
    validate_date(snapshot_date)
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", prefix):
        raise ValueError("prefix must be a short lowercase index name")
    if not 1 <= batch_size <= 5000:
        raise ValueError("batch_size must be between 1 and 5000")
    docs = iter(snapshot_documents(db, report, snapshot_date))
    first = next(docs)  # Validate the gate and product table before contacting ES.
    client = Elastic(url, api_key)
    alias = f"{prefix}-snapshots"
    dated_prefix = f"{prefix}-{snapshot_date}-"
    index = dated_prefix + uuid4().hex
    client.request("PUT", f"/{index}", mapping())
    count = 0

    def bulk(batch):
        payload = []
        for ordinal, doc in batch:
            # Sorted source rows and a content hash keep IDs deterministic on reruns.
            encoded = json.dumps(doc, sort_keys=True, allow_nan=False)
            identity = hashlib.sha256(f"{ordinal}:{encoded}".encode()).hexdigest()
            payload.extend([json.dumps({"index": {"_id": identity}}), encoded])
        result = client.request("POST", f"/{index}/_bulk", ("\n".join(payload) + "\n").encode())
        items = result.get("items", [])
        if (
            result.get("errors")
            or len(items) != len(batch)
            or any(item["index"]["status"] >= 300 for item in items)
        ):
            raise RuntimeError("Elasticsearch bulk item failed; previous snapshot retained")

    try:
        batch = []
        from itertools import chain

        for doc in chain([first], docs):
            batch.append((count, doc))
            count += 1
            if len(batch) == batch_size:
                bulk(batch)
                batch = []
        if batch:
            bulk(batch)
        client.request("POST", f"/{index}/_refresh")
        if client.request("GET", f"/{index}/_count")["count"] != count:
            raise RuntimeError("Elasticsearch staged document count mismatch")
        old = client.request("GET", f"/_alias/{alias}", missing_ok=True)
        previous = [name for name in old if name.startswith(dated_prefix)]
    except Exception:
        client.request("DELETE", f"/{index}", missing_ok=True)
        raise
    # A timeout here has an ambiguous outcome. Retain the staged index for recovery
    # rather than risk deleting a snapshot which the server has already published.
    actions = [{"remove": {"index": name, "alias": alias}} for name in previous]
    actions.append({"add": {"index": index, "alias": alias}})
    client.request("POST", "/_aliases", {"actions": actions})
    for name in previous:
        client.request("DELETE", f"/{name}", missing_ok=True)
    return {"snapshot_date": snapshot_date, "documents": count, "alias": alias}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument(
        "--url", default=os.environ.get("ELASTICSEARCH_URL", "http://127.0.0.1:19200")
    )
    parser.add_argument("--prefix", default="dpd")
    args = parser.parse_args(argv)
    try:
        result = export_snapshot(
            args.data_dir / "warehouse/dpd.duckdb",
            args.data_dir / f"reports/dq_{args.snapshot_date}.json",
            args.snapshot_date,
            url=args.url,
            prefix=args.prefix,
            api_key=os.environ.get("ELASTIC_API_KEY"),
        )
    except (ValueError, RuntimeError, OSError, duckdb.Error) as exc:
        parser.exit(1, f"Export failed ({type(exc).__name__}); check snapshot and service.\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
