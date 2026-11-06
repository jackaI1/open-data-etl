"""Read quality totals from an accepted snapshot without starting Spark."""

from __future__ import annotations

import json
import math
from pathlib import Path

import duckdb

from .elastic import validate_date


def quality_summary(db: Path, snapshot: str) -> dict:
    validate_date(snapshot)
    with duckdb.connect(str(db), read_only=True) as con:
        products = con.execute("""SELECT count(*), count(*) FILTER (WHERE din IS NULL),
            count(*) FILTER (WHERE brand_name IS NULL),
            count(*) FILTER (WHERE company_name IS NULL)
            FROM drug_products WHERE snapshot_date = ?""", [snapshot]).fetchone()
        if not products[0]:
            raise ValueError("snapshot has no products")
        strengths = con.execute("""SELECT count(*), count(*) FILTER (WHERE is_anomaly),
            count(DISTINCT drug_code) FILTER (WHERE is_anomaly)
            FROM strength_anomalies WHERE snapshot_date = ?""", [snapshot]).fetchone()
        pairs = con.execute("""SELECT count(*), count(*) FILTER (WHERE is_match)
            FROM product_links WHERE snapshot_date = ?""", [snapshot]).fetchone()
        ingredients = con.execute(
            "SELECT count(*) FROM drug_ingredients WHERE snapshot_date = ?", [snapshot]
        ).fetchone()[0]
    return {"snapshot_date": snapshot, "rows_processed": products[0], "ingredient_rows": ingredients,
                "strength_rows": strengths[0], "flagged_strengths": strengths[1],
                "flagged_records": strengths[2], "candidate_pairs": pairs[0], "duplicate_pairs": pairs[1],
                "null_rate_din": products[1] / products[0],
                "null_rate_brand_name": products[2] / products[0],
                "null_rate_company_name": products[3] / products[0]}


def accepted_summary(data_dir: Path, snapshot: str) -> dict:
    """A successful-run receipt supplies measured duration and rejects stale outputs."""
    validate_date(snapshot)
    receipt = json.loads((data_dir / "reports" / f"run_{snapshot}.json").read_text())
    checks = json.loads((data_dir / "reports" / f"dq_{snapshot}.json").read_text())
    if not checks or not all(c.get("passed") is True for c in checks):
        raise ValueError("snapshot must pass quality checks")
    summary = quality_summary(data_dir / "warehouse" / "dpd.duckdb", snapshot)
    if any(receipt.get(key) != value for key, value in summary.items()):
        raise ValueError("run receipt does not match current snapshot")
    duration = receipt["duration_seconds"]
    if not isinstance(duration, (float, int)) or not math.isfinite(duration) or duration < 0:
        raise ValueError("invalid run duration")
    return dict(summary, duration_seconds=duration, source_kind=receipt["source_kind"])
