"""Publish accepted catalogue outputs to the small SQL database used by Superset."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import duckdb

from .summary import accepted_summary

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
 snapshot_date DATE PRIMARY KEY, rows_processed INTEGER NOT NULL,
 flagged_records INTEGER NOT NULL, flagged_strengths INTEGER NOT NULL,
 duplicate_pairs INTEGER NOT NULL, duration_seconds REAL NOT NULL, source_kind TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS products (
 snapshot_date DATE NOT NULL, drug_code INTEGER NOT NULL, din TEXT, brand_name TEXT,
 drug_class TEXT, atc_group TEXT, ingredient_rows INTEGER NOT NULL,
 flagged_strengths INTEGER NOT NULL, flagged_record INTEGER NOT NULL,
 PRIMARY KEY(snapshot_date, drug_code)
);
CREATE TABLE IF NOT EXISTS pairs (
 snapshot_date DATE NOT NULL, left_id INTEGER NOT NULL, right_id INTEGER NOT NULL,
 score REAL, is_match INTEGER NOT NULL, reason TEXT, review_status TEXT NOT NULL,
 PRIMARY KEY(snapshot_date, left_id, right_id)
);
"""


def publish(data_dir: Path, snapshot: str, destination: Path, decisions: Path | None = None):
    summary = accepted_summary(data_dir, snapshot)
    reviews = {}
    if decisions:
        for row in json.loads(decisions.read_text()):
            if row["review_status"] not in {"pending", "confirmed", "dismissed"}:
                raise ValueError("invalid review status")
            key = (row["snapshot_date"], int(row["left_id"]), int(row["right_id"]))
            if key in reviews:
                raise ValueError("duplicate review decision")
            reviews[key] = row["review_status"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(data_dir / "warehouse" / "dpd.duckdb"), read_only=True) as source:
        source.execute("SET threads=2")
        # Aggregate each child independently before joining to the product table.
        product_query = """WITH ing AS (
            SELECT drug_code, count(*) ingredient_rows FROM drug_ingredients
            WHERE snapshot_date = ? GROUP BY drug_code
          ), flags AS (
            SELECT drug_code, count(*) flagged_strengths FROM strength_anomalies
            WHERE snapshot_date = ? AND is_anomaly GROUP BY drug_code
          ) SELECT p.snapshot_date, p.drug_code, p.din, p.brand_name,
            coalesce(p.drug_class, 'Unknown'), coalesce(p.atc_group, 'Unknown'),
            coalesce(i.ingredient_rows, 0), coalesce(f.flagged_strengths, 0),
            CASE WHEN coalesce(f.flagged_strengths, 0) > 0 THEN 1 ELSE 0 END
          FROM drug_products p LEFT JOIN ing i USING (drug_code)
          LEFT JOIN flags f USING (drug_code) WHERE p.snapshot_date = ?"""
        with sqlite3.connect(destination) as target:
            target.executescript(SCHEMA)
            # One SQLite transaction publishes the entire date; older dates survive.
            for table in ("runs", "products", "pairs"):
                target.execute(f"DELETE FROM {table} WHERE snapshot_date = ?", [snapshot])
            target.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?)", [summary[c] for c in (
                "snapshot_date", "rows_processed", "flagged_records", "flagged_strengths",
                "duplicate_pairs", "duration_seconds", "source_kind")])
            cursor = source.execute(product_query, [snapshot] * 3)
            while rows := cursor.fetchmany(500):
                target.executemany("INSERT INTO products VALUES (?,?,?,?,?,?,?,?,?)",
                                   [(str(r[0]), *r[1:]) for r in rows])
            cursor = source.execute("""SELECT left_id, right_id, score, is_match, reason
                FROM product_links WHERE snapshot_date = ?""", [snapshot])
            seen = set()
            while rows := cursor.fetchmany(500):
                records = []
                for row in rows:
                    key = (snapshot, int(row[0]), int(row[1]))
                    seen.add(key)
                    records.append((snapshot, *row, reviews.get(key, "pending")))
                target.executemany("INSERT INTO pairs VALUES (?,?,?,?,?,?,?)", records)
            if any(k[0] == snapshot and k not in seen for k in reviews):
                raise ValueError("decision refers to a missing pair")
    return summary


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", type=Path, required=True)
    ap.add_argument("--snapshot-date", required=True)
    ap.add_argument("--database", type=Path, default=Path("data/superset/catalogue.db"))
    ap.add_argument("--decisions", type=Path)
    args = ap.parse_args(argv)
    print(json.dumps(publish(args.data_dir, args.snapshot_date, args.database, args.decisions),
                     indent=2))


if __name__ == "__main__":
    main()
