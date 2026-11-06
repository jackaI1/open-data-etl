"""Seed three clearly labelled example runs through the same accepted-output reader.

The invented values exercise flags, trends and review states without downloading
any catalogue. Use dpd_etl.dashboard for real accepted pipeline snapshots.
"""

import argparse
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from dpd_etl.dashboard import publish
from dpd_etl.summary import quality_summary


def seed(destination):
    with TemporaryDirectory(prefix="dpd-dashboard-seed-") as work:
        root = Path(work)
        (root / "warehouse").mkdir()
        (root / "reports").mkdir()
        db = root / "warehouse" / "dpd.duckdb"
        with duckdb.connect(str(db)) as con:
            con.execute("""CREATE TABLE drug_products (snapshot_date DATE, drug_code BIGINT,
                din VARCHAR, brand_name VARCHAR, company_name VARCHAR,
                drug_class VARCHAR, atc_group VARCHAR);
                CREATE TABLE drug_ingredients (snapshot_date DATE, drug_code BIGINT);
                CREATE TABLE strength_anomalies (snapshot_date DATE, drug_code BIGINT,
                                                is_anomaly BOOLEAN);
                CREATE TABLE product_links (snapshot_date DATE, left_id BIGINT,
                    right_id BIGINT, score DOUBLE, is_match BOOLEAN, reason VARCHAR)""")
            for day, flags in [("2026-11-01", 2), ("2026-11-02", 1), ("2026-11-03", 0)]:
                for code in (1, 2, 3):
                    con.execute("INSERT INTO drug_products VALUES (?, ?, ?, ?, ?, ?, ?)",
                                [day, code, f"{code:08d}", f"Example product {code}",
                                 "Example maker", "Human" if code < 3 else "Veterinary", "N"])
                    con.execute("INSERT INTO drug_ingredients VALUES (?, ?)", [day, code])
                    con.execute("INSERT INTO strength_anomalies VALUES (?, ?, ?)",
                                [day, code, code <= flags])
                con.execute("INSERT INTO product_links VALUES (?, 1, 2, 1.0, true, 'exact_din')",
                            [day])
        decisions = root / "decisions.json"
        decisions.write_text(json.dumps([
            {"snapshot_date": "2026-11-02", "left_id": 1, "right_id": 2, "review_status": "confirmed"},
            {"snapshot_date": "2026-11-03", "left_id": 1, "right_id": 2, "review_status": "dismissed"}]))
        for day, duration in [("2026-11-01", 12.5), ("2026-11-02", 10.0), ("2026-11-03", 9.0)]:
            summary = quality_summary(db, day)
            summary.update(duration_seconds=duration, source_kind="example")
            (root / "reports" / f"run_{day}.json").write_text(json.dumps(summary))
            (root / "reports" / f"dq_{day}.json").write_text(json.dumps([{"passed": True}]))
            publish(root, day, destination, decisions)
    return {"example_runs": 3, "database": str(destination)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--database", type=Path, default=Path("data/superset/catalogue.db"))
    print(json.dumps(seed(ap.parse_args().database)))
