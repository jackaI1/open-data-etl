"""Load an accepted snapshot, run parameterized SQL and reconcile with Spark outputs."""

import argparse
import json
from importlib.resources import files
from pathlib import Path

from google.cloud import bigquery as bq

from .bigquery import load_snapshot, validate
from .summary import accepted_summary


def health_checks(client, project, dataset, snapshot, *, expected=None,
                  maximum_bytes_billed=1024**3):
    validate(project, dataset, snapshot)
    if maximum_bytes_billed < 1:
        raise ValueError("maximum bytes billed must be positive")
    sql = (files("dpd_etl").joinpath("sql/health_checks.sql").read_text()
           .replace("{{dataset}}", f"{project}.{dataset}"))
    config = bq.QueryJobConfig(
        use_legacy_sql=False, use_query_cache=False, maximum_bytes_billed=maximum_bytes_billed,
        query_parameters=[bq.ScalarQueryParameter("snapshot_date", "STRING", snapshot),
                          bq.ScalarQueryParameter("z_threshold", "FLOAT64", 3.5)],
    )
    rows = list(client.query(sql, job_config=config).result(timeout=120))
    if len(rows) != 1 or not rows[0]["rows_processed"]:
        raise ValueError("health query returned no products for the requested date")
    result = dict(rows[0])
    for actual, recomputed in [("flagged_strengths", "recomputed_strength_flags"),
                                ("duplicate_pairs", "recomputed_duplicate_pairs")]:
        if result[actual] != result[recomputed]:
            raise ValueError(f"SQL detection mismatch: {actual}")
    if expected is not None:
        for key, value in result.items():
            if key in expected and abs(value - expected[key]) > 1e-9:
                raise ValueError(f"Spark output mismatch: {key}")
    return result


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", type=Path, required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", default="dpd_review")
    ap.add_argument("--snapshot-date", required=True)
    ap.add_argument("--location", default="US")
    args = ap.parse_args(argv)
    expected = accepted_summary(args.data_dir, args.snapshot_date)
    with bq.Client(project=args.project, location=args.location) as client:
        load_snapshot(client, args.data_dir / "parquet", args.project, args.dataset,
                      args.snapshot_date, args.location)
        print(json.dumps(health_checks(client, args.project, args.dataset, args.snapshot_date,
                                       expected=expected), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
