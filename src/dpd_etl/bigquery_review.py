"""Preview query bytes or print the first 100 products needing review."""

import argparse
import json
from importlib.resources import files

from google.cloud import bigquery as bq

from .bigquery import validate


def review_query(client, project, dataset, snapshot, *, dry_run=False,
                 maximum_bytes_billed=1024**3):
    validate(project, dataset, snapshot)
    sql = (files("dpd_etl").joinpath("sql/review_queue.sql").read_text()
           .replace("{{dataset}}", f"{project}.{dataset}")
           .replace("{{day}}", snapshot.replace("-", "")))
    config = bq.QueryJobConfig(dry_run=dry_run, use_query_cache=False,
                               maximum_bytes_billed=maximum_bytes_billed)
    job = client.query(sql, job_config=config)
    rows = [] if dry_run else [dict(row) for row in job.result()]
    return {"dry_run": dry_run, "bytes_processed": job.total_bytes_processed, "rows": rows}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", default="dpd_review")
    ap.add_argument("--snapshot-date", required=True)
    ap.add_argument("--location", default="US")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--maximum-bytes-billed", type=int, default=1024**3)
    args = ap.parse_args(argv)
    validate(args.project, args.dataset, args.snapshot_date)
    if args.maximum_bytes_billed < 1:
        ap.error("maximum bytes billed must be positive")
    with bq.Client(project=args.project, location=args.location) as client:
        print(json.dumps(review_query(client, args.project, args.dataset, args.snapshot_date,
                                      dry_run=args.dry_run,
                                      maximum_bytes_billed=args.maximum_bytes_billed),
                         default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
