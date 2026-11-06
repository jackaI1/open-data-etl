"""Optional BigQuery destination for quality-checked Parquet snapshots."""

from __future__ import annotations

import argparse
import json
import re
import uuid
from contextlib import ExitStack
from datetime import date
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryFile

import duckdb
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery as bq

# No autodetection: DINs and strengths stay strings (leading zeros and units matter).
FIELDS = {
    "drug_products": {
        "drug_code": "INTEGER", "product_categorization": "STRING", "drug_class": "STRING",
        "din": "STRING", "brand_name": "STRING", "descriptor": "STRING",
        "is_pediatric": "BOOLEAN", "number_of_ais": "INTEGER", "last_update_date": "DATE",
        "current_status": "STRING", "current_status_since": "DATE",
        "first_approved_date": "DATE", "company_code": "STRING", "company_name": "STRING",
        "company_province": "STRING", "atc_code": "STRING", "atc_name": "STRING",
        "active_ingredients": "STRING", "atc_group": "STRING",
    },
    "drug_ingredients": {
        "drug_code": "INTEGER", "ingredient": "STRING", "strength": "STRING",
        "strength_unit": "STRING",
    },
    "strength_anomalies": {
        "drug_code": "INTEGER", "ingredient": "STRING", "strength": "STRING",
        "strength_unit": "STRING", "numeric_strength": "FLOAT",
        "peer_count": "INTEGER", "peer_median": "FLOAT", "peer_mad": "FLOAT",
        "reason": "STRING", "robust_z": "FLOAT", "is_evaluated": "BOOLEAN",
        "is_anomaly": "BOOLEAN",
    },
}
FIELDS["product_links"] = {
    "left_id": "INTEGER", "right_id": "INTEGER", "left_din": "STRING", "right_din": "STRING",
    "brand_similarity": "FLOAT", "company_similarity": "FLOAT", "score": "FLOAT",
    "is_match": "BOOLEAN", "reason": "STRING",
}
CLUSTERS = {
    "product_links": ["is_match", "left_id", "right_id"],
    "drug_products": ["atc_group", "company_name", "drug_code"],
    "drug_ingredients": ["ingredient", "drug_code"],
    "strength_anomalies": ["is_anomaly", "ingredient", "drug_code"],
}


def validate(project: str, dataset: str, snapshot: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", project):
        raise ValueError("project must be a Google Cloud project ID")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,1023}", dataset):
        raise ValueError("dataset must contain only letters, digits and underscores")
    if date.fromisoformat(snapshot).isoformat() != snapshot:
        raise ValueError("snapshot must be YYYY-MM-DD")


def schema(table: str) -> list[bq.SchemaField]:
    return [bq.SchemaField(name, kind) for name, kind in FIELDS[table].items()] + [
        bq.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bq.SchemaField("load_id", "STRING", mode="REQUIRED"),
    ]


def table_definition(project: str, dataset: str, table: str) -> bq.Table:
    obj = bq.Table(f"{project}.{dataset}.{table}", schema=schema(table))
    obj.time_partitioning = bq.TimePartitioning(type_="DAY", field="snapshot_date")
    obj.require_partition_filter = True
    obj.clustering_fields = CLUSTERS[table]
    return obj


def review_sql(project: str, dataset: str, snapshot: str, load_id: str) -> str:
    validate(project, dataset, snapshot)
    if not re.fullmatch(r"[a-f0-9]{32}", load_id):
        raise ValueError("invalid load ID")
    return (files("dpd_etl").joinpath("sql/reviewer_summary.sql").read_text()
            .replace("{{dataset}}", f"{project}.{dataset}")
            .replace("{{snapshot}}", snapshot).replace("{{load_id}}", load_id))


def _export(root: Path, table: str, snapshot: str, load_id: str, output) -> int:
    """Validate and stream selected columns to disk, not a whole catalogue in RAM."""
    sources = sorted((root / table / f"snapshot_date={snapshot}").glob("*.parquet"))
    if not sources:
        raise ValueError(f"missing Parquet partition for {table}: {snapshot}")
    with duckdb.connect() as con:
        con.execute("SET threads=2")
        relation = con.read_parquet([str(p) for p in sources], hive_partitioning=False)
        if set(relation.columns) != set(FIELDS[table]):
            raise ValueError(f"unexpected columns in {table}: {relation.columns}")
        # Read by name. Spark's physical column order differs between tables.
        con.register("source", relation)
        cursor = con.execute("SELECT " + ", ".join(FIELDS[table]) + " FROM source")
        count = 0
        while batch := cursor.fetchmany(1000):
            for row in batch:
                record = dict(zip(FIELDS[table], row))
                record.update(snapshot_date=snapshot, load_id=load_id)
                keys = ["left_id", "right_id"] if table == "product_links" else ["drug_code"]
                if any(record[key] is None for key in keys):
                    raise ValueError(f"null product key in {table}")
                # Reject NaN/infinity instead of uploading invalid JSON.
                output.write((json.dumps(record, default=str, allow_nan=False) + "\n").encode())
                count += 1
        if table == "drug_products" and not count:
            raise ValueError("cannot publish an empty product snapshot")
        output.seek(0)
        return count


def load_snapshot(client: bq.Client, root: Path, project: str, dataset: str,
                  snapshot: str, location: str = "US") -> dict:
    """Overwrite only the chosen day; install its review view after all jobs succeed.

    Serialize writers for the same dataset/date. A failed rerun can temporarily
    hide that day's old review view; rerunning repairs it without duplicating rows.
    """
    validate(project, dataset, snapshot)
    load_id = uuid.uuid4().hex
    counts, jobs = {}, {}
    with ExitStack() as stack:
        payloads = {t: stack.enter_context(TemporaryFile()) for t in FIELDS}
        # Preflight ALL files before any remote writes.
        for table in FIELDS:
            counts[table] = _export(root, table, snapshot, load_id, payloads[table])
        ds = bq.Dataset(f"{project}.{dataset}")
        ds.location = location
        try:
            actual_dataset = client.get_dataset(ds)
        except NotFound:
            actual_dataset = client.create_dataset(ds, exists_ok=True)
        if actual_dataset.location.upper() != location.upper():
            raise ValueError("dataset location differs from requested location")
        for table in FIELDS:
            expected = table_definition(project, dataset, table)
            actual = client.create_table(expected, exists_ok=True)
            if (actual.schema != expected.schema
                    or actual.time_partitioning != expected.time_partitioning
                    or actual.clustering_fields != expected.clustering_fields
                    or not actual.require_partition_filter):
                raise ValueError(f"incompatible BigQuery table: {table}; use a new dataset")
            target = f"{project}.{dataset}.{table}${snapshot.replace('-', '')}"
            config = bq.LoadJobConfig(
                schema=schema(table), source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bq.CreateDisposition.CREATE_NEVER,
                max_bad_records=0, ignore_unknown_values=False,
            )
            job_id = f"dpd_{table}_{snapshot.replace('-', '')}_{load_id}"
            try:
                job = client.load_table_from_file(
                    payloads[table], target, job_config=config,
                    job_id=job_id, location=location, rewind=True,
                )
            except Conflict:
                job = client.get_job(job_id, location=location)
            job.result()
            jobs[table] = job.job_id
        view_id = f"{project}.{dataset}.reviewer_summary_{snapshot.replace('-', '')}"
        view = bq.Table(view_id)
        view.view_query = review_sql(project, dataset, snapshot, load_id)
        view.view_use_legacy_sql = False
        try:
            client.create_table(view)
        except Conflict:
            client.update_table(view, ["view_query", "view_use_legacy_sql"])
    return {"snapshot_date": snapshot, "rows": counts, "jobs": jobs, "view": view_id}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--parquet-root", type=Path, required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", default="dpd_review")
    ap.add_argument("--snapshot-date", required=True)
    ap.add_argument("--location", default="US")
    args = ap.parse_args(argv)
    validate(args.project, args.dataset, args.snapshot_date)
    # Uses application default credentials. No keys or emulator switches in production.
    with bq.Client(project=args.project, location=args.location) as client:
        print(json.dumps(load_snapshot(client, args.parquet_root, args.project,
                                       args.dataset, args.snapshot_date, args.location), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
