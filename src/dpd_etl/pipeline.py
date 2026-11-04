"""Orchestrates transform -> quality gate -> load for one snapshot."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from . import quality
from .config import DPD_URL, FIXTURE_THRESHOLDS, Paths, Thresholds
from .extract import extract
from .load import load_duckdb, previous_row_count, write_parquet
from .transform import (
    build_drug_products,
    build_status_scd2,
    clean_atc,
    clean_companies,
    clean_drugs,
    clean_ingredients,
    clean_status,
    get_spark,
    read_raw,
)

TABLES = ["drug_products", "drug_status_history", "drug_ingredients"]


def run(raw_dir: Path, out_root: Path, snapshot_date: str, thresholds: Thresholds) -> None:
    paths = Paths(out_root)
    spark = get_spark()
    try:
        raw_drug = read_raw(spark, raw_dir, "drug")
        raw_drug_rows = raw_drug.count()
        drugs = clean_drugs(raw_drug).cache()
        status = build_status_scd2(clean_status(read_raw(spark, raw_dir, "status"))).cache()
        ingredients = clean_ingredients(read_raw(spark, raw_dir, "ingred")).cache()
        products = build_drug_products(
            drugs,
            status,
            clean_companies(read_raw(spark, raw_dir, "comp")),
            clean_atc(read_raw(spark, raw_dir, "ther")),
            ingredients,
            snapshot_date,
        ).cache()

        checks = quality.run_checks(
            raw_drug_rows=raw_drug_rows,
            drugs=drugs,
            status_scd2=status,
            ingredients=ingredients,
            products=products,
            thresholds=thresholds,
            previous_rows=previous_row_count(paths.duckdb, snapshot_date),
        )
        # Gate: raises (non-zero exit) BEFORE anything is written to Parquet / DuckDB.
        quality.enforce(checks, paths.reports / f"dq_{snapshot_date}.json")

        from pyspark.sql import functions as F

        snap = F.lit(snapshot_date).cast("date")
        write_parquet(products, paths.parquet, "drug_products")
        write_parquet(
            status.withColumn("snapshot_date", snap), paths.parquet, "drug_status_history"
        )
        write_parquet(
            ingredients.withColumn("snapshot_date", snap), paths.parquet, "drug_ingredients"
        )
        load_duckdb(paths.duckdb, paths.parquet, snapshot_date, TABLES)
        print(f"[load] {products.count()} products -> {paths.parquet} and {paths.duckdb}")
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="dpd_etl")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("extract", help="download + checksum + unpack the DPD bulk files")
    e.add_argument("--data-dir", default="data")
    e.add_argument("--url", default=DPD_URL)
    e.add_argument("--sha256", default=None, help="fail if the download does not match")
    r = sub.add_parser("transform", help="transform, quality-check and load a snapshot")
    r.add_argument("--raw-dir", default="data/raw/files")
    r.add_argument("--data-dir", default="data")
    r.add_argument("--snapshot-date", default=date.today().isoformat())
    r.add_argument("--fixture", action="store_true", help="use relaxed thresholds for tiny data")
    args = ap.parse_args(argv)

    if args.cmd == "transform":
        try:
            if date.fromisoformat(args.snapshot_date).isoformat() != args.snapshot_date:
                raise ValueError
        except ValueError:
            ap.error("--snapshot-date must be a valid YYYY-MM-DD date")

    if args.cmd == "extract":
        extract(Path(args.data_dir) / "raw", args.url, args.sha256)
        return 0
    try:
        run(
            Path(args.raw_dir),
            Path(args.data_dir),
            args.snapshot_date,
            FIXTURE_THRESHOLDS if args.fixture else Thresholds(),
        )
    except quality.DataQualityError as exc:
        print(f"[dq] RUN FAILED: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
