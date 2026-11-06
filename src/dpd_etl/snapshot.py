"""Shared, checked Spark frames for local databases and notebook jobs."""

from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession

from . import quality
from .config import Thresholds
from .monitoring import Monitor
from .transform import (
    build_drug_products,
    build_status_scd2,
    clean_atc,
    clean_companies,
    clean_drugs,
    clean_ingredients,
    clean_status,
    read_raw,
)


@contextmanager
def checked_snapshot(
    spark: SparkSession, raw_dir: Path, snapshot_date: str, thresholds: Thresholds,
    previous_rows: int | None, report_path: Path, monitor: Monitor,
):
    """Yield products, history and ingredients only after enforcing the quality gate."""
    cached = []

    def keep(frame):
        cached.append(frame.cache())
        return frame

    try:
        raw_drug = read_raw(spark, raw_dir, "drug")
        drugs = keep(clean_drugs(raw_drug))
        status = keep(build_status_scd2(clean_status(read_raw(spark, raw_dir, "status"))))
        ingredients = keep(clean_ingredients(read_raw(spark, raw_dir, "ingred")))
        products = keep(build_drug_products(
            drugs, status, clean_companies(read_raw(spark, raw_dir, "comp")),
            clean_atc(read_raw(spark, raw_dir, "ther")), ingredients, snapshot_date,
        ))
        checks = quality.run_checks(
            raw_drug_rows=raw_drug.count(), drugs=drugs, status_scd2=status,
            ingredients=ingredients, products=products, thresholds=thresholds,
            previous_rows=previous_rows,
        )
        monitor.quality(checks)
        quality.enforce(checks, report_path)
        yield products, status, ingredients
    finally:
        for frame in reversed(cached):
            frame.unpersist()
