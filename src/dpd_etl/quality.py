"""Data-quality gate. Any failed check raises DataQualityError, failing the run."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Thresholds


class DataQualityError(RuntimeError):
    pass


@dataclass
class Check:
    name: str
    passed: bool
    observed: float
    limit: str


def _null_ratio(df: DataFrame, col: str, total: int) -> float:
    return df.where(F.col(col).isNull()).count() / total if total else 1.0


def _orphan_ratio(child: DataFrame, parent: DataFrame) -> float:
    total = child.count()
    if not total:
        return 0.0
    orphans = child.select("drug_code").join(parent.select("drug_code"), "drug_code", "left_anti")
    return orphans.count() / total


def run_checks(
    *,
    raw_drug_rows: int,
    drugs: DataFrame,
    status_scd2: DataFrame,
    ingredients: DataFrame,
    products: DataFrame,
    thresholds: Thresholds,
    previous_rows: int | None,
) -> list[Check]:
    n = drugs.count()
    checks: list[Check] = []

    def add(name: str, passed: bool, observed: float, limit: str) -> None:
        checks.append(Check(name, bool(passed), round(float(observed), 6), limit))

    add("row_count_min", n >= thresholds.min_drug_rows, n, f">= {thresholds.min_drug_rows}")
    dupes = raw_drug_rows - n
    add(
        "raw_vs_deduped_rows",
        dupes / raw_drug_rows <= thresholds.max_dedup_ratio if raw_drug_rows else False,
        dupes,
        f"<= {thresholds.max_dedup_ratio:.0%} of raw rows removed by dedup",
    )
    add(
        "drug_code_unique",
        products.count() == products.select("drug_code").distinct().count(),
        products.count() - products.select("drug_code").distinct().count(),
        "== 0 duplicates",
    )
    for col, limit in [
        ("brand_name", thresholds.max_null_ratio_brand),
        ("din", thresholds.max_null_ratio_din),
        ("last_update_date", 0.0),
    ]:
        r = _null_ratio(drugs, col, n)
        add(f"null_ratio_{col}", r <= limit, r, f"<= {limit}")
    r = _null_ratio(products, "company_name", n)
    add(
        "null_ratio_company_name",
        r <= thresholds.max_null_ratio_company,
        r,
        f"<= {thresholds.max_null_ratio_company}",
    )
    r = _null_ratio(products, "current_status", n)
    add(
        "null_ratio_current_status",
        r <= thresholds.max_null_ratio_status,
        r,
        f"<= {thresholds.max_null_ratio_status}",
    )
    for name, child in [("status", status_scd2), ("ingredients", ingredients)]:
        r = _orphan_ratio(child, drugs)
        add(
            f"orphan_ratio_{name}",
            r <= thresholds.max_orphan_ratio,
            r,
            f"<= {thresholds.max_orphan_ratio}",
        )
    r = _null_ratio(status_scd2, "valid_from", status_scd2.count())
    add("null_ratio_status_valid_from", r == 0.0, r, "== 0")
    multi_current = (
        status_scd2.where("is_current").groupBy("drug_code").count().where("count > 1").count()
    )
    add("scd2_single_current_version", multi_current == 0, multi_current, "== 0")
    bad_interval = status_scd2.where(
        F.col("valid_to").isNotNull() & (F.col("valid_to") < F.col("valid_from"))
    ).count()
    add("scd2_valid_intervals", bad_interval == 0, bad_interval, "== 0 (valid_to >= valid_from)")
    if previous_rows:
        drop = max(0.0, (previous_rows - n) / previous_rows)
        add(
            "row_drop_vs_previous_snapshot",
            drop <= thresholds.max_row_drop_vs_previous,
            drop,
            f"<= {thresholds.max_row_drop_vs_previous}",
        )
    return checks


def enforce(checks: list[Check], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps([asdict(c) for c in checks], indent=2) + "\n")
    failed = [c for c in checks if not c.passed]
    for c in checks:
        print(f"[dq] {'PASS' if c.passed else 'FAIL'} {c.name}: {c.observed} (limit {c.limit})")
    if failed:
        raise DataQualityError("data-quality checks failed: " + ", ".join(c.name for c in failed))
