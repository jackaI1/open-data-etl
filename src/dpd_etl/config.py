"""Run configuration and data-quality thresholds."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DPD_URL = (
    "https://www.canada.ca/content/dam/hc-sc/documents/services/drug-product-database/allfiles.zip"
)


@dataclass(frozen=True)
class Thresholds:
    """Limits that, when breached, fail the run."""

    min_drug_rows: int = 10_000  # the real DPD extract has ~13k products
    max_null_ratio_brand: float = 0.0
    max_null_ratio_din: float = 0.02
    max_null_ratio_company: float = 0.01
    max_null_ratio_status: float = 0.0
    max_orphan_ratio: float = 0.001  # child rows with no parent drug
    max_row_drop_vs_previous: float = 0.05  # vs. previous snapshot in DuckDB
    max_dedup_ratio: float = 0.01


FIXTURE_THRESHOLDS = Thresholds(
    min_drug_rows=20,
    max_null_ratio_din=0.1,
    max_null_ratio_company=0.1,
    max_dedup_ratio=0.02,
)


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def parquet(self) -> Path:
        return self.root / "parquet"

    @property
    def duckdb(self) -> Path:
        return self.root / "warehouse" / "dpd.duckdb"

    @property
    def reports(self) -> Path:
        return self.root / "reports"
