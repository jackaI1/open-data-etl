"""Load: Parquet partitioned by snapshot_date + DuckDB tables for SQL queries."""

from __future__ import annotations

from pathlib import Path

import duckdb
from pyspark.sql import DataFrame


def write_parquet(df: DataFrame, root: Path, table: str, snapshot_date: str | None = None) -> Path:
    """Write a table partitioned by snapshot_date; dynamic overwrite makes reruns idempotent."""
    out = root / table
    if snapshot_date is not None and df.isEmpty():
        # Spark writes no partition for an empty result. A typed empty file makes
        # no-candidate snapshots loadable and replaces stale candidates on rerun.
        df.drop("snapshot_date").write.mode("overwrite").parquet(
            str(out / f"snapshot_date={snapshot_date}")
        )
    else:
        df.write.mode("overwrite").partitionBy("snapshot_date").parquet(str(out))
    return out


def previous_row_count(db_path: Path, snapshot_date: str) -> int | None:
    """Row count of the latest earlier snapshot, for the row-drop quality check."""
    if not db_path.exists():
        return None
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            "SELECT count(*) FROM drug_products WHERE snapshot_date = "
            "(SELECT max(snapshot_date) FROM drug_products WHERE snapshot_date < ?)",
            [snapshot_date],
        ).fetchone()
    except duckdb.CatalogException:
        return None
    finally:
        con.close()
    return row[0] if row and row[0] else None


def load_duckdb(db_path: Path, parquet_root: Path, snapshot_date: str, tables: list[str]) -> None:
    """Replace this snapshot's rows in DuckDB from the Parquet output (idempotent)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("BEGIN TRANSACTION")
        for t in tables:
            glob = f"{parquet_root / t}/snapshot_date={snapshot_date}/*.parquet"
            table = '"' + t.replace('"', '""') + '"'
            src = "SELECT *, CAST(? AS DATE) AS snapshot_date FROM read_parquet(?, hive_partitioning=false)"
            exists = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [t]
            ).fetchone()[0]
            if exists:
                con.execute(
                    f"DELETE FROM {table} WHERE snapshot_date = CAST(? AS DATE)", [snapshot_date]
                )
                con.execute(f"INSERT INTO {table} BY NAME {src}", [snapshot_date, glob])
            else:
                con.execute(f"CREATE TABLE {table} AS {src}", [snapshot_date, glob])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
