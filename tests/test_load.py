import datetime as dt

import duckdb
import pytest

from dpd_etl.load import load_duckdb


def test_load_failure_rolls_back_all_tables_and_handles_quoted_paths(tmp_path):
    root = tmp_path / "Jack's data"
    part = root / "drug_products" / "snapshot_date=2026-11-01"
    part.mkdir(parents=True)
    source = part / "part.parquet"
    db = tmp_path / "dpd.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute("create table drug_products (drug_code integer, snapshot_date date)")
        con.execute("insert into drug_products values (1, DATE '2026-11-01')")
        con.execute("copy (select 2 as drug_code) to ? (format parquet)", [str(source)])
    with pytest.raises(duckdb.Error):
        load_duckdb(db, root, "2026-11-01", ["drug_products", "missing_table"])
    with duckdb.connect(str(db), read_only=True) as con:
        assert con.execute("select * from drug_products").fetchall() == [(1, dt.date(2026, 11, 1))]
    # A successful import must work in a path containing a quote, too.
    load_duckdb(db, root, "2026-11-01", ["drug_products"])
    with duckdb.connect(str(db), read_only=True) as con:
        assert con.execute("select drug_code from drug_products").fetchall() == [(2,)]


def test_duckdb_load_has_exactly_one_snapshot_date_column(tmp_path):
    root = tmp_path / "parquet"
    part = root / "drug_products" / "snapshot_date=2026-11-01"
    part.mkdir(parents=True)
    with duckdb.connect() as con:
        con.execute(
            "copy (select 7 as drug_code) to ? (format parquet)", [str(part / "part.parquet")]
        )
    db = tmp_path / "dpd.duckdb"
    load_duckdb(db, root, "2026-11-01", ["drug_products"])
    with duckdb.connect(str(db), read_only=True) as con:
        assert con.execute("select * from drug_products").fetchall() == [(7, dt.date(2026, 11, 1))]
        assert [r[0] for r in con.execute("describe drug_products").fetchall()] == [
            "drug_code",
            "snapshot_date",
        ]
