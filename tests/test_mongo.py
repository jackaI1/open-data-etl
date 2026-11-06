import duckdb
import pytest

from dpd_etl.mongo import documents, sql_summary


def test_documents_nest_only_matching_rows_and_preserve_flags(tmp_path):
    db = tmp_path / "sample.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute(
            "create table drug_products (drug_code integer, brand_name varchar, snapshot_date date)"
        )
        con.execute(
            "insert into drug_products values (1, 'ONE', '2026-01-01'), (2, 'TWO', '2026-01-01')"
        )
        con.execute(
            "create table drug_ingredients (drug_code integer, ingredient varchar, strength varchar, strength_unit varchar, snapshot_date date)"
        )
        con.execute("insert into drug_ingredients values (1, 'A', '10', 'MG', '2026-01-01')")
        con.execute(
            "create table strength_anomalies (drug_code integer, ingredient varchar, strength varchar, strength_unit varchar, is_anomaly boolean, reason varchar, snapshot_date date)"
        )
        con.execute(
            "insert into strength_anomalies values (1, 'A', '10', 'MG', true, 'high', '2026-01-01')"
        )
    rows = list(documents(db, "2026-01-01"))
    assert len(rows) == 2
    assert rows[0]["ingredients"][0]["ingredient"] == "A"
    assert rows[0]["review_flags"][0]["reason"] == "high"
    assert rows[1]["ingredients"] == [] and rows[1]["review_flags"] == []
    assert sql_summary(db, "2026-01-01") == {"A": 1}


def test_rejects_invalid_date(tmp_path):
    with pytest.raises(ValueError):
        list(documents(tmp_path / "none", "not-a-date"))
