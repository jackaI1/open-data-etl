import os

import duckdb
import pytest
from pymongo import MongoClient
from pymongo.errors import WriteError

from dpd_etl.mongo import publish


@pytest.fixture
def data(tmp_path):
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
    return db


def test_real_mongodb_schema_rerun_and_sql_reconciliation(data):
    uri = os.environ.get("MONGO_TEST_URI")
    if not uri:
        pytest.skip("set MONGO_TEST_URI to run against MongoDB")
    with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        database = client.dpd_mongo_integration_test
        client.drop_database(database.name)
        try:
            assert publish(data, "2026-01-01", database) == {"A": 1}
            assert publish(data, "2026-01-01", database) == {"A": 1}
            assert database.products.count_documents({}) == 2
            assert (
                database.products.find_one({"drug_code": 1})["review_flags"][0]["reason"] == "high"
            )
            with pytest.raises(WriteError):
                database.products.insert_one({"drug_code": "wrong"})
            with duckdb.connect(str(data)) as con:
                con.execute("delete from drug_products where drug_code=2")
            publish(data, "2026-01-01", database)
            assert database.products.count_documents({}) == 1
        finally:
            client.drop_database(database.name)
