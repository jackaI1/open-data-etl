"""Write the CSV extract from a small warehouse with the same tables and column names as dbt."""

import csv
import json
import os
import re
from uuid import uuid4

import duckdb
import pytest

from dpd_etl import sql_extract, sqlserver

SNAPSHOT = "2026-11-01"


@pytest.fixture()
def warehouse(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA analytics")
        con.execute("""CREATE TABLE analytics.dim_company (company_key VARCHAR, snapshot_date DATE,
            company_code VARCHAR, company_name VARCHAR)""")
        con.execute("""CREATE TABLE analytics.dim_ingredient (ingredient_key VARCHAR, ingredient VARCHAR)""")
        con.execute("""CREATE TABLE analytics.dim_product (product_key VARCHAR, snapshot_date DATE,
            drug_code INTEGER, din VARCHAR, brand_name VARCHAR, drug_class VARCHAR, atc_group VARCHAR,
            current_status VARCHAR, company_key VARCHAR)""")
        con.execute("""CREATE TABLE analytics.fct_product (product_key VARCHAR, company_key VARCHAR,
            snapshot_date DATE, drug_class VARCHAR, ingredient_records BIGINT, flagged_strengths BIGINT)""")
        con.execute("""CREATE TABLE analytics.fct_product_ingredient (product_ingredient_key VARCHAR,
            product_key VARCHAR, ingredient_key VARCHAR, snapshot_date DATE, strength VARCHAR,
            strength_unit VARCHAR)""")
        con.execute("""CREATE TABLE analytics.fct_duplicate_pair (pair_key VARCHAR, left_product_key VARCHAR,
            right_product_key VARCHAR, snapshot_date DATE, score DOUBLE, is_match BOOLEAN, reason VARCHAR)""")
        con.execute("INSERT INTO analytics.dim_company VALUES ('c1', DATE '2026-11-01', NULL, 'Acme, Inc.')")
        con.execute("INSERT INTO analytics.dim_ingredient VALUES ('i1', 'ACETAMINOPHEN')")
        con.execute("""INSERT INTO analytics.dim_product VALUES
            ('p1', DATE '2026-11-01', 1, '00000001', 'Brand "Quoted"', 'HUMAN', NULL, 'MARKETED', 'c1')""")
        con.execute("INSERT INTO analytics.fct_product VALUES ('p1', 'c1', DATE '2026-11-01', 'HUMAN', 1, 0)")
        con.execute("INSERT INTO analytics.fct_product_ingredient VALUES ('p1|i1', 'p1', 'i1', DATE '2026-11-01', '500', 'MG')")
        con.execute("INSERT INTO analytics.fct_duplicate_pair VALUES ('p1|p1', 'p1', 'p1', DATE '2026-11-01', 0.9, true, 'exact_din')")
        con.execute("INSERT INTO analytics.fct_duplicate_pair VALUES ('p1|p2', 'p1', 'p1', DATE '2026-11-01', 0.1, false, 'fuzzy')")
    return path


def test_export_writes_one_csv_per_table_with_mapped_headers(warehouse, tmp_path):
    manifest = sql_extract.export(warehouse, SNAPSHOT, tmp_path / "out")
    folder = tmp_path / "out" / SNAPSHOT
    assert set(manifest["tables"]) == set(sqlserver.DDL)
    assert json.loads((folder / sql_extract.MANIFEST).read_text()) == manifest
    for name in sqlserver.DDL:
        with (folder / f"{name}.csv").open(newline="") as handle:
            header = next(csv.reader(handle))
        assert header, name
        assert all(re.search(rf"\b{column}\b", sqlserver.DDL[name]) for column in header), name
        assert manifest["tables"][name]["rows"] == (2 if name == "fct_duplicate_pair" else 1)


def test_booleans_become_bit_values_and_nulls_become_empty_fields(warehouse, tmp_path):
    sql_extract.export(warehouse, SNAPSHOT, tmp_path)
    folder = tmp_path / SNAPSHOT
    with (folder / "fct_duplicate_pair.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["is_match"] for row in rows] == ["1", "0"]
    with (folder / "dim_company.csv").open(newline="") as handle:
        company = next(csv.DictReader(handle))
    assert company["company_code"] == ""
    assert company["company_name"] == "Acme, Inc."


def test_missing_mirror_table_stops_the_export(tmp_path):
    path = tmp_path / "partial.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA analytics")
        con.execute("CREATE TABLE analytics.dim_company (company_key VARCHAR)")
    with pytest.raises(ValueError, match="analytics.dim_ingredient"):
        sql_extract.export(path, SNAPSHOT, tmp_path / "out")


def test_upload_refuses_an_incomplete_extract(tmp_path):
    pytest.importorskip("azure.storage.blob")
    (tmp_path / SNAPSHOT).mkdir()
    with pytest.raises(ValueError, match="run export first"):
        sql_extract.upload("UseDevelopmentStorage=true", "sql-extracts", tmp_path, SNAPSHOT)


@pytest.mark.skipif(not os.environ.get("AZURITE_CONNECTION_STRING"), reason="Azurite is required for the upload")
def test_upload_round_trip_to_azurite(warehouse, tmp_path):
    from azure.storage.blob import BlobServiceClient

    connection = os.environ["AZURITE_CONNECTION_STRING"]
    sql_extract.export(warehouse, SNAPSHOT, tmp_path)
    container = "adf-test-" + uuid4().hex
    try:
        assert sql_extract.upload(connection, container, tmp_path, SNAPSHOT) == len(sqlserver.DDL) + 1
        with BlobServiceClient.from_connection_string(connection) as service:
            client = service.get_container_client(container)
            names = {blob.name for blob in client.list_blobs(name_starts_with=f"{SNAPSHOT}/")}
            assert names == {f"{SNAPSHOT}/{name}.csv" for name in sqlserver.DDL} | {f"{SNAPSHOT}/manifest.json"}
            stored = client.get_blob_client(f"{SNAPSHOT}/manifest.json").download_blob().readall()
            assert json.loads(stored)["tables"]["dim_product"]["rows"] == 1
    finally:
        with BlobServiceClient.from_connection_string(connection) as service:
            service.get_container_client(container).delete_container()
