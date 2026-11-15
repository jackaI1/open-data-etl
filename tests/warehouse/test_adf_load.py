"""Load the CSV extract the way the Data Factory pipeline does, then compare it with the direct load."""

import csv
import json
import os
from pathlib import Path

import duckdb
import pytest

from dpd_etl import sql_extract, sqlserver

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(not os.environ.get('DPD_SQLSERVER_PASSWORD'), reason='SQL Server CI container required')
PIPELINE = json.loads((ROOT / 'adf/pipelines/MonthlyExtractToSql.json').read_text())['properties']
TABLE_ORDER = PIPELINE['parameters']['tables']['defaultValue']


def count(con, table):
    cur = con.cursor()
    try:
        cur.execute(f'SELECT count_big(*) FROM dbo.{table}')
        return cur.fetchone()[0]
    finally:
        cur.close()


def test_csv_extract_loaded_in_pipeline_order_matches_the_direct_publish(tmp_path):
    db = Path(os.environ['DPD_WAREHOUSE_PATH'])
    snapshot = '2026-11-01'
    manifest = sql_extract.export(db, snapshot, tmp_path)
    folder = tmp_path / snapshot
    assert TABLE_ORDER == list(sqlserver.DDL)
    con = sqlserver.connect()
    try:
        direct = sqlserver.publish(con, db)
        cur = con.cursor()
        cur.execute('EXEC dbo.usp_clear_mirror')
        for table in TABLE_ORDER:
            with (folder / f'{table}.csv').open(newline='') as handle:
                reader = csv.DictReader(handle)
                columns = reader.fieldnames
                values = [[None if value == '' else value for value in row.values()] for row in reader]
            if values:
                marks = ', '.join(['%s'] * len(columns))
                cur.executemany(f"INSERT INTO dbo.{table} ({', '.join(columns)}) VALUES ({marks})", values)
        cur.execute('EXEC dbo.usp_refresh_flagged_strength_summary')
        con.commit()
        cur.close()
        for table in TABLE_ORDER:
            assert count(con, table) == manifest['tables'][table]['rows'] == direct[table], table
        cur = con.cursor()
        cur.execute('SELECT count_big(*) FROM dbo.fct_duplicate_pair WHERE is_match = 1')
        matches = cur.fetchone()[0]
        cur.close()
        with duckdb.connect(str(db), read_only=True) as local:
            expected = local.execute('SELECT count(*) FROM analytics.fct_duplicate_pair WHERE is_match').fetchone()[0]
        assert matches == expected
        assert count(con, 'flagged_strength_summary') == direct['flagged_strength_summary']
    finally:
        con.close()
