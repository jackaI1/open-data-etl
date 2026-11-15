import os
from pathlib import Path

import duckdb
import pytest

from dpd_etl.sqlserver import connect, explain, publish

pytestmark = pytest.mark.skipif(not os.environ.get('DPD_SQLSERVER_PASSWORD'), reason='SQL Server CI container required')

SUMMARY = ('SELECT snapshot_date, company_key, products, products_with_flags, flagged_strengths, '
           'ingredient_records FROM dbo.flagged_strength_summary ORDER BY 1, 2')
RECOMPUTED = ('SELECT f.snapshot_date, f.company_key, count_big(*), '
              'sum(CASE WHEN f.flagged_strengths > 0 THEN 1 ELSE 0 END), sum(f.flagged_strengths), '
              'sum(f.ingredient_records) FROM dbo.fct_product f GROUP BY f.snapshot_date, f.company_key '
              'ORDER BY 1, 2')


def rows(con, query):
    cur = con.cursor()
    try:
        cur.execute(query)
        return [tuple(row) for row in cur.fetchall()]
    finally:
        cur.close()


def test_real_tsql_constraints_reporting_rerun_and_query_plans(tmp_path):
    db = Path(os.environ['DPD_WAREHOUSE_PATH'])
    con = connect()
    try:
        first = publish(con, db)
        assert first['dim_product'] == 60
        assert publish(con, db) == first
        cur = con.cursor()
        cur.execute('SELECT sum(products), sum(ingredient_records), sum(flagged_strengths) FROM dbo.rpt_catalogue')
        actual = cur.fetchone()
        with duckdb.connect(str(db)) as local:
            expected = local.execute('SELECT sum(products), sum(ingredient_records), sum(flagged_strengths) FROM analytics.rpt_catalogue').fetchone()
        assert actual == expected
        cur.execute('SELECT * FROM dbo.rpt_reconciliation WHERE expected_ingredients != actual_ingredients')
        assert cur.fetchall() == []
        import pymssql
        with pytest.raises(pymssql.IntegrityError):
            cur.execute("INSERT INTO dbo.fct_product VALUES ('missing', 'missing', '2026-11-01', 'HUMAN', 0, 0)")
        con.rollback()
        cur.close()
        explain(con, tmp_path)
        for label in ['before', 'after']:
            assert '<ShowPlanXML' in (tmp_path / f'{label}.sqlplan').read_text()
        cur = con.cursor()
        cur.execute("SELECT name FROM sys.indexes WHERE name='ix_product_date_class'")
        assert cur.fetchone()[0] == 'ix_product_date_class'
        cur.close()
    finally:
        con.close()


def test_flagged_strength_procedure_matches_fact_rows_and_is_repeatable():
    db = Path(os.environ['DPD_WAREHOUSE_PATH'])
    con = connect()
    try:
        counts = publish(con, db)
        summary = rows(con, SUMMARY)
        assert counts['flagged_strength_summary'] == len(summary) > 0
        assert summary == rows(con, RECOMPUTED)
        cur = con.cursor()
        cur.execute('EXEC dbo.usp_refresh_flagged_strength_summary')
        con.commit()
        cur.close()
        assert rows(con, SUMMARY) == summary
    finally:
        con.close()
