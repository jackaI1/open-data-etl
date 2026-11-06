"""The SQL runs through dbt-core; the connector runs against a labelled emulator."""
import os
import shutil
import subprocess
from datetime import date
from pathlib import Path

import duckdb
import pytest

from dpd_etl.metrics import generate, report_sql
from dpd_etl.warehouse import identifier, publish, read_snapshot

DAY = '2026-11-01'
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def rows():
    day = date.fromisoformat(DAY)
    data = {
        'products': [(1, '00000001', 'EXAMPLE A', 'HUMAN', 'C1', 'MAKER', 'N', 'MARKETED', day),
                     (2, '00000002', 'EXAMPLE B', 'HUMAN', None, None, 'N', 'MARKETED', day)],
        'ingredients': [(1, 'SAMPLE', '10', 'MG', day), (1, 'SAMPLE', '20', 'MG', day),
                        (2, 'SAMPLE', '30', 'MG', day)],
        'pairs': [(1, 2, 0.99, True, 'synthetic_match', day)],
        'strengths': [(1, 'SAMPLE', '10', 'MG', False, day), (1, 'SAMPLE', '20', 'MG', True, day),
                      (2, 'SAMPLE', '30', 'MG', False, day)],
    }
    data['manifest'] = [(name, len(value), day) for name, value in data.items()]
    return data


def test_snowflake_connector_roundtrip_replaces_date_and_preserves_history(rows):
    fakesnow = pytest.importorskip('fakesnow')
    import snowflake.connector
    with fakesnow.patch():
        con = snowflake.connector.connect(database='DPD', schema='RAW', autocommit=False)
        try:
            assert publish(con, rows, DAY)['products'] == 2
            publish(con, rows, DAY)
            cur = con.cursor()
            assert cur.execute('SELECT count(*) FROM raw.products').fetchone()[0] == 2
            # Clear a formerly non-empty flag list on the same date.
            rows['pairs'] = []
            rows['manifest'][2] = ('pairs', 0, date.fromisoformat(DAY))
            publish(con, rows, DAY)
            assert cur.execute('SELECT count(*) FROM raw.pairs').fetchone()[0] == 0
            assert cur.execute('SELECT din FROM raw.products WHERE drug_code=1').fetchone()[0] == '00000001'
            cur.close()
        finally:
            con.close()


def test_transaction_rolls_back_all_sources_on_bad_row(rows):
    con = duckdb.connect()
    try:
        publish(con, rows, DAY, snowflake=False)
        rows['ingredients'] = [('not-an-integer', 'BROKEN', '1', 'MG', date.fromisoformat(DAY))]
        with pytest.raises(duckdb.ConversionException):
            publish(con, rows, DAY, snowflake=False)
        assert con.execute('select count(*) from raw.products').fetchone()[0] == 2
        assert con.execute('select count(*) from raw.ingredients').fetchone()[0] == 3
    finally:
        con.close()


def run_dbt(db, *commands):
    env = dict(os.environ, DPD_WAREHOUSE_PATH=str(db), DBT_SEND_ANONYMOUS_USAGE_STATS='false')
    return subprocess.run(
        [str(Path(shutil.which('dbt')).resolve()), *commands,
         '--project-dir', str(ROOT / 'warehouse'), '--profiles-dir', str(ROOT / 'warehouse')],
        check=False, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120,
    )


@pytest.mark.skipif(not shutil.which('dbt'), reason='install .[warehouse] to execute dbt')
def test_dbt_star_schema_and_metrics_fail_on_invalid_din(rows, tmp_path):
    db = tmp_path / 'warehouse.duckdb'
    with duckdb.connect(str(db)) as con:
        publish(con, rows, DAY, snowflake=False)
    result = run_dbt(db, 'build')
    assert result.returncode == 0, result.stdout[-7000:]
    with duckdb.connect(str(db)) as con:
        assert con.execute('select products, ingredient_records, flagged_strengths from analytics.rpt_catalogue').fetchone() == (2, 3, 1)
        assert con.execute('select count(*) from analytics.dim_company').fetchone()[0] == 2
        assert con.execute('select count(*) from analytics.fct_duplicate_pair').fetchone()[0] == 1
        con.execute("update raw.products set din='BAD' where drug_code=1")
    result = run_dbt(db, 'build')
    assert result.returncode != 0
    assert 'accepted_values_stg_products_din_status' in result.stdout


def test_generated_definitions_are_current(tmp_path):
    for path in ['warehouse/models/marts', 'docs']:
        (tmp_path / path).mkdir(parents=True)
    shutil.copy(ROOT / 'warehouse/models/schema.yml', tmp_path / 'warehouse/models/schema.yml')
    generate(tmp_path)
    for path in ['warehouse/models/marts/rpt_catalogue.sql', 'docs/data-dictionary.md']:
        assert (ROOT / path).read_text() == (tmp_path / path).read_text()
    assert 'count(*) as products' in report_sql('gold_product')


@pytest.mark.skipif(not os.environ.get('DPD_SPARK_PARQUET'), reason='CI creates the Spark fixture first')
def test_python_reconciles_actual_spark_output_and_dbt(tmp_path):
    root = Path(os.environ['DPD_SPARK_PARQUET'])
    data = read_snapshot(root, DAY)
    assert len(data['products']) == 60
    db = tmp_path / 'spark_warehouse.duckdb'
    with duckdb.connect(str(db)) as con:
        totals = publish(con, data, DAY, snowflake=False)
    result = run_dbt(db, 'build')
    assert result.returncode == 0, result.stdout[-7000:]
    with duckdb.connect(str(db)) as con:
        assert con.execute('select count(*) from analytics.dim_product').fetchone()[0] == totals['products']
        assert con.execute('select count(*) from analytics.fct_product_ingredient').fetchone()[0] == totals['ingredients']
        assert con.execute('select count(*) from analytics.fct_duplicate_pair').fetchone()[0] == totals['pairs']


def test_identifiers_and_dates_cannot_be_sql(rows):
    with pytest.raises(ValueError):
        identifier('RAW; DROP DATABASE DPD')
    with duckdb.connect() as con, pytest.raises(ValueError):
        publish(con, rows, "2026-11-01' OR TRUE", snowflake=False)


def test_source_output_is_required(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_snapshot(tmp_path, DAY)


def test_dbt_snowflake_adapter_executes_models_and_tests_in_emulator(rows, monkeypatch):
    fakesnow = pytest.importorskip('fakesnow')
    pytest.importorskip('dbt.adapters.snowflake')
    import snowflake.connector
    from dbt.cli.main import dbtRunner
    if os.environ.get('DPD_SPARK_PARQUET'):
        rows = read_snapshot(Path(os.environ['DPD_SPARK_PARQUET']), DAY)
    for key, value in {'ACCOUNT': 'local', 'USER': 'test', 'PASSWORD': 'test',
                       'ROLE': 'tester', 'DATABASE': 'DPD', 'WAREHOUSE': 'TEST_WH'}.items():
        monkeypatch.setenv('SNOWFLAKE_' + key, value)
    monkeypatch.setenv('DBT_SEND_ANONYMOUS_USAGE_STATS', 'false')
    with fakesnow.patch():
        con = snowflake.connector.connect(database='DPD', schema='RAW', autocommit=False)
        try:
            publish(con, rows, DAY)
            result = dbtRunner().invoke([
                'build', '--target', 'snowflake', '--project-dir', str(ROOT / 'warehouse'),
                '--profiles-dir', str(ROOT / 'warehouse'), '--target-path', 'target-snowflake',
            ])
            assert result.success, str(result.exception)
            cur = con.cursor()
            assert cur.execute('SELECT sum(products) FROM analytics.rpt_catalogue').fetchone()[0] == len(rows['products'])
            assert cur.execute('SELECT sum(ingredient_records) FROM analytics.rpt_catalogue').fetchone()[0] == len(rows['ingredients'])
            assert cur.execute('SELECT count(*) FROM analytics.fct_duplicate_pair').fetchone()[0] == len(rows['pairs'])
            cur.close()
        finally:
            con.close()
