from datetime import date

import pytest

pytest.importorskip('delta')

from dpd_etl.delta_job import get_delta_spark, write_snapshot
from dpd_etl.lakehouse import require_key, run_lakehouse, validate_freshness

DAY = '2026-11-01'


@pytest.fixture(scope='module')
def lake_spark():
    spark = get_delta_spark()
    spark.sparkContext.setLogLevel('ERROR')
    yield spark
    spark.stop()


def test_bronze_silver_gold_reconciles_rerun_sql_and_partition_plans(lake_spark, raw_dir, tmp_path):
    args = {'raw_dir': str(raw_dir), 'delta_root': str(tmp_path / 'delta'), 'snapshot_date': DAY,
            'report_dir': str(tmp_path / 'reports'), 'fixture': True, 'as_of': date(2026, 11, 2)}
    first = run_lakehouse(lake_spark, **args)
    assert first['raw_rows'] == 61 and first['raw_duplicate_rows'] == 1
    assert first['cleaned_products'] == first['gold_products'] == 60
    assert run_lakehouse(lake_spark, **args) == first
    spark = lake_spark
    assert spark.sql('SELECT sum(products) FROM dosage_cohorts').first()[0] == 60
    stages = {r.stage: r.rows for r in spark.table('stage_funnel').collect()}
    assert stages['raw_rows'] == 61 and stages['cleaned_products'] == 60
    metrics = spark.sql('SELECT sum(products), sum(ingredient_records), sum(flagged_strengths) FROM rpt_catalogue').first()
    assert tuple(metrics) == (60, first['ingredient_records'], first['flagged_strengths'])
    plan = (tmp_path / 'reports' / f'plans_{DAY}.txt').read_text()
    assert 'PartitionFilters' in plan
    assert "snapshot_date" in plan.split('Date-filtered scan')[1]
    # Delta rejects an incompatible type on an existing typed table.
    from pyspark.sql import functions as F
    bad = spark.table('silver_products').withColumn('drug_code', F.lit('BROKEN'))
    from pyspark.errors import AnalysisException
    with pytest.raises(AnalysisException):
        write_snapshot(bad, str(tmp_path / 'delta/silver_products'), DAY)


def test_key_and_freshness_fail_loudly(lake_spark):
    with pytest.raises(ValueError, match='freshness'):
        validate_freshness(DAY, date(2026, 11, 10))
    with pytest.raises(ValueError, match='freshness'):
        validate_freshness('2026-11-03', date(2026, 11, 2))
    duplicate = lake_spark.createDataFrame([(1,), (1,)], 'drug_code int')
    with pytest.raises(ValueError, match='duplicate key'):
        require_key(duplicate, ['drug_code'])
