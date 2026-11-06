"""Bronze/silver/gold Delta tables with saved reconciliation and query plans."""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from .config import FIXTURE_THRESHOLDS, Thresholds
from .delta_job import get_delta_spark, valid_date, write_snapshot
from .detection import detect_strength_anomalies
from .metrics import definitions, report_sql
from .monitoring import Monitor
from .snapshot import checked_snapshot
from .transform import read_raw


def validate_freshness(snapshot_date, as_of=None):
    age = ((as_of or date.today()) - date.fromisoformat(valid_date(snapshot_date))).days
    if age < 0 or age > definitions()['checks']['freshness_days']:
        raise ValueError(f'snapshot freshness failed: {age} days old')


def require_key(frame, keys):
    if frame.where(' OR '.join(f'{k} IS NULL' for k in keys)).limit(1).count():
        raise ValueError(f'null key: {keys}')
    if frame.groupBy(*keys).count().where('count > 1').limit(1).count():
        raise ValueError(f'duplicate key: {keys}')


def run_lakehouse(spark, *, raw_dir, delta_root, snapshot_date, report_dir,
                  fixture=False, as_of=None):
    """A completion receipt is published only after totals and references reconcile.

    Delta commits are atomic per table. Readers must require this date's receipt;
    writers for a date must be serialized, and interrupted runs must be repeated.
    """
    valid_date(snapshot_date)
    reports = Path(report_dir)
    reports.mkdir(parents=True, exist_ok=True)
    receipt = reports / f'lakehouse_{snapshot_date}.json'
    receipt.unlink(missing_ok=True)
    validate_freshness(snapshot_date, as_of)
    day = F.lit(snapshot_date).cast('date')
    with checked_snapshot(spark, Path(raw_dir), snapshot_date,
                          FIXTURE_THRESHOLDS if fixture else Thresholds(), None,
                          reports / f'lakehouse_dq_{snapshot_date}.json', Monitor.from_env()
                          ) as (products, _, ingredients):
        raw = read_raw(spark, Path(raw_dir), 'drug').withColumn('snapshot_date', day)
        forms = read_raw(spark, Path(raw_dir), 'form').select(
            F.col('drug_code').cast('int').alias('drug_code'),
            F.upper(F.trim('pharm_form')).alias('dosage_form'),
        ).groupBy('drug_code').agg(F.min('dosage_form').alias('dosage_form'))
        silver = products.join(forms, 'drug_code', 'left')
        strengths = detect_strength_anomalies(ingredients).withColumn('snapshot_date', day)
        for name, frame in [('bronze_products', raw), ('silver_products', silver),
                            ('silver_strengths', strengths)]:
            if name == 'silver_products':
                require_key(frame, ['snapshot_date', 'drug_code'])
            write_snapshot(frame, f'{delta_root}/{name}', snapshot_date)
        for name in ['bronze_products', 'silver_products', 'silver_strengths']:
            spark.read.format('delta').load(f'{delta_root}/{name}').createOrReplaceTempView(name)
        gold = spark.sql('''
          SELECT p.snapshot_date, p.drug_code, p.drug_class, p.dosage_form,
                 coalesce(s.ingredient_records, 0) as ingredient_records,
                 coalesce(s.flagged_strengths, 0) as flagged_strengths
          FROM silver_products p LEFT JOIN (
            SELECT snapshot_date, drug_code, count(*) as ingredient_records,
                   sum(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as flagged_strengths
            FROM silver_strengths GROUP BY snapshot_date, drug_code
          ) s ON p.snapshot_date=s.snapshot_date AND p.drug_code=s.drug_code
        ''').where(F.col('snapshot_date') == day)
        require_key(gold, ['snapshot_date', 'drug_code'])
        if strengths.join(products, ['snapshot_date', 'drug_code'], 'left_anti').count():
            raise ValueError('ingredient foreign key does not exist')
        write_snapshot(gold, f'{delta_root}/gold_product', snapshot_date)
        spark.read.format('delta').load(f'{delta_root}/gold_product').createOrReplaceTempView('gold_product')
        spark.sql(report_sql('gold_product')).createOrReplaceTempView('rpt_catalogue')
        # Cohorts group by dosage form; funnel stages use their own grain.
        spark.sql('''SELECT snapshot_date, coalesce(dosage_form, 'UNKNOWN') dosage_form,
                     count(*) products, sum(CASE WHEN flagged_strengths > 0 THEN 1 ELSE 0 END) flagged_products
                     FROM gold_product GROUP BY snapshot_date, dosage_form''').createOrReplaceTempView('dosage_cohorts')
        counts = {'raw_rows': raw.count(), 'cleaned_products': products.count(),
                  'raw_distinct_keys': raw.select('drug_code').distinct().count(),
                  'gold_products': gold.count(), 'ingredient_records': strengths.count(),
                  'flagged_strengths': strengths.where('is_anomaly').count()}
        totals = gold.agg(F.sum('ingredient_records'), F.sum('flagged_strengths')).first()
        if (counts['raw_distinct_keys'] != counts['cleaned_products']
                or counts['gold_products'] != counts['cleaned_products']
                or totals[0] != counts['ingredient_records']
                or totals[1] != counts['flagged_strengths']):
            raise ValueError(f'stage reconciliation failed: {counts}')
        spark.sql('''SELECT snapshot_date, 'raw_rows' stage, count(*) rows FROM bronze_products GROUP BY snapshot_date
                     UNION ALL SELECT snapshot_date, 'cleaned_products', count(*) FROM silver_products GROUP BY snapshot_date
                     UNION ALL SELECT snapshot_date, 'flagged_products',
                     sum(CASE WHEN flagged_strengths > 0 THEN 1 ELSE 0 END)
                     FROM gold_product GROUP BY snapshot_date''').createOrReplaceTempView('stage_funnel')
        # Keep both plans. Filtering at the scan prunes Delta's date partitions.
        before = '\n'.join(r[0] for r in spark.sql('EXPLAIN FORMATTED SELECT * FROM gold_product').collect())
        after = '\n'.join(r[0] for r in spark.sql(
            'EXPLAIN FORMATTED SELECT * FROM gold_product WHERE snapshot_date = :day',
            args={'day': snapshot_date}).collect())
        (reports / f'plans_{snapshot_date}.txt').write_text('Unfiltered scan\n' + before + '\nDate-filtered scan\n' + after)
        summary = {'snapshot_date': snapshot_date, **counts,
                   'raw_duplicate_rows': counts['raw_rows'] - counts['raw_distinct_keys']}
        receipt.write_text(json.dumps(summary, indent=2) + '\n')
        return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--raw-dir', required=True)
    p.add_argument('--delta-root', required=True)
    p.add_argument('--snapshot-date', required=True)
    p.add_argument('--report-dir', required=True)
    p.add_argument('--fixture', action='store_true')
    p.add_argument('--as-of', type=date.fromisoformat, help='Explicit historical replay reference date')
    args = p.parse_args()
    spark = get_delta_spark()
    try:
        print(json.dumps(run_lakehouse(spark, **vars(args)), indent=2))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
