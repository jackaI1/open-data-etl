# Keep the source, cleaned records and reports together

I use three Delta layers so I can check where a changed count came from. Bronze
keeps the raw product rows, silver keeps cleaned products and ingredient-strength
checks, and gold keeps one product per date with reporting counts. Delta tables
are files with a transaction log; rerunning a date replaces its partition.

```sh
uv pip install --python .venv/bin/python -e '.[dev,delta]'
export SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python"
.venv/bin/python -m dpd_etl.lakehouse --raw-dir tests/fixtures/raw \
  --delta-root data/lakehouse --snapshot-date 2026-11-01 \
  --report-dir data/lakehouse-reports --fixture --as-of 2026-11-02
.venv/bin/python -m pytest tests/delta -q
```

`--as-of` is the explicit reference date for historical replay. Without it, the
command rejects future inputs or snapshots older than seven days. The input
must be the saved archive for that date; changing the date does not reconstruct
an older catalogue. The existing Airflow scheduler pins dated archives and the
`health_canada_lakehouse` DAG orders download, validation and modeling with two
retries and per-task logging. Install `.[airflow,delta]` using the constraints in
[the scheduling guide](airflow.md), then enable that DAG in the scheduler.

The receipt `lakehouse_YYYY-MM-DD.json` records raw rows, distinct source keys,
cleaned products, gold products, ingredient records and strength flags. It is
written only when key, reference and total checks pass. On this sample, 61 raw
rows include one old duplicate, leaving 60 products in both silver and gold.
Tables commit separately, so readers must require the receipt and serialize
writers for a date; a failed rerun removes the old receipt before starting.

After the job runs, these Spark SQL views are available in its session:

```sql
SELECT * FROM rpt_catalogue ORDER BY snapshot_date, drug_class;
SELECT * FROM dosage_cohorts ORDER BY snapshot_date, dosage_form;
SELECT * FROM stage_funnel ORDER BY snapshot_date, stage;
```

The first report summarizes flagged ingredient strengths by drug class. The
cohort report groups products by dosage form; the funnel counts raw rows,
cleaned products and products with flags at their own grains. The checks compare
distinct raw product keys with cleaned and gold products, then compare ingredient
and flagged-strength totals. Reconciliation fails on any difference. Metrics
and lineage come from [one definition file](../src/dpd_etl/metrics.yml).

## Query optimization I can inspect

I partition by snapshot date because review queries nearly always select a date.
I avoid partitioning by product or ingredient, which would create many tiny files.
For the small fixture, this is a layout choice rather than a benchmark win.
The job saves `EXPLAIN FORMATTED` plans for an unfiltered scan and a date-filtered
scan. I compare `PartitionFilters` and the scan's data filters in
`plans_YYYY-MM-DD.txt`. Date predicates prune partitions; other predicates can
be pushed down into Parquet scans. I don't claim a runtime speedup from this
small sample. Tests also compare the actual files read across two date partitions, since Delta
can fold a partition predicate into its file index and omit that label from the
printed plan. The selected date reads fewer files and returns 60 products.

## Run as a Databricks job

`notebooks/health_canada_lakehouse.py` is a source-format notebook with cells and
widgets. It accepts the same four paths/date parameters as the local job.
`databricks.yml` builds a wheel, installs it in the job environment and defines
a serverless notebook task with retries and one concurrent run.

In a Free Edition workspace, create a writable Unity Catalog volume, upload the
saved source files there, authenticate the Databricks CLI, and run:

```sh
databricks bundle validate --var='raw_dir=/Volumes/catalog/schema/volume/raw,delta_root=/Volumes/catalog/schema/volume/delta,report_dir=/Volumes/catalog/schema/volume/reports,snapshot_date=2026-11-06'
databricks bundle deploy --var='raw_dir=/Volumes/catalog/schema/volume/raw,delta_root=/Volumes/catalog/schema/volume/delta,report_dir=/Volumes/catalog/schema/volume/reports,snapshot_date=2026-11-06'
databricks bundle run catalogue
# When finished: databricks bundle destroy
```

I compare the printed receipt with the same pinned-input local run. CI checks
local Spark/Delta execution, schema enforcement, report SQL and idempotent reruns.
I have not run the job in a hosted Databricks workspace or collected hosted row
counts. Serverless/runtime compatibility and workspace permissions still need
that deployment check. The bundle is deployment configuration, not a claim that
an account-free CI run executed inside Databricks.

## How I map this to Microsoft's hosted stack

| This project | Hosted equivalent | What I tested |
| --- | --- | --- |
| Spark and Delta notebooks | Databricks | Real Spark/Delta in Linux CI |
| Bronze/silver/gold files | Azure Data Lake Storage | Local Delta transaction logs, schema and partitions |
| Airflow ordered daily tasks | Azure Data Factory | Airflow runtime and task tests; no Azure deployment |
| SQL Server reporting mirror | Azure SQL | Real SQL Server container and T-SQL integration |
| Superset catalogue dashboard | Power BI reporting | Superset import/query/export; no Power BI release or RLS engine |

I use the free equivalents where I have no hosted account. This catalogue
contains public product records, not borrower defaults or meter readings. It
provides no evidence for LightGBM credit-risk modeling, MLflow model serving,
SHAP explanations or an Azure interval-meter monitor. Those require separate
working projects and cannot truthfully be inferred from catalogue drift checks.
