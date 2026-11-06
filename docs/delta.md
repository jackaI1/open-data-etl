# Dated catalogue tables and a notebook job

I keep the cleaned catalogue and its strength review records in Delta Lake so I
can rerun a date, preserve other dates and inspect the version before a change.
Delta adds a transaction log to table files. A write either commits a new table
version or leaves its previous version readable.

## Try the notebook on my computer

From the repository root, with Python 3.10 to 3.12, Java 17 and uv installed:

```sh
make install
# On macOS with Homebrew's Java 17:
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
make delta-install
make delta-test
make delta-fixture SNAPSHOT_DATE=2026-11-01
```

The first Delta run downloads Java libraries from Maven, a public package store.
It needs internet access, but subsequent runs can reuse the local library cache.
The sample writes 60 products under `data/fixture/delta/drug_products/` and the
ingredient review under `data/fixture/delta/strength_anomalies/`. It writes checks
and row counts under `data/fixture/reports/`, and prints at most 20 flagged rows.
The unchanged sample may have no flags; the tests deliberately add one known
outlier to exercise a nonempty review list.

To process the full download with the normal quality thresholds:

```sh
make extract
make delta SNAPSHOT_DATE=2026-11-06
# Equivalent notebook command with every parameter shown:
.venv/bin/python notebooks/health_canada_delta.py \
  --raw-dir data/raw/files --delta-root data/delta \
  --snapshot-date 2026-11-06 --report-dir data/reports
```

The date labels the copy I downloaded. Giving today's download an earlier date
does not reconstruct that day's history. For a historical rerun I supply that
date's saved raw files. `--fixture` relaxes size checks for the small sample;
I leave it off for real downloads.

## Read the review list without reprocessing files

I can reopen the saved tables in another Python process:

```python
from dpd_etl.delta_job import get_delta_spark, register_views

spark = get_delta_spark()
try:
    register_views(spark, "data/delta")
    spark.sql("""
        SELECT drug_code, brand_name, ingredient, strength, strength_unit, robust_z
        FROM flagged_strengths
        WHERE snapshot_date = DATE '2026-11-06'
        ORDER BY abs(robust_z) DESC, drug_code
    """).show(20, truncate=False)
finally:
    spark.stop()
```

`flagged_strengths` is a temporary SQL view for the current session. The
[saved SQL](../src/dpd_etl/sql/flagged_strengths.sql) selects only flagged rows and
joins products on both product key and date, so several snapshots cannot multiply
the results. `register_views` recreates it from the persistent Delta tables.
The strength table keeps unflagged and unscored ingredients too, with the reason,
peer count, median, median absolute deviation and score. I can still investigate
why an ingredient received no score. The [review guide](detection.md) explains
the detection rules and their limitations.

The [earlier catalogue run](detection.md#downloaded-catalogue-check) recorded
13,379 products, 19,403 ingredient strengths and 637 flags. I reproduced those same three counts through the Delta notebook on 2026-11-06,
using archive SHA-256 `e89dc3e303c5372b101059b273cb96230d957a37ca760440b85db6a2e3575737`.
All quality checks passed. I use the same detector for this view, rather than
storing 637 in the SQL. Current downloads can differ.

## Run the source notebook in Databricks

`notebooks/health_canada_delta.py` uses Databricks' source notebook format:
`# Databricks notebook source` and `# COMMAND ----------` separate the cells.
Locally it accepts command-line arguments. In Databricks it reads the same values
from text widgets, the named notebook inputs supplied by a job.

1. Build the package with `uv build --wheel`. Install the resulting wheel from
   `dist/` as a library on the job's compute. Databricks already supplies Spark and
   Delta; I do not install the local `delta` extra on that compute.
2. Import the Python source notebook into the workspace. Put the raw files in a
   Unity Catalog Volume, a file folder readable by the compute's identity.
3. Create a notebook task on compatible compute and set its parameters:

   | Parameter | Example |
   | --- | --- |
   | `raw_dir` | `/Volumes/my_catalog/my_schema/dpd/raw/2026-11-06` |
   | `delta_root` | `/Volumes/my_catalog/my_schema/dpd/delta` |
   | `snapshot_date` | `2026-11-06` |
   | `report_dir` | `/Volumes/my_catalog/my_schema/dpd/reports` |
   | `fixture` | `false` |

4. Run the task and read its row-count summary and review table. The notebook uses
   the supplied Spark session and leaves that managed session running.

The volume paths are examples, not a deployed account. The tested implementation
uses local Spark 3.5 and `delta-spark==3.3.2`; the [Delta compatibility table](https://docs.delta.io/releases/)
maps the 3.3 release series to Spark 3.5. I followed the official guides for
[notebook source import](https://docs.databricks.com/aws/en/notebooks/notebook-export-import)
and [widget parameters](https://docs.databricks.com/aws/en/notebooks/widgets).
This project demonstrates the notebook pattern locally. It does not establish
hosted scheduling, workspace permissions or performance in Databricks.

## Writes, tests and recovery

The original Parquet, DuckDB, Airflow, monitoring and cloud commands remain
available. Both sinks use `checked_snapshot` to build the cleaned frames and
enforce the same quality rules before writing data. The Delta job reads the
latest earlier product count from its own Delta output for the row-drop check.
Cached Spark frames are released on success and failure.

Each table is partitioned by `snapshot_date`, which stores dates separately.
I use [Delta's `replaceWhere`](https://docs.delta.io/delta-batch/) to replace only
that date. Empty replacements clear the old date as well. Schema changes fail
rather than silently adding columns. Delta's version history can reopen an
earlier committed table with `spark.read.format("delta").option("versionAsOf", 0)`.
Reading the folders as plain Parquet would include obsolete files, so I always
read them with `format("delta")`.

Transactions cover one table at a time. If the products write succeeds and the
strengths write fails, the pair is incomplete until I rerun the same date. I
serialize writers to an output root and accept a run only after it exits
successfully and writes its `delta_YYYY-MM-DD.json` summary. Starting a rerun removes its previous summary; a rejected rerun leaves only
its quality report. These are local
path-based tables, not permanent Unity Catalog table registrations.

CI runs `make delta-test delta-fixture` in a separate process from the original
pipeline tests so the Delta Java libraries are loaded before Spark starts.
Six tests exercise real transaction logs, one injected outlier, repeated dates,
two-date preservation, clearing flags, time travel, reopening SQL in another
session, empty replacements, date mismatches, invalid parameters and rejected
quality checks. No Databricks account is needed. The notebook closes the Spark
session it creates locally; tests close their own sessions in fixture cleanup.
