# open-data-etl

I built a local copy of Canada's drug catalogue because I wanted to look up products and their history without piecing together several files each time.

To try it, install Python 3.10 to 3.12, Java 17 and [uv](https://docs.astral.sh/uv/),
then run `make install` and `make fixture`. The included sample has 60 products
and needs no account or download. I can search the cleaned catalogue and review
unusual ingredient strengths without changing the original records.

I can also keep a dated, checked copy in Delta tables, which record changes to stored data. The bronze, silver and gold layers let me trace source rows through cleaning to a per-run quality summary and compare flagged strengths over time. The [lakehouse guide](docs/lakehouse.md) shows how to run the sample or deploy the job to Databricks. I can also archive each monthly download to Azure Blob Storage and restore any month with its checksums verified.

## Overview

Health Canada's catalogue separates products, ingredients, companies and status
changes into different files. I wanted to explore them together and keep the
version I had downloaded, rather than repeat the joins whenever I had a question.

I download the marketed-product catalogue, clean the records and connect them
into tables I can search on my computer. I also get a review list of possible
duplicate products and unusual ingredient strengths, so I can check suspicious
entries before comparing counts. The original records stay available.
Each run keeps a dated snapshot, a copy of the data for that date. Checks stop a run when required data is missing
or product counts drop sharply. I can start with the included sample below
before downloading the current catalogue.

To try it, I install Python 3.10 to 3.12, Java 17 and
[uv](https://docs.astral.sh/uv/), then run `make install` and `make fixture`.
That processes the included 60-product sample. I can query the result in
`data/fixture/warehouse/dpd.duckdb`; the commands below show how to download the
current catalogue, inspect flags and export a snapshot.

I can also watch run times, product counts and rejected snapshots without
opening each report. The optional monitoring sends summary numbers to Datadog,
a service for watching software runs. `make monitor-demo` tries a good snapshot
and a deliberately broken one on this computer, with no account needed.

I can schedule the same work each day and retry just the step that failed.
Airflow, a tool for scheduling ordered tasks, keeps a separate copy for each
date. I can rerun a date or fill in earlier dates from saved downloads without
adding duplicate rows. The [scheduling guide](docs/airflow.md) starts with a local
sample and shows how to turn on the daily schedule.

This is a personal project for exploring Canada's drug catalogue and keeping a
queryable copy of its history. It uses public product and company data.

For longer runs, I can put the source files in Google Cloud Storage (GCS), a cloud
file store, and run the same pipeline on Cloud Batch, which starts a temporary
computer for the job. Cloud Run hosts a small page where I can read that run's
checks. Each attempt keeps its own outputs, including rejected-run reports.
The [cloud guide](docs/gcp.md) walks through deployment and cleanup; the local
cloud tests below work without an account.

I can also send a checked snapshot to BigQuery, Google's service for querying
stored tables. It gives me a dated review summary of products, ingredient counts
and strengths that need a closer look. Repeating a date replaces its cloud copy
without adding duplicates. The [BigQuery guide](docs/bigquery.md) includes a local
test that needs no account and commands for loading my own cloud project.

I can search my saved catalogue and review its flags in a browser. Elasticsearch,
a search database, holds the accepted snapshots; Kibana turns them into charts
and review tables. I can compare flagged ingredient strengths and duplicate-pair
counts by date, then find the product records behind them. After `make install`
and `make fixture SNAPSHOT_DATE=2026-11-01`, I run:

```sh
make elastic-up
make elastic-load DATA_DIR=data/fixture SNAPSHOT_DATE=2026-11-01
make elastic-import
# Open http://127.0.0.1:15601/app/dashboards#/view/dpd-quality
# When finished: make elastic-down
```

This option needs Docker with Compose and about 4 GiB for the local services.
The [search and dashboard guide](docs/elastic.md) explains setup, date filters,
saved searches and cleanup. Repeating a date clears outdated flags without
removing other dates. The small sample can have no flags; a zero count is still
a result. These flags help me review data, not make medical decisions.

## Compare runs in a dashboard

I can compare product counts, flagged records, possible duplicate pairs and run
seconds in Apache Superset, a free dashboard tool. I added it because separate dated reports made changes harder to
compare. After `make install`, on a machine with Docker and Compose I run:

```sh
.venv/bin/python scripts/seed_dashboard.py
export SUPERSET_SECRET_KEY="$(.venv/bin/python -c 'import secrets; print(secrets.token_hex(32))')"
export SUPERSET_ADMIN_PASSWORD="$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(24))')"
docker compose -p dpd-superset -f superset/compose.yml up -d --wait
# Open http://127.0.0.1:18088/superset/dashboard/catalogue-quality/
# Sign in as admin with SUPERSET_ADMIN_PASSWORD. When finished:
docker compose -p dpd-superset -f superset/compose.yml down --volumes
```

The example history is invented and labelled. For my accepted pipeline output,
I run `.venv/bin/python -m dpd_etl.dashboard --data-dir data/fixture
--snapshot-date 2026-11-01` after a dated fixture run. The dashboard has eleven
charts with date, drug-class and pair-review filters. Its definitions are saved
in the repo, including SQL metrics that calculate the share of products with
flagged strengths. The [dashboard guide](docs/superset.md) explains real data,
review decisions, chart meanings and cleanup.

I can also save a small daily summary with Azure Functions, a program started
by a timer, and Azurite, a local imitation of Azure file storage. It reads the
same accepted counts and measured run duration, then writes one dated JSON file.
The [summary guide](docs/azure-summary.md) includes the start, read and stop
commands. CI tests both the function and a real timer-driven Functions host;
I have not deployed this app to Azure.

## Compare counts without counting a product twice

I can build a reporting copy that separates products, companies and ingredients.
A product with three ingredients still counts as one product. The report checks
its totals against the files my pipeline wrote and stops if they disagree.
I start with the same 60-product sample, then follow the
[warehouse guide](docs/warehouse.md) to run the reporting models without an
account or configure a Snowflake trial. dbt, a tool that builds and checks SQL
tables, tests the keys, links and eight-digit product numbers.

I also keep the source rows, cleaned records and reporting counts as three
Delta layers, so I can see why counts changed between steps. The
[lakehouse guide](docs/lakehouse.md) includes dosage-form groups, stage counts,
freshness checks, a scheduled run and a Databricks job configuration. A separate
SQL Server copy has typed tables, reporting views and count checks. CI runs the
small example on Linux; I have not run a hosted Snowflake or Databricks account.
The [generated dictionary](docs/data-dictionary.md) explains the tables and
metrics in one place.

## Run locally

Install Python 3.10 to 3.12, Java 17, curl, Make and
[uv](https://docs.astral.sh/uv/). See [local setup](docs/pipeline.md#local-setup)
for the Java configuration on macOS. Run these commands from the repository root:

```sh
make install
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
make fixture   # Try the included 60-product sample without a download.
make evaluate  # Check duplicate and unusual-strength detection against known examples.
make monitor-demo  # Check monitoring with a good and a rejected sample snapshot.
make pipeline  # Download and process the current marketed-product catalogue.
make insights  # Print a few summaries of the downloaded data.
```

The full run writes a queryable database to `data/warehouse/dpd.duckdb`, dated
tables under `data/parquet/` and a check report under `data/reports/`.
The sample writes its results to `data/fixture/`. The evaluation writes precision
and recall, how many flags were right and how many known cases were found, to
`data/reports/detection_evaluation.json`. The [review guide](docs/detection.md)
shows how to inspect the duplicate links and strength flags with example queries.
Rerunning a date replaces that snapshot while preserving other dates. A weekly workflow can also download new
snapshots; the [pipeline reference](docs/pipeline.md) explains its storage limits.

## How it works

I use PySpark, Python's interface to the Spark table-processing engine, to clean
and join the source records. This is an ETL pipeline: extract the files,
transform their contents and load the results. Status changes become dated
intervals, so I can query which status applied during a period.

Parquet files store the dated tables in a compact format. DuckDB, a database
that runs locally, lets me query them with SQL, a language for asking questions
of tables. Quality checks run before either output is written. A failed database
load rolls back its changes, but the table files and database are separate
stores, so recovery can require rerunning that date.

## Keep tables I can rerun and review

I also keep the cleaned products and ingredient-strength review records as Delta
tables, files with a change log that lets me replace one date and read earlier
versions. A notebook, a script divided into runnable cells, takes the input
folder, output folder and date as parameters. I can use the same notebook for
the sample or a saved download:

```sh
make delta-install
make delta-test
make delta-fixture SNAPSHOT_DATE=2026-11-01
# After make extract, process the full downloaded catalogue:
make delta SNAPSHOT_DATE=2026-11-06
```

The notebook prints the product and flag counts plus up to 20 flagged strengths.
Its SQL view, a saved table query, adds product names to the flagged ingredients.
My full Delta run on 2026-11-06 saved 13,379 products and returned 637 flagged
strengths from 19,403 ingredient records; that count can change with the download.
A flag is a suggestion to inspect a record, not a medical finding.
The [notebook guide](docs/delta.md) shows how to reopen the tables, query a date
and pass the parameters in Databricks, a hosted service for running Spark jobs.
I have tested this workflow with local Spark and Delta Lake; I have not run it
in a hosted Databricks workspace.

## Add each month without reprocessing everything

Each new monthly download is merged into one `current_products` table, keyed by
drug code. A product that changed is updated, a new one is added, and one that
disappeared is marked inactive rather than deleted. Rerunning the same month
changes nothing. Delta keeps every committed version, so I can compare two months
and see how many products were added, changed or deactivated, without parsing the
source files again. A download with a new or retyped column is refused before the current table
changes, and the error names the column. The raw files are also kept byte for byte,
each with its checksum, in `bronze_raw_files`. The [lakehouse guide](docs/lakehouse.md)
has the commands.

## Keep monthly releases in Azure

I built an archive for the monthly downloads because I wanted them off my laptop
and reproducible. `make archive-upload` sends one month's raw files and Delta tables
to a private Azure Blob Storage container, a cloud file store. A manifest records
every file's SHA-256 checksum. `make archive-list` shows the stored months, and
`make archive-restore` downloads one back and refuses any file that fails its checksum.

```sh
export DPD_ARCHIVE_CONNECTION_STRING='...'  # the storage account's connection string
make archive-upload SNAPSHOT_DATE=2026-11-06
make archive-list
make archive-restore SNAPSHOT_DATE=2026-11-06 RESTORE_DIR=data/restored
```

The storage account, its container and the access rules are a Bicep template in
[`infra/azure/main.bicep`](infra/azure/main.bicep). Bicep is Azure's file format for
describing resources, so the same setup can be rebuilt later. The tests run against
Azurite, Microsoft's free local storage emulator, so no account or cost is needed.
The [archive guide](docs/archive.md) has the setup steps, the tests and what I have not
checked in a real subscription.

## Query a saved snapshot in BigQuery

After `make install` and the sample run above, I can test the export and review
queries on my computer:

```sh
scripts/test_bigquery.sh tests/test_bigquery_emulator.py
```

With a Google Cloud project and `gcloud auth application-default login`, I can
load that same sample and preview how much data a review query would read:

```sh
.venv/bin/python -m dpd_etl.bigquery \
  --project my-project-id --dataset dpd_review --snapshot-date 2026-11-01 \
  --parquet-root data/fixture/parquet
.venv/bin/python -m dpd_etl.bigquery_review \
  --project my-project-id --dataset dpd_review --snapshot-date 2026-11-01 --dry-run
```

I use `make fixture SNAPSHOT_DATE=2026-11-01` for this dated example. Removing
`--dry-run` prints up to 100 products needing review, with a 1 GiB query billing
limit by default. The [guide](docs/bigquery.md) explains permissions, locations,
cleanup and the emulator's limits.

I also load duplicate-pair candidates and run parameterized SQL, queries with
separate input values, to check counts, missing-value rates, strength flags and
suggested duplicate pairs against the saved Spark results:

```sh
.venv/bin/python -m dpd_etl.bigquery_health --project my-project-id \
  --dataset dpd_review --data-dir data/fixture --snapshot-date 2026-11-01
```

This command loads all four tables and fails if the SQL totals differ from the
accepted local output. It recomputes the strength decision from the saved peer
statistics and counts duplicate decisions from their reasons. The 637 flags
mentioned above belong to my dated full download; the small fixture has its
own expected count. CI tests the query through the BigQuery emulator, including
a known flag, null values, cleared results and a date with no data.

I divide each table into dates, called partitions, and group related values
inside each date, called clustering. Every load replaces one date. The review
SQL counts ingredients and flags before joining them, so several ingredients do
not inflate the flag count. A failed rerun can temporarily hide that date's
summary; rerunning repairs it. I serialize uploads for the same date.

## Schedule daily snapshots

After `make install`, run `make airflow-install` and `make airflow-test` to try
the scheduler tests without a server or account. The [Airflow guide](docs/airflow.md)
shows the commands to run the sample, start a local scheduler and backfill,
meaning process earlier dates. I keep each date's original download so retries
use the same input. An older date without a saved download fails, since the
current catalogue cannot reconstruct history.

Each date has its own database under `data/airflow/snapshots/`. I accept the
results only after a final check writes `complete.json`. This keeps the existing
local and cloud commands available alongside the daily schedule.

## Run in Google Cloud

I use this option when I want a run to finish without keeping my laptop open.
It needs a cloud project with billing enabled, Docker, Terraform and Google's
`gcloud` command. After `make install` and `gcloud auth login` plus
`gcloud auth application-default login`, I can deploy and try the sample:

```sh
export PROJECT_ID=my-project-id BUCKET=my-unique-dpd-bucket
export SNAPSHOT_DATE=2026-11-01 RUN_ID=sample-001 FIXTURE=true
scripts/deploy_gcp.sh deploy
gcloud storage cp tests/fixtures/raw/*.txt "gs://$BUCKET/raw/$SNAPSHOT_DATE/"
scripts/deploy_gcp.sh submit
gcloud run services proxy dpd-report --project "$PROJECT_ID" --region us-central1 --port 8080
# Open http://localhost:8080/reports/2026-11-01/sample-001
# Ctrl-C stops the local report proxy.
```

The job description checks stay in `infra/gcp/`; the script builds and submits
the container, a package of the program and its dependencies. The
[cloud guide](docs/gcp.md) explains real-data uploads, job state, using an earlier
snapshot for comparisons, permissions, costs and removing resources when finished.
I accept a snapshot only when its `complete.json` record exists in the bucket.

## Watching scheduled runs

I keep monitoring off unless I choose to enable it. With a Datadog Agent, the
local program that receives measurements, I can set `DPD_DATADOG_ENABLED=true`
and run the same commands. I get each command's outcome and duration, the product
count after a successful load and the checks that rejected a snapshot. The
included dashboard groups these measurements so I can compare runs.

The [monitoring guide](docs/monitoring.md) shows the local demo, Agent setup,
dashboard import and alert settings. Only counts and short deployment labels
leave the pipeline; product records, paths and exception messages stay out of
metrics. The local demo checks real network packets from Datadog's Python
client. It does not connect to the Datadog cloud, and a missing receiver never
changes whether a snapshot passes its checks.

## Check the project

```sh
make lint test
# Exercise cloud storage boundaries and the report page without a cloud account:
.venv/bin/python -m pytest -q tests/test_gcp.py
```

The tests use a small public-data sample and cover cleaning, history intervals,
failed downloads, rejected bad data, repeated runs, duplicate decisions and
unusual-strength flags. The labelled examples include cases the detector misses;
I report those misses alongside successful detections. The [pipeline reference](docs/pipeline.md)
contains the diagram, table descriptions, example queries, measured data
summaries and source links. Only marketed products are included; the approved,
cancelled and dormant catalogues are separate upstream downloads.

CI runs lint, the pipeline and task tests, two Airflow runtime tests, the sample
pipeline, the detection evaluation and the monitoring demo. The demo receives
61 Datadog packets across one successful and
one rejected snapshot. Its captured report is included in the CI artifact.
The cloud checks also validate infrastructure, build the container, run the sample
inside it and check the report server. I have verified the storage flow with
mocked clients; running it on GCP requires a project with billing enabled.

The BigQuery checks execute both saved SQL models in a pinned local emulator,
including a 60-product export, repeated dates, preserved history and cleared
flags. Separate tests check the production load configuration. I have not
measured query cost or clustering speedups in Google's cloud; the emulator
checks results and cannot reproduce cloud billing or physical storage behavior.

The Airflow tests execute 12 real task instances across two dates and one rerun.
They compare all six output tables, check retries and task ordering, and reject
missing historical archives and corrupt input. The [scheduling guide](docs/airflow.md)
explains shared-storage requirements and the limits of dated snapshots.

The Delta checks run six tests with real local Spark and Delta Lake, then execute
the notebook on the 60-product sample. They check a known outlier, date replacement,
preserved history, empty results, SQL joins, rejected input and Delta's earlier
versions. The notebook and the original local pipeline share the same cleaning
and quality gate. Each Delta table commits separately; if a run stops between
tables, I rerun that date before reviewing it.

The Elastic CI job runs 13 export and server checks with Elasticsearch and
Kibana 8.19.6. The 60-product sample produces 173 searchable documents, including
its ingredient checks and quality results. Kibana uses UTC so the snapshot dates
stay correct in different browser timezones. The tests check bulk loads, explicit
field types, repeated dates, cleared
flags, preserved history and failed document handling. It also imports and
exports the seven dashboard objects, including three saved searches. The local
stack needs no account. It lets me search the saved catalogue and inspect the
records behind each chart.

The Superset CI job imports the eleven-chart export into Superset 4.1.2, runs
all chart queries and date/class filters through its HTTP API, then exports it
again. The Azure summary job invokes the Functions SDK harness against Azurite
and starts the real Functions host to verify a timer-written blob. Both jobs
clean up their containers. Small local checks run without containers; the Linux
integration checks run in GitHub Actions.

## MongoDB catalogue copy

I can copy one accepted DuckDB snapshot into a local document database to look up a product with its ingredients and flagged strengths together. The data is the same public catalogue output; the small example below uses a real 60-product subset with one seeded duplicate, not patient data. Each product is identified by its snapshot date and drug code, so rerunning the copy replaces the same records. I compare the count of flagged strengths by ingredient with DuckDB after each copy, and stop with an error if the totals differ.

```sh
python -m pip install -e '.[dev,mongo]'
make fixture PYTHON=python SNAPSHOT_DATE=2026-11-01
docker compose -f mongo/compose.yml up -d --wait
python -m dpd_etl.mongo --db data/fixture/warehouse/dpd.duckdb --snapshot-date 2026-11-01
MONGO_TEST_URI=mongodb://localhost:27017 python -m pytest tests/test_mongo.py tests/test_mongo_integration.py -q
docker compose -f mongo/compose.yml down -v
```

The pinned MongoDB container binds only to localhost and is intended for a disposable local demonstration. The copy uses schema validation, a unique snapshot/product index and ingredient lookup indexes. The test deletes its own `dpd_mongo_integration_test` database. Do not point it at a database containing data you need. Publication is a separate step after the pipeline quality gate; interrupted bulk writes can leave a partial copy until the command succeeds again. This is not a transactional snapshot swap.

The warehouse checks exercise the Snowflake Python connector against the fakesnow
emulator and execute the dbt models with DuckDB. They include transaction rollback,
leading-zero DINs, a rejected invalid DIN, repeated loads and row reconciliation
against real Spark-written Parquet. Linux CI runs SQL Server constraints, T-SQL
views and before/after optimizer plans. The additional Delta tests check bronze,
silver and gold totals, dosage-form and stage SQL, schema rejection and freshness.
The [warehouse guide](docs/warehouse.md) explains what these checks establish and
which hosted-service behavior remains unverified.
