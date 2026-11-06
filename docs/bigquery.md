# Reviewing snapshots in BigQuery

I use this destination when I want to query my saved catalogue from another
computer or share a review query. It reads the existing cleaned Parquet output;
it does not download or clean a second copy of the catalogue. The local DuckDB
output, detection and cloud-run options still work independently.

## Try a snapshot

Install Python 3.10 to 3.12, Java 17 and uv as in the main README, then:

```sh
make install
export PYSPARK_PYTHON="$PWD/.venv/bin/python" SPARK_LOCAL_IP=127.0.0.1
make fixture SNAPSHOT_DATE=2026-11-01
```

For a local check with no Google account, run:

```sh
scripts/test_bigquery.sh tests/test_bigquery_emulator.py
```

This downloads the pinned emulator v0.8.1 with a SHA-256 check into a temporary
directory, starts it on an available loopback port and removes it afterwards.
The tests load the real 60-product sample, execute the installed review view,
rerun a date, add a second date, replace older data and recover from an interrupted
upload. A second case checks multiple ingredients, null company names, leading
zeros in DINs, cleared flags and corrected inputs. The server has no credentials
and no public listener. Linux x86-64 and Apple Silicon have automatic downloads;
on other platforms set `BIGQUERY_EMULATOR_BIN` to a v0.8.1 binary.

## Load a Google Cloud project

I need a project with the BigQuery API enabled and enough storage/query quota.
A sandbox can be used within its limits. For a normal project, I authenticate
with Google's CLI instead of saving keys in this repo:

```sh
export PROJECT_ID=my-project-id
# In a project I own, if the API is not already enabled:
gcloud services enable bigquery.googleapis.com --project "$PROJECT_ID"
gcloud auth application-default login
.venv/bin/python -m dpd_etl.bigquery \
  --project "$PROJECT_ID" --dataset dpd_review --location US \
  --snapshot-date 2026-11-01 --parquet-root data/fixture/parquet
```

The identity needs permission to create datasets in that project, create/update
tables and views in this dataset, and create BigQuery jobs. An administrator can
create the dataset first and grant BigQuery Data Editor on that dataset and
BigQuery Job User on the project instead. Existing dataset locations must match
`--location`; locations cannot be changed by rerunning the command. Prefer a
dedicated dataset for this project.

For a full catalogue run, use `make pipeline` and `--parquet-root data/parquet`.
Use the date passed to that pipeline run. Only export snapshots that passed the
pipeline's quality gate. The loader requires all three dated partitions and
prepares every upload before writing remotely. It rejects unexpected columns,
null product keys, non-finite numbers and empty product snapshots.

The command prints row counts, job IDs and the installed view name. I can query
that view in the BigQuery console or use the saved review queue, which returns
up to 100 products with flagged or unscored strengths:

```sh
.venv/bin/python -m dpd_etl.bigquery_review \
  --project "$PROJECT_ID" --dataset dpd_review --location US \
  --snapshot-date 2026-11-01 --dry-run
# After checking bytes_processed, run with a limit I choose:
.venv/bin/python -m dpd_etl.bigquery_review \
  --project "$PROJECT_ID" --dataset dpd_review --location US \
  --snapshot-date 2026-11-01 --maximum-bytes-billed 1073741824
```

`--dry-run` validates the query and estimates bytes without fetching records.
The default billing limit is 1 GiB; a query estimated above the limit fails.
The console equivalent is:

```sql
SELECT drug_code, din, brand_name, flagged_strengths, unscored_strengths
FROM `my-project-id.dpd_review.reviewer_summary_20261101`
WHERE flagged_strengths > 0 OR unscored_strengths > 0
ORDER BY flagged_strengths DESC, drug_code;
```

These flags are prompts to inspect catalogue records, not medical advice or
proof that a product is wrong. Unsupported strengths remain visible as unscored.

## Tables and replacement

`src/dpd_etl/bigquery.py` defines the complete schema. DINs and original strengths
stay strings, preserving leading zeros and source notation. Dates, counts and
flags have explicit DATE, INTEGER and BOOLEAN types. Uploads stream batches of
1,000 rows to temporary files and use newline-delimited JSON batch load jobs.
No Spark session or intermediate cloud bucket is needed for the export.

All three tables are partitioned by `snapshot_date`, meaning each date is a
separate part of the table. All require a date filter. Clustering sorts related
values into storage blocks so selective queries can read fewer blocks:

| Table | Cluster columns, in order | Purpose |
| --- | --- | --- |
| `drug_products` | `atc_group`, `company_name`, `drug_code` | Review a therapeutic group or company |
| `drug_ingredients` | `ingredient`, `drug_code` | Follow an ingredient across products |
| `strength_anomalies` | `is_anomaly`, `ingredient`, `drug_code` | Find flagged strengths within peers |

A load targets `table$20261101` with `WRITE_TRUNCATE`. BigQuery replaces that
partition atomically, including replacing flags with an empty result, while
preserving every other date. Repeating the operation therefore does not append
duplicates. Each invocation gets fresh job IDs so restoring an older input after
a correction actually runs again. A conflicting job submission resumes its exact
job ID and waits for completion rather than submitting a second copy.

The three jobs are **not one transaction**. I run only one writer for a given
dataset/date at a time. Products load first, and each row carries the invocation's
`load_id`. The dated review view is installed only after all three jobs succeed;
it filters every source to that date and load ID. If a later rerun stops midway,
the old view returns no products rather than combining new products with older
flags. Rerunning all three jobs repairs it. Direct base-table readers can see
partial results during a load and should wait for the successful command output.
Concurrent writers for the same date need external serialization.

The saved SQL first counts ingredients and flags per product, then joins those
counts to products. Joining the raw child tables would multiply counts for
products with several ingredients. Products without ingredients or flags still
appear with zero counts. Each date gets its own `reviewer_summary_YYYYMMDD` view,
so a backfill does not move a shared latest-date pointer.

## Cost and performance choices

I filter every base scan to one literal date inside the view, project only the
columns the review needs and aggregate before joining. Daily partitions match
my snapshot replacement and review pattern. Clustering is useful for selective
base-table queries, but a full-day summary still needs to read that day's review
columns. The tiny sample does not demonstrate a speedup. Small partitions also
have overhead; a much larger history might justify monthly partitions, but that
would need a different replacement design.

`LIMIT 100` bounds the displayed queue, not its scan cost. I disable the query
cache for byte measurements, report `total_bytes_processed` and use
`maximum_bytes_billed` for execution. Clustering can make pre-execution estimates
conservative. The emulator cannot measure Google's bytes billed, block pruning,
latency or pricing; I have not measured a cloud speedup or dollar saving. Batch
loads avoid streaming writes, but stored snapshots and queries still consume
quota or incur charges according to the project and region. I check the
[pricing page](https://cloud.google.com/bigquery/pricing) before larger runs.

I remove a dated view and its three partitions when I no longer need a snapshot.
To remove **all** exported data, only for the dedicated dataset created above:

```sh
bq rm --recursive --dataset "$PROJECT_ID:dpd_review"
```

Source files and local outputs are separate and remain on my computer.

## What CI proves

CI executes the shipped SQL through the official Python BigQuery client against
[goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator), not through
a SQL translation to DuckDB. It also checks the exact production SDK requests:
explicit schema, partition destination, strict loading and overwrite mode.
The SQL files are included in the installed Python package.

The emulator v0.8.1 has four compatibility gaps, isolated in a **test-only**
adapter in `tests/test_bigquery_emulator.py`: no partition decorators (emulated
with a dated DELETE then an append load), ISO DATE uploads need a SQL write to
normalize their internal representation, duplicate resource creation returns
500 instead of 409, and view PATCH changes metadata without replacing executable
SQL (the test deletes and recreates the view). Production uses normal SDK
operations and has none of these workarounds. The emulator checks query results
and replacement outcomes; it does not prove Google's partition atomicity, IAM,
partition-filter enforcement, billing or physical clustering. Those need a real
project check before relying on cloud performance or access controls.

I followed Google's guides for
[partition loads](https://docs.cloud.google.com/bigquery/docs/load-data-partitioned-tables),
[clustered tables](https://docs.cloud.google.com/bigquery/docs/creating-clustered-tables),
[dry runs](https://docs.cloud.google.com/bigquery/docs/running-queries#dry-run) and
[cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs).
