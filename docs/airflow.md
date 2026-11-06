# Scheduling catalogue snapshots

I use Apache Airflow to run the catalogue each day and see which step needs
attention when a run fails. A DAG is Airflow's ordered list of tasks. Mine runs
`download -> validate -> transform -> quality_check`; a failed step retries twice
with increasing delays before Airflow marks the run failed.

## Try two dates without a download or a server

Use Python 3.12 and Java 17, as in the [local setup](pipeline.md#local-setup).
From the repository root:

```sh
make install
make airflow-install
export PYSPARK_PYTHON="$PWD/.venv/bin/python" SPARK_LOCAL_IP=127.0.0.1
# On macOS with Homebrew Java:
# export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export DPD_AIRFLOW_ROOT="$PWD/data/airflow/snapshots"
export DPD_AIRFLOW_SOURCE_DIR="$PWD/data/airflow/archives"
export DPD_AIRFLOW_FIXTURE=true
.venv/bin/python - <<'PY'
import os
from pathlib import Path
from zipfile import ZipFile
source = Path(os.environ['DPD_AIRFLOW_SOURCE_DIR'])
source.mkdir(parents=True, exist_ok=True)
for day in ['2020-01-01', '2020-01-02']:
    with ZipFile(source / f'{day}.zip', 'w') as archive:
        for file in Path('tests/fixtures/raw').glob('*.txt'):
            archive.write(file, file.name)
PY
.venv/bin/airflow db migrate
.venv/bin/airflow dags test health_canada_catalogue 2020-01-02
.venv/bin/airflow dags test health_canada_catalogue 2020-01-01
.venv/bin/airflow dags test health_canada_catalogue 2020-01-01
cat data/airflow/snapshots/snapshot_date=2020-01-01/complete.json
```

These commands run real Airflow task instances locally, without starting a web
server or scheduler. Both dates should contain 60 products. The dates label a
synthetic sample; they are not claims about Canada's catalogue in January 2020.
The third run replaces the older date's results. It keeps the newer date intact.
Airflow is pinned to 3.3.2, with its [official Python 3.12 constraints](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt)
used during installation. The [Airflow testing guide](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/debug.html)
explains the local test runner.

## Schedule the real catalogue

In the same terminal, switch off the sample settings:

```sh
unset DPD_AIRFLOW_SOURCE_DIR DPD_AIRFLOW_FIXTURE
.venv/bin/airflow standalone
```

Keep this command in the foreground. Open the local address printed by Airflow,
sign in with the local credentials it prints, and unpause
`health_canada_catalogue`. Ctrl-C stops the local Airflow services when finished.
Standalone is for local use; a long-running deployment needs an operated Airflow
installation. Run this DAG on one machine with Java 17, curl and this package
installed. All task processes must share the same absolute `DPD_AIRFLOW_ROOT`.
Separate workers without shared storage are not supported.

The daily schedule closes an interval at midnight UTC. Its logical date, the
date Airflow assigns to that interval, names the output directory. For example,
the run after midnight on November 6 uses November 5. This identifies a scheduled
collection, not an upstream publication date. `catchup=False` avoids downloading
an entire backlog on first startup. The earliest supported date is 2020-01-01.
One active run at a time prevents overlapping writers; keep the same setting
when creating a backfill. Each task has a 20-minute timeout.

## Rerun and backfill

A backfill runs earlier scheduled dates. I retain each date's first downloaded
ZIP and its SHA-256 checksum, a fingerprint of its bytes. Rerunning any task uses
those pinned bytes even if the live catalogue changes. Each date has its own
DuckDB database and Parquet tables, so processing dates in a different order
cannot change another date's result.

For dates never collected, put the actual saved archives in an absolute source
directory as `YYYY-MM-DD.zip` and set `DPD_AIRFLOW_SOURCE_DIR` for every Airflow
process. Restart local services after changing their environment. Without a
pinned archive or a dated source file, old dates fail rather than silently
calling today's catalogue historical. New downloads are allowed only for today
or yesterday in UTC. The upstream URL is configurable with `DPD_AIRFLOW_URL`.

With the scheduler and API server running and the DAG unpaused:

```sh
.venv/bin/airflow backfill create --dag-id health_canada_catalogue \
  --from-date 2020-01-01 --to-date 2020-01-02 \
  --max-active-runs 1 --reprocess-behavior completed
```

Use this example range with the sample archives above, or replace it with dates
for which you have real archives. Airflow's [backfill reference](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/backfill.html)
explains the reprocessing choices. Clearing failed tasks in the UI retries them
against their pinned input. Never run a second scheduler or manual test against
the same partition while a task is writing it.

## Files and acceptance

Each `data/airflow/snapshots/snapshot_date=YYYY-MM-DD/` contains:

- `raw/allfiles.zip` and `raw/manifest.json`: pinned input and checksum.
- `raw/files/`: the seven source files, restored from the ZIP during validation.
- `output/parquet/`: all six existing cleaned and review tables, partitioned by date.
- `output/warehouse/dpd.duckdb`: the queryable database for this date.
- `output/reports/`: the existing quality and review reports.
- `complete.json`: source checksum and table counts, written only after final acceptance.

Validation rejects missing files, empty files, broken CSV and shifted columns.
Transformation uses the existing Spark cleaning, joins, history, duplicate
review and strength checks. Its quality gate still runs before writing tables.
The final task checks the saved report, every table's date and every persisted
row against Parquet before creating `complete.json`. A successful sample has
60 products; the completion record lists counts for all six tables.

Every step removes the previous completion marker before doing work. Readers
must require that marker: Parquet and DuckDB are separate stores, so a failed
write can leave partial files until that date is rerun. Results are identical
rows and deterministic reports for the same archive and code, rather than
identical Parquet filenames or database bytes. Raw archives remain pinned;
derived outputs overwrite their own partition.

Date-isolated runs use the minimum-count, null, uniqueness, history and orphan
checks. They do not compare against whichever date happened to run previously.
The original command-line pipeline still offers its previous-snapshot row-drop
check and shared warehouse. Keep source archives when upgrading transformation
code: changes to the code can intentionally change derived results.

## Tests and cleanup

```sh
make airflow-test
make lint test
```

The Airflow target checks import safety, task ordering, retry settings, real task
functions, malformed input and historical-source requirements. Its integration
test runs all four Airflow tasks three times: newer date, older date, then older
date again. It compares every row in all six tables and both completion records.
I also ran the four steps against the live Health Canada archive on 2026-11-06
UTC with normal thresholds: 13,379 products and 46,045 status-history rows passed
the source validation, existing quality gate and persisted-output comparison.

CI installs Airflow explicitly and runs this target, so the runtime tests cannot
be skipped because Airflow is missing. The regular CI job also keeps running
all existing tests and sample commands.

Tests use temporary metadata databases and close their connections. They start
no persistent scheduler or web server. To remove a local demo after stopping
`airflow standalone`, delete only this project's `.airflow/` and
`data/airflow/` directories. Never delete archived real snapshots you need for
future reruns. No credentials are needed for the public download.
