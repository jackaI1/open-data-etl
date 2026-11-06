# A catalogue I can query without counting a product twice

I keep one row per product and date, with separate company and ingredient tables.
That layout is called a star schema: the product facts contain counts, and the
smaller dimension tables explain what those counts refer to. A product with three
ingredients still counts as one product in the reporting view.

## Try it without an account

From the repository root, with Python 3.12 and Java 17:

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[dev,mongo,warehouse]'
export PATH="$PWD/.venv/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python"
make fixture SNAPSHOT_DATE=2026-11-01
python -m dpd_etl.warehouse --parquet-root data/fixture/parquet \
  --snapshot-date 2026-11-01 --local-db data/warehouse.duckdb
export DPD_WAREHOUSE_PATH="$PWD/data/warehouse.duckdb"
dbt build --project-dir warehouse --profiles-dir warehouse
dbt docs generate --project-dir warehouse --profiles-dir warehouse
DPD_SPARK_PARQUET=data/fixture/parquet python -m pytest tests/warehouse/test_warehouse.py -q
```

I can open `data/warehouse.duckdb` with DuckDB and run
`SELECT * FROM analytics.rpt_catalogue`. The fixture is a real public catalogue
subset with one deliberately added old duplicate; it contains 61 raw rows and
60 products. The two-product warehouse unit example is entirely synthetic.
The dictionary and metric definitions are in [data-dictionary.md](data-dictionary.md).

## Use a Snowflake trial

I need a trial account, a database, a small warehouse and a role that can create
schemas and tables in that database. I create those resources in the Snowflake
web console, set an auto-suspend interval, and export these values in my shell:

```sh
export SNOWFLAKE_ACCOUNT='<organization-account>'
export SNOWFLAKE_USER='<user>'
# Set SNOWFLAKE_PASSWORD through a secret manager or a hidden shell prompt.
export SNOWFLAKE_WAREHOUSE='DPD_WH'
export SNOWFLAKE_DATABASE='DPD'
export SNOWFLAKE_ROLE='DPD_TRANSFORMER'
python -m dpd_etl.warehouse --parquet-root data/fixture/parquet --snapshot-date 2026-11-01
dbt build --target snowflake --project-dir warehouse --profiles-dir warehouse
dbt docs generate --target snowflake --project-dir warehouse --profiles-dir warehouse
```

`DPD_RAW_SCHEMA` optionally changes the default `RAW` schema. The dbt models go
in `ANALYTICS`. I grant only this database and warehouse to the loader/transformer
role, keep passwords out of source control, and suspend or remove trial resources
when finished. A Snowflake password-authentication policy may require a programmatic
access token in place of a password. The connector also supports key-pair login;
this command currently exposes password/token authentication only.

The loader uses `snowflake-connector-python`, typed columns and bound values.
It reads the exact dated Parquet files Spark wrote, including empty pair tables.
It creates tables before starting a transaction because Snowflake schema changes
commit implicitly. It then replaces all four source tables and a count manifest
in one transaction. Counts must match before it commits. Concurrent publishers
for the same date are not supported. Historical dates remain available.

I have exercised the production connector with fakesnow, a Snowflake emulator,
and executed the dbt models with both the DuckDB and Snowflake adapters. CI also builds all models and runs all 31 dbt data tests through the real
Snowflake adapter against fakesnow. I have not loaded a hosted Snowflake trial, measured its
query plans or used a paid dbt Semantic Layer service. These are account-free
checks of the implementation, not evidence of a hosted deployment.

## Tables and checks

- `stg_products` and `stg_ingredients` expose the typed sources and stable keys.
- `dim_product` and `dim_company` include the snapshot date in their keys. Missing
  companies have an explicit UNKNOWN member per date. When a code has several
  source names on one date, I choose the first ordered name deterministically.
- `dim_ingredient` uses normalized ingredient names across dates.
- `fct_product` aggregates ingredient and review counts before joining them.
- `fct_product_ingredient` keeps the product/ingredient/strength/unit grain.
- `fct_duplicate_pair` preserves both candidate product keys, score and decision.
- `rpt_catalogue` counts products, ingredients and flagged strengths by date/class.

DIN means Drug Identification Number. It must contain exactly eight decimal
characters; I keep it as text so leading zeros survive. DINs are not required
to be globally unique across dates or products. dbt tests validate the accepted
`valid` status as well as keys, required values, references and counts against
the source manifest. A Python integration test compares the built dimensions
and facts with the exact Spark output. It also injects a bad DIN to prove a
failure is caught.

## One definition for reports

`src/dpd_etl/metrics.yml` defines each metric's SQL expression once.
`python -m dpd_etl.metrics` generates the reporting model, dbt semantic annotations
and the dictionary/lineage document. Spark uses the same expressions for its
report view. CI checks that these committed generated files have not drifted.
Semantic annotations describe product identity, daily time grain and drug class;
they do not require a hosted metrics endpoint to run the reporting SQL.

## A SQL Server reporting copy

On Linux with Docker, after building the local warehouse:

```sh
uv pip install --python .venv/bin/python -e '.[warehouse]'
# Set a disposable strong DPD_SQLSERVER_PASSWORD in the shell.
docker compose -f sqlserver/compose.yml up -d --wait
python -m dpd_etl.sqlserver --warehouse-db data/warehouse.duckdb --plans data/sqlplans
python -m pytest tests/warehouse/test_sqlserver.py -q
docker compose -f sqlserver/compose.yml down --volumes --remove-orphans
```

This is a full mirror of the checked dbt tables in a dedicated SQL Server
database, `tempdb` by default for the disposable demo. I never point it at an
unrelated database. `DPD_SQLSERVER_HOST`, `PORT`, `USER` and `DATABASE` configure a
persistent dedicated target. Inserts, deletes, key constraints and reconciliation
run in one transaction. Three T-SQL views show catalogue totals, ingredient
reconciliation and duplicate review pairs.

The loader captures XML optimizer plans before and after adding a date/class
index that includes the count columns. The fixture is too small to establish a
speedup; the plans are CI artifacts for inspection. I do not claim the old
13,379-product download was loaded into SQL Server: this integration uses the
60-product fixture. Real full-download counts vary over time.
