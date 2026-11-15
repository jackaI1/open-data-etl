# Azure SQL Database, Data Factory and Functions

This guide covers the hosted copy of the reporting tables. Four pieces work together:

| Piece | What it does | Where it lives |
| --- | --- | --- |
| Azure SQL Database | Holds the mirror tables, the reporting views and the flagged-strength summary | `infra/azure/sql.bicep`, `src/dpd_etl/sqlserver.py` |
| Stored procedures | Clear the mirror, and rebuild the summary from the product rows | created by `dpd_etl.sqlserver` |
| Data Factory | Copies one month's CSV extract into the database on a schedule | `adf/`, `infra/azure/data-factory.bicep` |
| Azure Functions | Runs the summary, archive and data-quality timers | `azure_summary/function_app.py` |

Two ways fill the database. A direct load (`python -m dpd_etl.sqlserver`) is the one I run
after a local build. The Data Factory pipeline is the one I want on a schedule. Both load the
same tables, and CI checks that they produce the same rows.

## What is in the database

Each mirror table matches a dbt table in `warehouse/`. The tables are created in this order,
parents first: `dim_company`, `dim_ingredient`, `dim_product`, `fct_product`,
`fct_product_ingredient`, `fct_duplicate_pair`. Each has a primary key, foreign keys, and
`CHECK` constraints for values that must be non-negative or digits only.

The reporting views read those tables:

- `rpt_catalogue` counts products, ingredient rows and flagged strengths for each date and drug class.
- `rpt_reconciliation` checks that the product ingredient counts agree with the ingredient table.
- `rpt_duplicate_review` joins the duplicate pairs to both products, so a reviewer sees the two DINs and brand names together.

`flagged_strength_summary` is a table filled by a stored procedure,
`dbo.usp_refresh_flagged_strength_summary`. It has one row for each snapshot date and company,
with the product count, how many products have a flagged strength, the flagged-strength total,
the ingredient-row total and the share of ingredient rows flagged. Running the procedure again
gives the same rows, so a retry never doubles a count.

`dbo.usp_clear_mirror` empties every table child-first, so the foreign keys never block it.

## Create the Azure resources

You need an Azure subscription. The database uses the free offer, which covers one database per
subscription and pauses when it is idle. Set the password in your shell, never in a file:

```sh
export DPD_SQL_ADMIN_PASSWORD='choose-a-long-password'
az group create -n dpd-rg -l canadacentral
az deployment group create -g dpd-rg -f infra/azure/main.bicep -p storageAccountName=<unique-storage-name>
az deployment group create -g dpd-rg -f infra/azure/sql.bicep \
  -p sqlServerName=<unique-server-name> sqlAdminLogin=dpdadmin sqlAdminPassword="$DPD_SQL_ADMIN_PASSWORD"
```

The server admin should only create a loader user. Run this in the database as the admin
(`sqlcmd`, or the query editor in the portal):

```sql
CREATE USER dpd_loader WITH PASSWORD = '<a different long password>';
ALTER ROLE db_datawriter ADD MEMBER dpd_loader;
ALTER ROLE db_ddladmin ADD MEMBER dpd_loader;
```

`db_ddladmin` lets the direct load create the tables, views and procedures on its first run.
The monthly copy only needs `db_datawriter` and `EXECUTE`, so you can remove `db_ddladmin`
after the first load if you prefer.

## Load the mirror directly

From a checkout after `make fixture` or a real build (`data/warehouse.duckdb` must exist):

```sh
export DPD_SQLSERVER_HOST=<unique-server-name>.database.windows.net
export DPD_SQLSERVER_DATABASE=dpd
export DPD_SQLSERVER_USER=dpd_loader
export DPD_SQLSERVER_PASSWORD='<the loader password>'
python -m dpd_etl.sqlserver --warehouse-db data/warehouse.duckdb
```

The command prints row counts for each table. It creates the tables, views and procedures on
first use, then clears and reloads the mirror. If a count or the ingredient reconciliation does
not match, the whole load is rolled back.

To try the same code without an account, the local SQL Server container in `sqlserver/compose.yml`
works with the same variables. CI runs it on every pull request.

## Monthly copy with Data Factory

The pipeline needs one folder of CSV files for each month:

```sh
make adf-export SNAPSHOT_DATE=2026-12-01          # writes data/adf/2026-12-01/*.csv and manifest.json
export DPD_ADF_CONNECTION_STRING='...'            # the storage account's connection string
make adf-upload SNAPSHOT_DATE=2026-12-01          # uploads to the sql-extracts container
```

The export writes booleans as `1` or `0`, so SQL Server's `bit` columns load them directly. Each
CSV header is the column name in SQL, and Data Factory maps columns by name.

Then deploy the factory:

```sh
az deployment group create -g dpd-rg -f infra/azure/data-factory.bicep \
  -p factoryName=<unique-factory-name> storageAccountName=<storage-name> \
     sqlServerFqdn=<unique-server-name>.database.windows.net sqlLoginName=dpd_loader \
     alertEmail=<address to notify> sqlLoginPassword="$DPD_SQL_ADMIN_PASSWORD"
```

The factory's identity gets read access to the storage account, so the copy needs no storage key.
Use the loader password for `sqlLoginPassword`, not the admin password. Deployment creates the
trigger stopped; start it in the Data Factory studio when the first monthly upload is ready.

`adf/` holds the JSON that Data Factory reads:

- `datasets/ExtractCsv.json` is one CSV file, chosen by the snapshot folder and table name.
- `datasets/DpdSqlTable.json` is one mirror table in `dbo`, chosen by table name.
- `pipelines/MonthlyExtractToSql.json` runs `usp_clear_mirror`, then copies each table
  one at a time in parent-first order, then runs `usp_refresh_flagged_strength_summary`.
- `triggers/MonthlySchedule.json` runs the pipeline on the first day of each month at 09:00 UTC.
  It passes that day as `snapshotDate`.

Every activity retries three times, with a wait between tries. The Bicep template also creates
an Azure Monitor alert on `PipelineFailedRuns`. A failed run that still fails after its retries
sends an email to the address you give.

Environment-specific values (storage account, server name and the password) live only in the
Bicep template and in your shell, so the JSON in `adf/` can be reviewed and shared.

## Data-quality timer

The Functions app has a third timer, `daily_quality_check`. It reads the Spark run's
`reports/dq_<snapshot>.json`, logs each check as `[dq] PASS` or `[dq] FAIL` with the observed
value and its limit, and raises an error when any check failed. A failed invocation shows in the
Functions monitor and in the host's logs, so a bad snapshot is visible without opening a report.

| Setting | Default | Meaning |
| --- | --- | --- |
| `DPD_QUALITY_SCHEDULE` | `0 45 8 * * *` | NCRONTAB schedule, 08:45 UTC daily |
| `DPD_DATA_DIR` | required | The folder that holds `reports/` and `warehouse/` |
| `DPD_SNAPSHOT_DATE` | today (UTC) | The snapshot to check |

## Run the Functions app locally

With Azure Functions Core Tools v4 and Azurite installed:

```sh
cp azure_summary/local.settings.json.example azure_summary/local.settings.json
# set DPD_DATA_DIR to an absolute path, then:
cd azure_summary && func start
```

To deploy to a function app in your subscription, from the same folder:

```sh
func azure functionapp publish <function-app-name>
```

Set `DPD_DATA_DIR`, `DPD_ARCHIVE_CONNECTION_STRING` and the schedules in the app's settings first.

## What CI checks, and what it cannot check

- The Bicep templates build and lint with the Azure CLI's Bicep compiler.
- The Data Factory JSON is checked for references between linked services, datasets, pipeline and
  trigger, the parameters each dataset receives, retries on every activity, and the same table order
  as the Python loader.
- A real SQL Server container runs the loader, the constraints, the views, the summary procedure,
  and a test that loads the CSV extract in the pipeline's order and compares counts with the direct load.
- Azurite stores and reads back the uploaded extract, and the Functions host runs the timers.

CI does not deploy to Azure. It cannot prove that the subscription accepts the free offer, that
Data Factory accepts the linked-service settings at deployment, or that an alert email arrives. The
first deployment in a real subscription is the real test of those parts.
