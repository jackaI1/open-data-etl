"""Daily Health Canada catalogue snapshots, with pinned inputs for safe backfills."""
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.definitions.timetables.interval import CronDataIntervalTimetable

from dpd_etl.airflow_tasks import (
    download_partition,
    quality_check_partition,
    transform_partition,
    validate_partition,
)
from dpd_etl.config import DPD_URL


def _root() -> Path:
    # Read configuration during task execution, never while parsing the DAG.
    return Path(os.environ.get("DPD_AIRFLOW_ROOT", "data/airflow/snapshots")).resolve()


@dag(
    dag_id="health_canada_catalogue",
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "jack",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["health-canada", "catalogue"],
    doc_md="""I keep one immutable input archive and one output partition per logical date.
    Download -> validate -> transform -> quality_check. A snapshot is accepted only
    when complete.json exists. See docs/airflow.md for setup and backfills.
    """,
)
def catalogue():
    @task
    def download(snapshot_date: str):
        source = os.environ.get("DPD_AIRFLOW_SOURCE_DIR")
        download_partition(snapshot_date, _root(), Path(source) if source else None,
                           os.environ.get("DPD_AIRFLOW_URL", DPD_URL))

    @task
    def validate(snapshot_date: str):
        validate_partition(snapshot_date, _root())

    @task
    def transform(snapshot_date: str):
        fixture = os.environ.get("DPD_AIRFLOW_FIXTURE", "false").lower() == "true"
        transform_partition(snapshot_date, _root(), fixture=fixture)

    @task
    def quality_check(snapshot_date: str):
        quality_check_partition(snapshot_date, _root())

    # Only the date travels through templates. Files and tables stay on disk,
    # rather than passing a whole catalogue through Airflow's small-message store.
    download("{{ ds }}") >> validate("{{ ds }}") >> transform("{{ ds }}") >> quality_check("{{ ds }}")


catalogue_dag = catalogue()
