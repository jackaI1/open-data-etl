"""A daily Delta run over the same pinned archives as the original local pipeline."""
import os
from datetime import timedelta
from pathlib import Path

from airflow.sdk import dag, task

from dpd_etl.airflow_tasks import download_partition, partition_dir, validate_partition


@dag(dag_id='health_canada_lakehouse', schedule='@daily', start_date=__import__('pendulum').datetime(2026, 1, 1, tz='UTC'),
     catchup=False, max_active_runs=1,
     default_args={'owner': 'jack', 'retries': 2, 'retry_delay': timedelta(minutes=1),
                   'execution_timeout': timedelta(minutes=30)})
def lakehouse():
    def root():
        return Path(os.environ.get('DPD_AIRFLOW_ROOT', 'data/airflow/snapshots')).resolve()

    @task
    def download(day):
        source = os.environ.get('DPD_AIRFLOW_SOURCE_DIR')
        download_partition(day, root(), Path(source) if source else None)

    @task
    def validate(day):
        validate_partition(day, root())

    @task
    def model(day):
        from datetime import date

        from dpd_etl.delta_job import get_delta_spark
        from dpd_etl.lakehouse import run_lakehouse
        spark = get_delta_spark()
        try:
            partition = partition_dir(day, root())
            return run_lakehouse(
                spark, raw_dir=str(partition / 'raw/files'),
                delta_root=os.environ.get('DPD_DELTA_ROOT', 'data/lakehouse'),
                snapshot_date=day, report_dir=str(partition / 'reports'),
                fixture=os.environ.get('DPD_AIRFLOW_FIXTURE', 'false') == 'true',
                as_of=date.fromisoformat(day),
            )
        finally:
            spark.stop()

    download('{{ ds }}') >> validate('{{ ds }}') >> model('{{ ds }}')


lakehouse_dag = lakehouse()
