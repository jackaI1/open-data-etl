"""Import and execute the actual DAG against an isolated Airflow metadata DB."""
import json
import logging
import os
import shutil
import zipfile
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import duckdb
import pytest

try:
    version("apache-airflow")
except PackageNotFoundError:
    pytest.skip("install the airflow extra to run DAG integration tests", allow_module_level=True)

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def runtime(tmp_path_factory):
    home = tmp_path_factory.mktemp("airflow-runtime")
    settings = {
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__DAGS_FOLDER": str(REPO / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "true",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{home}/airflow.db",
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": "60",
        "AIRFLOW__LOGGING__BASE_LOG_FOLDER": str(home / "logs"),
        "DPD_AIRFLOW_ROOT": str(home / "snapshots"),
        "DPD_AIRFLOW_SOURCE_DIR": str(home / "archives"),
        "DPD_AIRFLOW_FIXTURE": "true",
    }
    previous_log_level = logging.getLogger().level
    previous = {key: os.environ.get(key) for key in settings}
    os.environ.update(settings)
    try:
        yield home
    finally:
        # Dispose only this test process's metadata connections before deleting
        # its owned temporary state. No persistent Airflow services are started.
        from airflow import settings as airflow_settings
        airflow_settings.Session.remove()
        airflow_settings.engine.dispose()
        shutil.rmtree(home)
        # dag.test configures global logging. Restore the caller's verbosity so
        # later Spark cleanup does not write INFO to an expired capture stream.
        logging.getLogger().setLevel(previous_log_level)
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def load_dag():
    from airflow.dag_processing.dagbag import DagBag
    bag = DagBag(dag_folder=REPO / "dags", safe_mode=False)
    assert not bag.import_errors, bag.import_errors
    assert "health_canada_catalogue" in bag.dags, "catalogue DAG is missing"
    return bag.dags["health_canada_catalogue"]


def test_dag_import_topology_and_retry_policy(runtime):
    dag = load_dag()
    assert set(dag.task_ids) == {"download", "validate", "transform", "quality_check"}
    for before, after in zip(["download", "validate", "transform"],
                             ["validate", "transform", "quality_check"], strict=True):
        assert dag.get_task(before).downstream_task_ids == {after}
    assert dag.get_task("quality_check").downstream_task_ids == set()
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert dag.timetable.expression == "0 0 * * *"
    assert all(task.retries == 2 and task.retry_exponential_backoff for task in dag.tasks)
    assert all(task.execution_timeout.total_seconds() == 1200 for task in dag.tasks)
    # Parsing defines the schedule only; no archive downloads or output directories.
    assert not (runtime / "snapshots").exists()
    assert not (runtime / "archives").exists()


def test_real_dag_backfill_and_rerun(runtime):
    import pendulum
    from airflow.utils import db

    db.initdb()
    source = runtime / "archives"
    source.mkdir()
    for day in ["2020-01-01", "2020-01-02"]:
        with zipfile.ZipFile(source / f"{day}.zip", "w") as zf:
            for file in (REPO / "tests/fixtures/raw").glob("*.txt"):
                zf.write(file, file.name)
    dag = load_dag()

    def snapshot(day):
        partition = runtime / "snapshots" / f"snapshot_date={day}"
        with duckdb.connect(str(partition / "output/warehouse/dpd.duckdb"), read_only=True) as con:
            tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
            rows = {table: sorted(map(repr, con.execute(f'SELECT * FROM "{table}"').fetchall()))
                    for table in tables}
        return rows, json.loads((partition / "complete.json").read_text())

    originals = {}
    # Execute newer first, backfill older, then rerun older. These are real
    # Airflow task instances using the Task SDK and metadata database, no mocks.
    for day in ["2020-01-02", "2020-01-01", "2020-01-01"]:
        run = dag.test(logical_date=pendulum.parse(day),
                       run_after=pendulum.parse(day).add(days=1))
        assert run.state == "success"
        assert len(run.get_task_instances()) == 4
        assert all(ti.state == "success" for ti in run.get_task_instances())
        current = snapshot(day)
        assert len(current[0]["drug_products"]) == 60
        assert current[1]["logical_date"] == day
        if day in originals:
            assert current == originals[day]
        originals[day] = current
        for existing_day, original in originals.items():
            assert snapshot(existing_day) == original
