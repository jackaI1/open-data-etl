"""Production request contracts. SQL execution lives in test_bigquery_emulator.py."""

import json
from datetime import date
from unittest.mock import Mock

import duckdb
import pytest
from google.api_core.exceptions import Conflict
from google.cloud import bigquery as bq

from dpd_etl.bigquery import FIELDS, load_snapshot, review_sql, schema, validate
from dpd_etl.bigquery_review import review_query

DAY = "2026-11-01"
PROJECT = "dpd-test"


def write_partition(root, table, rows, snapshot=DAY):
    """Typed Parquet also represents empty review results."""
    types = {"STRING": "VARCHAR", "INTEGER": "BIGINT", "FLOAT": "DOUBLE",
             "BOOLEAN": "BOOLEAN", "DATE": "DATE"}
    part = root / table / f"snapshot_date={snapshot}"
    part.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute("CREATE TABLE src (" + ", ".join(
            f'{name} {types[kind]}' for name, kind in FIELDS[table].items()) + ")")
        for row in rows:
            con.execute("INSERT INTO src VALUES (" + ",".join("?" for _ in FIELDS[table]) + ")",
                        [row.get(name) for name in FIELDS[table]])
        con.execute("COPY src TO ? (FORMAT PARQUET)", [str(part / "part.parquet")])


@pytest.fixture
def parquet(tmp_path):
    for table in FIELDS:
        write_partition(tmp_path, table, [{"drug_code": 7, "din": "00000007",
                                         "brand_name": "SAMPLE"}])
    return tmp_path


def client_stub():
    client = Mock()
    client.get_dataset.return_value.location = "US"
    client.create_table.side_effect = lambda table, **kw: table
    client.load_table_from_file.return_value.job_id = "test-job"
    return client


def test_load_uses_explicit_partition_overwrite_and_publishes_last(parquet):
    client = client_stub()
    payloads = []

    def upload(file, target, **kwargs):
        payloads.append((target, [json.loads(line) for line in file]))
        return Mock(job_id=kwargs["job_id"])

    client.load_table_from_file.side_effect = upload
    result = load_snapshot(client, parquet, PROJECT, "review", DAY)
    assert result["rows"] == dict.fromkeys(FIELDS, 1)
    assert result["view"] == "dpd-test.review.reviewer_summary_20261101"
    assert len(payloads) == 3
    for call, (target, rows) in zip(client.load_table_from_file.call_args_list, payloads):
        table = target.split(".")[-1].split("$")[0]
        assert target.endswith("$20261101")
        config = call.kwargs["job_config"]
        assert config.schema == schema(table)
        assert config.write_disposition == "WRITE_TRUNCATE"
        assert config.create_disposition == "CREATE_NEVER"
        assert config.source_format == "NEWLINE_DELIMITED_JSON"
        assert config.max_bad_records == 0
        assert not config.ignore_unknown_values
        assert rows[0]["snapshot_date"] == DAY
        assert len(rows[0]["load_id"]) == 32
    assert payloads[0][1][0]["din"] == "00000007"
    assert client.mock_calls[-1][0] == "create_table"
    view = client.create_table.call_args.args[0]
    assert view.view_use_legacy_sql is False
    for call in client.create_table.call_args_list[:3]:
        table = call.args[0]
        assert table.time_partitioning.field == "snapshot_date"
        assert table.require_partition_filter
        assert table.clustering_fields


def test_all_partitions_preflight_before_remote_writes(parquet):
    for file in (parquet / "strength_anomalies").rglob("*.parquet"):
        file.unlink()
    client = client_stub()
    with pytest.raises(ValueError, match="missing Parquet"):
        load_snapshot(client, parquet, PROJECT, "review", DAY)
    assert not client.mock_calls


def test_failed_job_never_publishes_view(parquet):
    client = client_stub()
    client.load_table_from_file.return_value.result.side_effect = RuntimeError("load failed")
    with pytest.raises(RuntimeError, match="load failed"):
        load_snapshot(client, parquet, PROJECT, "review", DAY)
    assert client.create_table.call_count == 1
    assert not client.update_table.called


def test_job_conflict_resumes_same_job_and_existing_view_is_updated(parquet):
    client = client_stub()

    def create(table, **kw):
        if table.view_query:
            raise Conflict("view exists")
        return table

    client.create_table.side_effect = create
    client.load_table_from_file.side_effect = Conflict("job exists")
    load_snapshot(client, parquet, PROJECT, "review", DAY)
    assert client.get_job.call_count == 3
    for upload, lookup in zip(client.load_table_from_file.call_args_list,
                              client.get_job.call_args_list):
        assert upload.kwargs["job_id"] == lookup.args[0]
    client.update_table.assert_called_once()


def test_each_rerun_has_new_jobs_so_old_content_can_be_restored(parquet):
    client = client_stub()
    load_snapshot(client, parquet, PROJECT, "review", DAY)
    first = client.load_table_from_file.call_args.kwargs["job_id"]
    load_snapshot(client, parquet, PROJECT, "review", DAY)
    assert client.load_table_from_file.call_args.kwargs["job_id"] != first


def test_incompatible_table_is_rejected(parquet):
    client = client_stub()
    client.create_table.side_effect = lambda table, **kw: bq.Table(table.reference)
    with pytest.raises(ValueError, match="incompatible"):
        load_snapshot(client, parquet, PROJECT, "review", DAY)
    assert not client.load_table_from_file.called


def test_location_mismatch_is_rejected(parquet):
    client = client_stub()
    client.get_dataset.return_value.location = "EU"
    with pytest.raises(ValueError, match="location"):
        load_snapshot(client, parquet, PROJECT, "review", DAY)
    assert not client.load_table_from_file.called


@pytest.mark.parametrize("project,dataset,snapshot", [
    ("x`; DROP TABLE x", "review", DAY), (PROJECT, "bad.name", DAY),
    (PROJECT, "review", "2026-02-30"), (PROJECT, "review", "20261101"),
])
def test_invalid_identifiers_rejected(project, dataset, snapshot):
    with pytest.raises(ValueError):
        validate(project, dataset, snapshot)


def test_dry_run_has_billing_cap_and_never_fetches_rows():
    client = Mock()
    client.query.return_value.total_bytes_processed = 12345
    result = review_query(client, PROJECT, "review", DAY, dry_run=True,
                          maximum_bytes_billed=100000)
    config = client.query.call_args.kwargs["job_config"]
    assert config.dry_run and not config.use_query_cache
    assert config.maximum_bytes_billed == 100000
    assert result == {"dry_run": True, "bytes_processed": 12345, "rows": []}
    assert not client.query.return_value.result.called


def test_review_sql_binds_each_scan_to_one_date_and_generation():
    sql = review_sql(PROJECT, "review", DAY, "a" * 32)
    assert sql.count("DATE '2026-11-01'") == 3
    assert sql.count("'" + "a" * 32 + "'") == 3
    assert "{{" not in sql
    assert date.fromisoformat(DAY)
