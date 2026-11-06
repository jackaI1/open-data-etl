"""Real filesystem and Spark tests for the scheduled catalogue steps."""
import json
import zipfile
from pathlib import Path

import duckdb
import pytest

from dpd_etl import airflow_tasks as tasks
from dpd_etl.pipeline import TABLES

DATE = "2020-01-02"
OLDER = "2020-01-01"
FIXTURES = Path(__file__).parent / "fixtures" / "raw"


def archive(source, day=DATE, *, missing=None, blank_brand=False):
    source.mkdir(parents=True, exist_ok=True)
    path = source / f"{day}.zip"
    with zipfile.ZipFile(path, "w") as zf:
        for file in FIXTURES.glob("*.txt"):
            if file.name == missing:
                continue
            content = file.read_text()
            if blank_brand and file.name == "drug.txt":
                import csv
                import io
                rows = list(csv.reader(io.StringIO(content)))
                for row in rows:
                    row[4] = ""
                buf = io.StringIO()
                csv.writer(buf, quoting=csv.QUOTE_ALL).writerows(rows)
                content = buf.getvalue()
            zf.writestr(file.name, content)
    return path


def rows(root, day):
    output = tasks.partition_dir(day, root) / "output"
    with duckdb.connect(str(output / "warehouse/dpd.duckdb"), read_only=True) as con:
        return {table: sorted(map(repr, con.execute(f'SELECT * FROM "{table}"').fetchall()))
                for table in TABLES}


def test_download_pins_source_and_validate_restores_unpacked_files(tmp_path):
    source, root = tmp_path / "source", tmp_path / "runs"
    archive(source)
    tasks.download_partition(DATE, root, source)
    tasks.validate_partition(DATE, root)
    partition = tasks.partition_dir(DATE, root)
    manifest = (partition / "raw/manifest.json").read_bytes()
    archive(source, blank_brand=True)  # Upstream changing must not change a retry.
    (partition / "raw/files/drug.txt").write_text("corrupt")
    tasks.download_partition(DATE, root, source)
    tasks.validate_partition(DATE, root)
    assert (partition / "raw/files/drug.txt").read_bytes() == (FIXTURES / "drug.txt").read_bytes()
    assert (partition / "raw/manifest.json").read_bytes() == manifest


def test_download_uses_existing_curl_path(tmp_path):
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).date().isoformat()
    source = archive(tmp_path / "source", today)
    tasks.download_partition(today, tmp_path / "runs", url=source.as_uri())
    tasks.validate_partition(today, tmp_path / "runs")
    assert (tasks.partition_dir(today, tmp_path / "runs") / "raw/allfiles.zip").read_bytes() == (
        source.read_bytes()
    )


def test_backfill_requires_dated_source_instead_of_relabelling_current_data(tmp_path):
    with pytest.raises(FileNotFoundError, match="historical archive"):
        tasks.download_partition(DATE, tmp_path, url="file:///never-download-this")


@pytest.mark.parametrize("day", ["../outside", "2020-1-2", "2020-02-30", "2020-01-02/../x"])
def test_invalid_logical_date_writes_nothing(tmp_path, day):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        tasks.download_partition(day, tmp_path)
    assert not list(tmp_path.iterdir())


def test_archive_checksum_change_fails_and_revokes_completion(tmp_path):
    source = tmp_path / "source"
    archive(source)
    tasks.download_partition(DATE, tmp_path / "runs", source)
    partition = tasks.partition_dir(DATE, tmp_path / "runs")
    (partition / "complete.json").write_text("{}")
    (partition / "raw/allfiles.zip").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        tasks.validate_partition(DATE, tmp_path / "runs")
    assert not (partition / "complete.json").exists()


@pytest.mark.parametrize("missing", ["route.txt", "drug.txt"])
def test_validate_rejects_incomplete_archive(tmp_path, missing):
    source = tmp_path / "source"
    archive(source, missing=missing)
    tasks.download_partition(DATE, tmp_path / "runs", source)
    with pytest.raises(ValueError, match="missing expected files"):
        tasks.validate_partition(DATE, tmp_path / "runs")


def test_validate_rejects_shifted_columns(tmp_path):
    source = tmp_path / "source"
    path = archive(source)
    with zipfile.ZipFile(path) as zf:
        contents = {name: zf.read(name) for name in zf.namelist()}
    contents["drug.txt"] = b'"1","two fields"\n'
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in contents.items():
            zf.writestr(name, content)
    tasks.download_partition(DATE, tmp_path / "runs", source)
    with pytest.raises(ValueError, match="columns"):
        tasks.validate_partition(DATE, tmp_path / "runs")


def test_backfill_and_rerun_preserve_rows_and_other_partition(tmp_path):
    source, root = tmp_path / "source", tmp_path / "runs"
    for day in [DATE, OLDER]:
        archive(source, day)
    original = None
    for day in [DATE, OLDER, DATE]:
        tasks.download_partition(day, root, source)
        tasks.validate_partition(day, root)
        tasks.transform_partition(day, root, fixture=True)
        tasks.quality_check_partition(day, root)
        result = rows(root, day)
        report = (tasks.partition_dir(day, root) / "complete.json").read_bytes()
        assert len(result["drug_products"]) == 60
        if original is None:
            original = result, report
        elif day == DATE:
            assert (result, report) == original
        assert rows(root, DATE) == original[0]
    assert len(rows(root, OLDER)["drug_products"]) == 60


def test_failed_transform_and_final_check_revoke_completion(tmp_path):
    source, root = tmp_path / "source", tmp_path / "runs"
    archive(source, blank_brand=True)
    tasks.download_partition(DATE, root, source)
    tasks.validate_partition(DATE, root)
    partition = tasks.partition_dir(DATE, root)
    (partition / "complete.json").write_text("{}")
    from dpd_etl.quality import DataQualityError
    with pytest.raises(DataQualityError):
        tasks.transform_partition(DATE, root, fixture=True)
    assert not (partition / "complete.json").exists()
    (partition / "complete.json").write_text("{}")
    with pytest.raises(ValueError, match="quality"):
        tasks.quality_check_partition(DATE, root)
    assert not (partition / "complete.json").exists()
    report = json.loads((partition / "output/reports" / f"dq_{DATE}.json").read_text())
    assert any(not check["passed"] for check in report)


def test_quality_check_rejects_missing_check_results(tmp_path):
    source, root = tmp_path / "source", tmp_path / "runs"
    archive(source)
    tasks.download_partition(DATE, root, source)
    tasks.transform_partition(DATE, root, fixture=True)
    tasks.quality_check_partition(DATE, root)
    partition = tasks.partition_dir(DATE, root)
    report = partition / "output/reports" / f"dq_{DATE}.json"
    checks = json.loads(report.read_text())
    report.write_text(json.dumps(checks[:1]))
    with pytest.raises(ValueError, match="missing checks"):
        tasks.quality_check_partition(DATE, root)
    assert not (partition / "complete.json").exists()
