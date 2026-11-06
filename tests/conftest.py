import shutil
import socket
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "raw"


@pytest.fixture(autouse=True)
def metrics_off_by_default(monkeypatch):
    monkeypatch.setenv("DPD_DATADOG_ENABLED", "false")


@pytest.fixture()
def monitoring_packets(monkeypatch):
    """Use real UDP on an ephemeral loopback port; always release our receiver."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(0.1)
        monkeypatch.setenv("DPD_DATADOG_ENABLED", "true")
        monkeypatch.setenv("DD_AGENT_HOST", "127.0.0.1")
        monkeypatch.setenv("DD_DOGSTATSD_PORT", str(receiver.getsockname()[1]))
        monkeypatch.setenv("DD_ENV", "test")
        monkeypatch.setenv("DD_VERSION", "test")
        monkeypatch.delenv("DD_DOGSTATSD_DISABLE", raising=False)

        def drain():
            packets = []
            while True:
                try:
                    packets.append(receiver.recv(8192).decode())
                except TimeoutError:
                    return packets

        yield drain
    assert receiver.fileno() == -1


@pytest.fixture(scope="module")
def spark():
    from dpd_etl.transform import get_spark

    s = get_spark("dpd-etl-tests")
    yield s
    s.stop()


@pytest.fixture()
def raw_dir(tmp_path):
    """A writable copy of the fixture so tests can corrupt it."""
    dest = tmp_path / "raw"
    shutil.copytree(FIXTURE_DIR, dest)
    return dest


@pytest.fixture
def elastic_snapshot(tmp_path):
    import json

    import duckdb

    db = tmp_path / "dpd.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute("""CREATE TABLE drug_products AS SELECT
            '1' drug_code, '00000001' din, 'SAMPLE' brand_name, 'MAKER' company_name,
            'MARKETED' current_status, 'TEST' active_ingredients, 'N' atc_group,
            DATE '2026-11-01' snapshot_date""")
        con.execute("""CREATE TABLE strength_anomalies AS SELECT
            '1' drug_code, 'TEST' ingredient, '999' strength, 'MG' strength_unit,
            999.0::DOUBLE numeric_strength, 12.0::DOUBLE robust_z, true is_anomaly,
            'scored' reason, DATE '2026-11-01' snapshot_date""")
        con.execute("""CREATE TABLE product_links AS SELECT
            '1' left_id, '2' right_id, 1.0::DOUBLE score, true is_match,
            'exact_din' reason, DATE '2026-11-01' snapshot_date""")
    report = tmp_path / "dq.json"
    report.write_text(
        json.dumps([{"name": "row_count_min", "passed": True, "observed": 1.0, "limit": ">= 1"}])
    )
    return db, report
