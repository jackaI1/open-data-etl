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
