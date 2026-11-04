import shutil
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "raw"


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
