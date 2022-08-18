import pytest
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db

from tests import SMALL, MEDIUM, LARGE, SMALL_MOCKED, SKIP_NON_MOCKED


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_build_small():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(SMALL, Path(output.name))


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_build_medium():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(MEDIUM, Path(output.name))
        cursor = db.execute("select id from observation")
        res = cursor.fetchall()
        assert len(res) == 999999


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_build_incremental_small():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_incremental(SMALL, Path(output.name), max_size=1000)
        cursor = db.execute("select id from observation")
        res = cursor.fetchall()
        assert len(res) == 10000


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_build_incremental():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_incremental(MEDIUM, Path(output.name))
        cursor = db.execute("select id from observation")
        res = cursor.fetchall()
        assert len(res) == 999999


@pytest.mark.skip
def test_build_large():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(LARGE, Path(output.name))


def test_build_small_mocked():
    for p in SMALL_MOCKED:
        with NamedTemporaryFile() as output:
            auk_db.build_db_pandas(p, Path(output.name))
