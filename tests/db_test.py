import pytest
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db


from tests import SMALL, MEDIUM, LARGE, SMALL_DB


def test_build_small():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(SMALL, Path(output.name))


def test_build_medium():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(MEDIUM, Path(output.name))
        cursor = db.execute("select id from observation")
        res = cursor.fetchall()
        assert len(res) == 999999


def test_build_incremental_small():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_incremental(SMALL, Path(output.name), max_size=1000)
        cursor = db.execute("select id from observation")
        res = cursor.fetchall()
        assert len(res) == 10000


def test_build_incremental():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_incremental(MEDIUM, Path(output.name))
        cursor = db.execute("select id from observation")
        res = cursor.fetchall()
        assert len(res) == 999999


@pytest.mark.skip
def test_build_large():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db(LARGE, Path(output.name))
