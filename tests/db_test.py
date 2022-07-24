from collections import defaultdict
import pytest
import sqlite3
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db

TEST_DATA = Path(__file__).parent / 'data'


def test_build_pandas_db_small():
    SMALL = TEST_DATA / 'small' / 'observations.txt'
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(SMALL, Path(output.name))


@pytest.mark.skip
def test_build_pandas_db_medium():
    MEDIUM = TEST_DATA / 'medium' / 'observations.txt'
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(MEDIUM, Path(output.name))


@pytest.mark.skip
def test_build_auk_db_large():
    LARGE = TEST_DATA / 'large' / 'observations.txt'
    with NamedTemporaryFile() as output:
        db = auk_db.build_db(LARGE, Path(output.name))