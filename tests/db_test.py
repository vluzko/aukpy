import pytest
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db


from tests import SMALL, MEDIUM, LARGE


def test_build_small():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(SMALL, Path(output.name))


def test_build_medium():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(MEDIUM, Path(output.name))


@pytest.mark.skip
def test_build_large():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db(LARGE, Path(output.name))
