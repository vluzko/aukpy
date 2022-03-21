import pytest
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db

TEST_DATA = Path(__file__).parent / 'data'
SAMPLE_DATA = TEST_DATA / 'ebd_US-AL-101_202103_202103_relMar-2021.txt'
LARGE = TEST_DATA / 'large' / 'observations.txt'


def test_build_db():
    with NamedTemporaryFile() as output:
        db.build_db(SAMPLE_DATA, Path(output.name))


# @pytest.mark.skip
def test_build_db_large():
    with NamedTemporaryFile() as output:
        db.build_db(LARGE, Path(output.name))