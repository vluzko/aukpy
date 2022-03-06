from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db


SAMPLE_DATA = Path(__file__).parent / 'data' / 'ebd_US-AL-101_202103_202103_relMar-2021.txt'


def test_build_db():
    with NamedTemporaryFile() as output:
        db.build_db(SAMPLE_DATA, Path(output.name))