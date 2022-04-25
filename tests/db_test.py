from collections import defaultdict
import pytest
import sqlite3
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db

TEST_DATA = Path(__file__).parent / 'data'


def test_add_line():
    line = ['URN:CornellLabOfOrnithology:EBIRD:OBS1096170792', '2021-03-20 21:48:09.640947', '25797', 'species', 'Ruby-crowned Kinglet', 'Regulus calendula', '', '', '1', '', '', '', '', 'United States', 'US', 'Alabama', 'US-AL', 'Montgomery', 'US-AL-101', '', '27', '', '', 'Oak Park', 'L2612975', 'H', '32.3692002', '-86.2869912', '2021-03-16', '09:56:00', 'obsr1051804', 'S83508237', 'Traveling', 'P22', 'EBIRD', '63', '1.754', '', '3', '1', '', '1', '1', '0', '', '', '']
    db = sqlite3.connect(':memory:')
    auk_db.create_tables(db)
    auk_db.add_line(line, db, defaultdict(dict))

    res = db.execute('SELECT * FROM observations;').fetchall()
    assert len(res) == 1
    assert res[0] == (1, 1, 1, 1, 1, 1, 1, 1, 'URN:CornellLabOfOrnithology:EBIRD:OBS1096170792', '2021-03-20 21:48:09.640947', 1, '', '', '', 32.3692002, -86.2869912, '2021-03-16', '09:56:00', 'S83508237', 63, 1.754, '', 3, 1, '', 1, 1, 0, '', '', '')


def test_build_auk_db():
    SAMPLE_DATA = TEST_DATA / 'sample' / 'observations.txt'
    with NamedTemporaryFile() as output:
        auk_db.build_db(SAMPLE_DATA, Path(output.name))


def test_build_auk_db_large():
    LARGE = TEST_DATA / 'medium' / 'observations.txt'
    with NamedTemporaryFile() as output:
        auk_db.build_db(LARGE, Path(output.name))


@pytest.mark.skip
def test_build_auk_db_large():
    LARGE = TEST_DATA / 'large' / 'observations.txt'
    with NamedTemporaryFile() as output:
        auk_db.build_db(LARGE, Path(output.name))