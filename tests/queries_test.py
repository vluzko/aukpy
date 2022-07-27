from pathlib import Path
import sqlite3
from aukpy import queries, db

from tests import SMALL_DB, MEDIUM_DB


def test_species_filter():
    conn = sqlite3.connect(str(SMALL_DB))
    f = queries.species('House Sparrow')
    result = f.run_pandas(conn)
    assert len(result) == 192

    assert (result['scientific_name'] == 'Passer domesticus').all()


def test_date_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    f = queries.date(after='2015-01-01', before='2015-01-05')
    res = f.run_pandas(conn)
    # This probably shouldn't be hard coded, given data sharing restrictions
    assert len(res) == 22897