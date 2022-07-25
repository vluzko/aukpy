from pathlib import Path
from aukpy import queries, db

TEST_DATA = Path(__file__).parent / 'data'


def test_species_filter():
    SMALL = TEST_DATA / 'small' / 'observations.txt'
    conn = db.build_db_pandas(SMALL)
    f = queries.species('House Sparrow')
    result = f.run_pandas(conn)
    assert len(result) == 192

    assert (result['scientific_name'] == 'Passer domesticus').all()