from pathlib import Path
from aukpy import queries, db

TEST_DATA = Path(__file__).parent / 'data'


def test_species_filter():
    SMALL = TEST_DATA / 'small' / 'observations.txt'
    conn = db.build_db_pandas(SMALL)
    f = queries.species('House Sparrow')
    result = f.run(conn)
    assert len(result) == 5
    for r in result:
        assert 'Passer domesticus' in r