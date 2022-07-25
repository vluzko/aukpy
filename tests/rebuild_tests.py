import pandas as pd
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db, queries

from tests import SMALL, MEDIUM


def compare_tables(df: pd.DataFrame, original: pd.DataFrame):
    df.sort_values(by='global_unique_identifier', inplace=True)
    df.index = range(len(df))
    df.fillna('', inplace=True)
    original.sort_values(by='global_unique_identifier', inplace=True)
    original.index = range(len(original))
    original.fillna('', inplace=True)

    assert len(df) == len(original)
    assert set(df.columns) == set(original.columns)

    for column in df.columns:
        assert (df[column] == original[column]).all()


def test_rebuild_small():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(SMALL, Path(output.name))
        q = queries.no_filter()

        df = auk_db.undo_compression(q.run_pandas(db))
        original = auk_db.read_clean(SMALL)
        compare_tables(df, original)


def test_rebuild_medium():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(MEDIUM, Path(output.name))
        q = queries.no_filter()
        df = auk_db.undo_compression(q.run_pandas(db))
        original = auk_db.read_clean(MEDIUM)

        compare_tables(df, original)