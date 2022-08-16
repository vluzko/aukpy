import pandas as pd
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db, queries

from tests import SMALL, MEDIUM, WITH_ATLAS


def compare_tables(df: pd.DataFrame, original: pd.DataFrame):
    df.sort_values(by="global_unique_identifier", inplace=True)
    df.index = range(len(df))
    df.fillna("", inplace=True)
    original.sort_values(by="global_unique_identifier", inplace=True)
    original.index = range(len(original))
    original.fillna("", inplace=True)

    assert len(df) == len(original)
    assert set(df.columns) == set(original.columns)

    # Don't want to deal with string formatting
    original["last_edited_date"] = (
        pd.to_datetime(original["last_edited_date"]).astype(int) // 1e9
    )

    for column in df.columns:
        comp = df[column] == original[column]
        assert comp.all()


def test_rebuild_small():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(SMALL, Path(output.name))
        q = queries.no_filter()

        df = auk_db.undo_compression(q.run_pandas(db))
        original = auk_db.read_clean(SMALL)
        compare_tables(df, original)


def test_rebuild_incremental():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_incremental(SMALL, Path(output.name), max_size=1000)
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


def test_rebuild_incremental_medium():
    with NamedTemporaryFile() as output:
        db = auk_db.build_db_incremental(MEDIUM, Path(output.name), max_size=100000)
        q = queries.no_filter()

        df = auk_db.undo_compression(q.run_pandas(db))
        original = auk_db.read_clean(MEDIUM)
        compare_tables(df, original)


def test_rebuild_atlas():

    with NamedTemporaryFile() as output:
        db = auk_db.build_db_pandas(WITH_ATLAS, Path(output.name))
        q = queries.no_filter()

        df = auk_db.undo_compression(q.run_pandas(db))
        original = auk_db.read_clean(WITH_ATLAS)
        compare_tables(df, original)


def test_rebuild_random():
    df = auk_db.read_clean(MEDIUM)
    from tests import gen_mock_data

    new_df = gen_mock_data.scramble_observations(df)
