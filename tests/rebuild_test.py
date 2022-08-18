import pandas as pd
import pytest
from tempfile import NamedTemporaryFile
from pathlib import Path
from aukpy import db as auk_db, queries

from tests import (
    SMALL_MOCKED,
    SMALL,
    MEDIUM,
    WITH_ATLAS,
    M_MEDIUM,
    M_SMALL,
    SKIP_NON_MOCKED,
)


def compare_tables(df: pd.DataFrame, original: pd.DataFrame):
    df.sort_values(by="global_unique_identifier", inplace=True)
    df.index = range(len(df))  # type: ignore
    df.fillna("", inplace=True)
    original.sort_values(by="global_unique_identifier", inplace=True)
    original.index = range(len(original))  # type: ignore
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


def run_rebuild(obs_path: Path, incremental: bool = False):
    with NamedTemporaryFile() as output:
        if incremental:
            db = auk_db.build_db_incremental(obs_path, Path(output.name))
        else:
            db = auk_db.build_db_pandas(obs_path, Path(output.name))
        q = queries.no_filter()

        df = auk_db.undo_compression(q.run_pandas(db))
        original = auk_db.read_clean(obs_path)
        compare_tables(df, original)


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_rebuild_small():
    run_rebuild(SMALL)


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_rebuild_medium():
    run_rebuild(MEDIUM)


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_rebuild_atlas():
    run_rebuild(WITH_ATLAS)


def test_rebuild_mocked_small():
    run_rebuild(M_SMALL)


@pytest.mark.skip
def test_rebuild_mocked_medium():
    run_rebuild(M_MEDIUM)


def test_rebuild_mocked_sub():
    for p in SMALL_MOCKED:
        run_rebuild(p)


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_rebuild_incremental():
    run_rebuild(SMALL, incremental=True)


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_rebuild_incremental_medium():
    run_rebuild(MEDIUM, incremental=True)


@pytest.mark.skip
def test_rebuild_incremental_mocked():
    run_rebuild(M_MEDIUM, incremental=True)
