import sqlite3
from pathlib import Path

import pytest
from aukpy import db

from tests.db_test import SMALL, MEDIUM, SKIP_NON_MOCKED


def check_usage(csv_file: Path):
    db_file = csv_file.with_suffix(".sqlite")
    if db_file.is_file():
        db_file.unlink()
    conn = db.build_db_pandas(csv_file, db_file)
    csv_usage = csv_file.stat().st_size
    sqlite_usage = db_file.stat().st_size

    print(f"Size ratio: {sqlite_usage / csv_usage}")

    for wrapper in db.WRAPPERS:
        q = f'SELECT SUM("pgsize") FROM "dbstat" WHERE name=\'{wrapper.table_name}\';'
        cursor = conn.execute(q)
        res = cursor.fetchall()[0][0]
        print(f"Size of {wrapper.table_name}: {res}")
    q = f'SELECT SUM("pgsize") FROM "dbstat" WHERE name=\'observation\';'
    cursor = conn.execute(q)
    res = cursor.fetchall()[0][0]
    print(f"Size of observation: {res}")


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_data_usage():
    for csv_file in (
        SMALL,
        MEDIUM,
    ):
        check_usage(csv_file)
