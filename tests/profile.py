import sqlite3

from pathlib import Path
from sys import argv
from time import time

from aukpy import db

from tests.db_test import SMALL, MEDIUM, LARGE


def disk_stats(csv_file: Path, db_file: Path, conn: sqlite3.Connection):
    """Calculate disk usage stats"""
    csv_usage = csv_file.stat().st_size
    sqlite_usage = db_file.stat().st_size

    data = {
        'csv_size': csv_usage,
        'sql_size': sqlite_usage,
        'compression': sqlite_usage / csv_usage
    }
    for wrapper in db.WRAPPERS:
        q = f'SELECT SUM("pgsize") FROM "dbstat" WHERE name=\'{wrapper.table_name}\';'
        cursor = conn.execute(q)
        res = cursor.fetchall()[0][0]
        data[wrapper.table_name] = res
    q = f'SELECT SUM("pgsize") FROM "dbstat" WHERE name=\'observation\';'
    cursor = conn.execute(q)
    res = cursor.fetchall()[0][0]
    data['observation'] = res
    return data


def stats(csv_file: Path, incremental: bool=False):
    """Run a build and get basic stats.
    No detailed profiling is performed.
    """
    db_file = csv_file.with_suffix(".sqlite")
    if db_file.is_file():
        db_file.unlink()
    start = time()
    if incremental:
        conn = db.build_db_pandas_incremental(csv_file, db_file)
    else:
        conn = db.build_db_pandas(csv_file, db_file)
    end = time()
    disk = disk_stats(csv_file, db_file, conn)
    return {
        'build_time': end-start,
        'data_stats': disk
    }


if __name__ == "__main__":
    if len(argv) <= 1:
        print(stats(SMALL))
        print(stats(MEDIUM))
    elif argv == 'large':
        print(stats(LARGE))
    else:
        print(stats(Path(argv[1])))