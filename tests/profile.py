import sqlite3

from pathlib import Path
from sys import argv
from time import time

from aukpy import db

from tests.db_test import SMALL, MEDIUM, LARGE


def analyze_table(table_name: str, conn):
    q = f'SELECT SUM("pgsize") FROM "dbstat" WHERE name=\'{table_name}\';'
    cursor = conn.execute(q)
    size = cursor.fetchall()[0][0]
    count_q = f"SELECT COALESCE(MAX(id) + 1, 0) from {table_name};"
    cursor = conn.execute(count_q)
    rows = cursor.fetchall()[0][0]
    return {"size": size, "rows": rows, "row_size": size / rows}


def disk_stats(csv_file: Path, db_file: Path, conn: sqlite3.Connection):
    """Calculate disk usage stats"""
    csv_usage = csv_file.stat().st_size
    sqlite_usage = db_file.stat().st_size

    data = {
        "csv_size": csv_usage,
        "sql_size": sqlite_usage,
        "compression": sqlite_usage / csv_usage,
        "tables": {},
    }
    for wrapper in db.WRAPPERS:
        data["tables"][wrapper.table_name] = analyze_table(wrapper.table_name, conn)
    data["tables"]["observation"] = analyze_table("observation", conn)
    return data


def print_stats(stats: dict):
    print(f'Build time: {stats["build_time"]}')
    print(f'\tCSV size: {stats["data_stats"]["csv_size"]}')
    print(f'\tSQL size: {stats["data_stats"]["sql_size"]}')
    print(f'\tRatio:    {stats["data_stats"]["compression"]}')

    for name, table_stats in stats["data_stats"]["tables"].items():
        print(f"Stats for {name}:")
        print(f'\tSize: {table_stats["size"]}')
        print(f'\tNumber of rows: {table_stats["rows"]}')
        print(f'\tAverage row size: {table_stats["row_size"]}')


def stats(csv_file: Path, incremental: bool = False):
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
    return {"build_time": end - start, "data_stats": disk}


if __name__ == "__main__":
    if len(argv) <= 1:
        print_stats(stats(SMALL))
        print_stats(stats(MEDIUM))
    elif argv[1] == "large":
        print_stats(stats(LARGE))
    else:
        print_stats(stats(Path(argv[1])))
