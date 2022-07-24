import sqlite3
from aukpy import db

from tests.db_test import SMALL, MEDIUM


def test_data_usage():
    for csv_file in (SMALL, MEDIUM):
        db_file = csv_file.with_suffix('.sqlite')
        if not db_file.is_file():
            conn = db.build_db_pandas(csv_file, db_file)
        else:
            conn = sqlite3.connect(str(db_file.absolute()))
        print(db_file)
        csv_usage = csv_file.stat().st_size
        sqlite_usage = db_file.stat().st_size

        print(sqlite_usage / csv_usage)

        for wrapper in db.WRAPPERS:
            q = f"SELECT SUM(\"pgsize\") FROM \"dbstat\" WHERE name=\'{wrapper.table_name}\';"
            cursor = conn.execute(q)
            res = cursor.fetchall()[0][0]
            print(f'Size of {wrapper.table_name}: {res}')
        q = f"SELECT SUM(\"pgsize\") FROM \"dbstat\" WHERE name=\'observation\';"
        cursor = conn.execute(q)
        res = cursor.fetchall()[0][0]
        print(f'Size of observation: {res}')

        # Check indices
        q = """SELECT name FROM sqlite_master WHERE type = 'index';"""
        res = conn.execute(q).fetchall()
        print(res)


