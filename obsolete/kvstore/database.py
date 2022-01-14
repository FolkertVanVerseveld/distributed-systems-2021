import contextlib
import os
import sqlite3
import threading
import typing

DATABASE_DB = os.path.join('kvstore', 'dictionaries.db')
DATABASE_SQL = os.path.join('kvstore', 'dictionaries.sql')
LOCK = threading.Lock()


def create_database():
    with cursor(create=True) as cur, open(DATABASE_SQL, 'r') as f:
        cur.execute(f.read())


@functools.lru_cache()
def connect_database():
    return sqlite3.connect(DATABASE_DB)


@contextlib.contextmanager
def cursor(create=False):
    if not create and not os.path.isfile(DATABASE_DB):
        create_database()
    with LOCK:
        database = connect_database()
        cursor = database.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            database.commit()
            database.close()


def get_item(key: str):
    with cursor() as cur:
        cur.execute(
            'SELECT is_multi,part_of_multi,length '
            'FROM objects '
            'WHERE key=? '
            'LIMIT 1',
            (key,)
        )
        is_multi, part_of_multi, length = cur.fetchone()
    if not part_of_multi:
        return key, is_multi, part_of_multi, length, 0
    with cursor() as cur:
        cur.execute(
            'SELECT multi_key,offset '
            'FROM partial_objects '
            'WHERE key=? '
            'LIMIT 1',
            (key,)
        )
        multi_key, offset = cur.fetchone()
    is_multi, part_of_multi, multi_length = get_item(multi_key)
    assert is_multi == 1, "Expected a multi object."
    assert part_of_multi == 0, "Did not expect partial object."
    return multi_key, 1, 0, length, offset


def store_single_item(item: 'KVItem', is_multi: bool = False,
                      part_of_multi: bool = False):
    with cursor() as cur:
        cur.execute(
            'INSERT INTO objects(key, is_multi, part_of_multi, length) '
            'VALUES (?, ?, ?, ?, ?)',
            (item.key, 1 if is_multi else 0, 1 if part_of_multi else 0, len(item))
        )


def store_multi_item(item: 'MultiKVItem'):
    store_single_item(item, is_multi=True)
    with cursor() as cur:
        for key, offset, length in item:
            cur.execute(
                'INSERT INTO partial_object(key, multi_key, offset) '
                'VALUES (?, ?, ?)',
                (key, item.key, offset)
            )
            cur.execute(
                'INSERT INTO objects(key, is_multi, part_of_multi, length) '
                'VALUES (?, ?, ?, ?, ?)',
                (key, 0, 1, length)
            )

