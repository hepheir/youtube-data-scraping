import sqlite3
import pandas as pd
from datetime import date
from pathlib import Path
from typing import Iterable, Tuple, Type
from youtube.models import Table, Comment, CommentThread, Video, Quota

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'


class DatabaseConnection:
    _conn: sqlite3.Connection

    def __init__(self, db_path=DATA_DIR / 'sqlite.db') -> None:
        self._db_path = db_path
        self._conn = self._get_connection()
        self._create_table(Quota)
        self._create_table(Video)
        self._create_table(Comment)
        self._create_table(CommentThread)
        self._conn.close()

    def __enter__(self):
        self._conn = self._get_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.close()

    def _get_connection(self):
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _create_table(self, table: Type[Table]):
        cursor = self._conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table.table_name()} (
                {', '.join([f'{col} {type_}' for col, type_ in table.table_scheme().items()])}
            )
        ''')
        self._conn.commit()

    def _save(self, row: Table):
        data = row.serialize()
        cursor = self._conn.cursor()
        cursor.execute(f'''
            INSERT OR REPLACE INTO {row.table_name()} ({
                ', '.join(data.keys())
            }) VALUES ({
                ', '.join(['?' for _ in data])
            })
        ''', tuple(data.values()))
        self._conn.commit()

    def get_quota(self, date_: date = date.today(), default: int = 10000) -> int:
        cursor = self._conn.cursor()
        cursor.execute('''
            SELECT value FROM quota WHERE date = ?
        ''', (date_.isoformat(),))
        if row := cursor.fetchone():
            return row['value']
        return default

    def set_quota(self, date_: date, quota: int):
        cursor = self._conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO quota (date, value) VALUES (?, ?)
        ''', (date_.isoformat(), quota))
        self._conn.commit()

    def save(self, obj: Table):
        if not isinstance(obj, Table):
            raise ValueError(f'Unsupported object type: {type(obj)}')
        self._save(obj)

    def query_to_dataframe(self, sql: str, params: Iterable = tuple()) -> pd.DataFrame:
        return self._query_to_dataframe(sql=sql, params=tuple(params))

    def _query_to_dataframe(self, sql: str, params: Tuple) -> pd.DataFrame:
        return pd.read_sql_query(sql=sql, con=self._conn, params=tuple(params))
