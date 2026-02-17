import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, Union

import pandas as pd

from youtube.models import Comment, CommentThread, Video

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'


class TableScheme:
    QUOTA = {
        'date': 'TEXT PRIMARY KEY',
        'value': 'INTEGER',
    }

    VIDEO = {
        'id': 'TEXT PRIMARY KEY',
        'publishedAt': 'TEXT',
        'channelId': 'TEXT',
        'title': 'TEXT',
        'description': 'TEXT',
        'thumbnails': 'TEXT',
        'channelTitle': 'TEXT',
        'tags': 'TEXT',
        'categoryId': 'TEXT',
        'liveBroadcastContent': 'TEXT',
        'defaultLanguage': 'TEXT',
        'localized': 'TEXT',
        'defaultAudioLanguage': 'TEXT',
    }

    COMMENT = {
        'id': 'TEXT PRIMARY KEY',
        'channelId': 'TEXT',
        'videoId': 'TEXT',
        'textDisplay': 'TEXT',
        'textOriginal': 'TEXT',
        'parentId': 'TEXT',
        'authorDisplayName': 'TEXT',
        'authorProfileImageUrl': 'TEXT',
        'authorChannelUrl': 'TEXT',
        'authorChannelId': 'TEXT',
        'canRate': 'INTEGER',
        'viewerRating': 'TEXT',
        'likeCount': 'INTEGER',
        'publishedAt': 'TEXT',
        'updatedAt': 'TEXT',
    }

    THREAD = {
        'id': 'TEXT PRIMARY KEY',
        'channelId': 'TEXT',
        'videoId': 'TEXT',
        'topLevelCommentId': 'TEXT',
        'canReply': 'INTEGER',
        'totalReplyCount': 'INTEGER',
        'isPublic': 'INTEGER',
    }


class DatabaseConnection:
    _conn: sqlite3.Connection

    def __init__(self, db_path=DATA_DIR / 'sqlite.db') -> None:
        self._db_path = db_path
        self._conn = self._get_connection()
        self._create_table('quota', TableScheme.QUOTA)
        self._create_table('videos', TableScheme.VIDEO)
        self._create_table('comments', TableScheme.COMMENT)
        self._create_table('threads', TableScheme.THREAD)
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

    def _create_table(self, table_name: str, scheme: Dict[str, str]):
        cursor = self._conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f'{col} {type_}' for col, type_ in scheme.items()])}
            )
        ''')
        self._conn.commit()

    def _save_data(self, table_name: str, data: Dict[str, Any]):
        cursor = self._conn.cursor()
        cursor.execute(f'''
            INSERT OR REPLACE INTO {table_name} ({
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

    def save(self, obj: Union[Video, Comment, CommentThread]):
        if isinstance(obj, Video):
            table_name = 'videos'
        elif isinstance(obj, Comment):
            table_name = 'comments'
        elif isinstance(obj, CommentThread):
            table_name = 'threads'
        else:
            raise ValueError(f'Unsupported object type: {type(obj)}')
        self._save_data(table_name, obj.flatten())

    def query_to_dataframe(self, sql: str, params: Iterable = tuple()) -> pd.DataFrame:
        return self._query_to_dataframe(sql=sql, params=tuple(params))

    def _query_to_dataframe(self, sql: str, params: Tuple) -> pd.DataFrame:
        return pd.read_sql_query(sql=sql, con=self._conn, params=tuple(params))
