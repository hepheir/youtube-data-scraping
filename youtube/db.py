import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union

import pandas as pd

from youtube.models import Comment, Video

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'


def serialize_comment(comment: Comment) -> Dict[str, Union[str, int, None]]:
    return {
        'id': comment.id,
        'channelId': comment.snippet.channelId,
        'videoId': comment.snippet.videoId,
        'textDisplay': comment.snippet.textDisplay,
        'textOriginal': comment.snippet.textOriginal,
        'parentId': comment.snippet.parentId,
        'authorDisplayName': comment.snippet.authorDisplayName,
        'authorProfileImageUrl': comment.snippet.authorProfileImageUrl,
        'authorChannelUrl': comment.snippet.authorChannelUrl,
        'authorChannelId': json.dumps(comment.snippet.authorChannelId, ensure_ascii=False),
        'canRate': comment.snippet.canRate,
        'viewerRating': comment.snippet.viewerRating,
        'likeCount': comment.snippet.likeCount,
        'publishedAt': comment.snippet.publishedAt,
        'updatedAt': comment.snippet.updatedAt,
    }


def deserialize_comment(row: Dict) -> Comment:
    return Comment.from_dict({
        "kind": "youtube#comment",
        "etag": None,
        "id": row['id'],
        "snippet": {
            "channelId": row['channelId'],
            "videoId": row['videoId'],
            "textDisplay": row['textDisplay'],
            "textOriginal": row['textOriginal'],
            "parentId": row['parentId'],
            "authorDisplayName": row['authorDisplayName'],
            "authorProfileImageUrl": row['authorProfileImageUrl'],
            "authorChannelUrl": row['authorChannelUrl'],
            "authorChannelId": json.loads(row['authorChannelId']),
            "canRate": bool(row['canRate']),
            "viewerRating": row['viewerRating'],
            "likeCount": row['likeCount'],
            "publishedAt": row['publishedAt'],
            "updatedAt": row['updatedAt'],
        },
    })


def serialize_video(video: Video) -> Dict[str, Union[str, int, None]]:
    return {
        'id': video.id,
        'publishedAt': video.snippet.publishedAt,
        'channelId': video.snippet.channelId,
        'title': video.snippet.title,
        'description': video.snippet.description,
        'thumbnails': json.dumps(video.snippet.thumbnails, ensure_ascii=False),
        'channelTitle': video.snippet.channelTitle,
        'tags': json.dumps(video.snippet.tags, ensure_ascii=False),
        'categoryId': video.snippet.categoryId,
        'liveBroadcastContent': video.snippet.liveBroadcastContent,
        'defaultLanguage': video.snippet.defaultLanguage,
        'localized': json.dumps(video.snippet.localized, ensure_ascii=False),
        'defaultAudioLanguage': video.snippet.defaultAudioLanguage,
    }


def deserialize_video(row: Dict) -> Video:
    return Video.from_dict({
        "kind": "youtube#video",
        "etag": None,
        "id": row['id'],
        "snippet": {
            "publishedAt": row['publishedAt'],
            "channelId": row['channelId'],
            "title": row['title'],
            "description": row['description'],
            "thumbnails": json.loads(row['thumbnails']),
            "channelTitle": row['channelTitle'],
            "tags": json.loads(row['tags']),
            "categoryId": row['categoryId'],
            "liveBroadcastContent": row['liveBroadcastContent'],
            "defaultLanguage": row['defaultLanguage'],
            "localized": json.loads(row['localized']),
            "defaultAudioLanguage": row['defaultAudioLanguage'],
        }
    })


TABLE_SCHEME_VIDEO = {
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

TABLE_SCHEME_COMMENT = {
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


class DatabaseConnection:
    _conn: sqlite3.Connection

    def __init__(self, db_path=DATA_DIR / 'sqlite.db',
                 comments_table_name='comments',
                 videos_table_name='videos') -> None:
        self._db_path = db_path
        self._comments_table_name = comments_table_name
        self._videos_table_name = videos_table_name
        self._conn = self._get_connection()
        self._create_quota_table()
        self._create_videos_table()
        self._create_comments_table()
        self._conn.close()

    def __enter__(self):
        self._conn = self._get_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.close()

    def _create_quota_table(self):
        # 날짜별 남은 quota 수를 저장함.
        cursor = self._conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quota (
                date TEXT PRIMARY KEY,
                value INTEGER
            )
        ''')
        self._conn.commit()

    def _create_videos_table(self):
        cursor = self._conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self._videos_table_name} (
                {', '.join([f'{col} {type_}' for col, type_ in TABLE_SCHEME_VIDEO.items()])}
            )
        ''')
        self._conn.commit()

    def _create_comments_table(self):
        cursor = self._conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self._comments_table_name} (
                {', '.join([f'{col} {type_}' for col, type_ in TABLE_SCHEME_COMMENT.items()])}
            )
        ''')
        self._conn.commit()

    def _get_connection(self):
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @property
    def videos_table_name(self) -> str:
        return self._videos_table_name

    @property
    def comments_table_name(self) -> str:
        return self._comments_table_name

    def get_quota(self, date_: date, default: int = 10000) -> int:
        cursor = self._conn.cursor()
        cursor.execute('''
            SELECT value FROM quota WHERE date = ?
        ''', (date_.isoformat(),))
        row = cursor.fetchone()
        if not row:
            return default
        return row['value']

    def set_quota(self, date_: date, quota: int):
        cursor = self._conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO quota (date, value) VALUES (?, ?)
        ''', (date_.isoformat(), quota))
        self._conn.commit()

    def insert_video(self, video: Video):
        video_data = serialize_video(video)
        cursor = self._conn.cursor()
        cursor.execute(f'''
            INSERT OR REPLACE INTO {self._videos_table_name} ({
                ', '.join(TABLE_SCHEME_VIDEO.keys())
            }) VALUES ({
                ', '.join(['?' for _ in TABLE_SCHEME_VIDEO])
            })
        ''', tuple(video_data[col] for col in TABLE_SCHEME_VIDEO.keys()))
        self._conn.commit()

    def insert_comment(self, comment: Comment):
        comment_data = serialize_comment(comment)
        cursor = self._conn.cursor()
        cursor.execute(f'''
            INSERT OR REPLACE INTO {self._comments_table_name} ({
                ', '.join(TABLE_SCHEME_COMMENT.keys())
            }) VALUES ({
                ', '.join(['?' for _ in TABLE_SCHEME_COMMENT])
            })
        ''', tuple(comment_data[col] for col in TABLE_SCHEME_COMMENT.keys()))
        self._conn.commit()

    def has_video(self, video_id: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(f'''
            SELECT COUNT(*) FROM {self._videos_table_name} WHERE id = ?
        ''', (
            video_id,
        ))
        return cursor.fetchone()[0] > 0

    def has_comment(self, comment_id: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(f'''
            SELECT COUNT(*) FROM {self._comments_table_name} WHERE id = ?
        ''', (
            comment_id,
        ))
        return cursor.fetchone()[0] > 0

    def get_comment(self, comment_id: str) -> Optional[Comment]:
        cursor = self._conn.cursor()
        cursor.execute(f'''
            SELECT * FROM {self._comments_table_name} WHERE id = ?
        ''', (comment_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return deserialize_comment(dict(row))

    def get_video(self, video_id: str) -> Optional[Video]:
        cursor = self._conn.cursor()
        cursor.execute(f'''
            SELECT * FROM {self._videos_table_name} WHERE id = ?
        ''', (video_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return deserialize_video(dict(row))

    def list_videos(self) -> List[Video]:
        cursor = self._conn.cursor()
        cursor.execute(f'''
            SELECT * FROM {self._videos_table_name}
        ''')
        return [deserialize_video(dict(row)) for row in cursor.fetchall()]

    def list_comments(self) -> List[Comment]:
        cursor = self._conn.cursor()
        cursor.execute(f'''
            SELECT * FROM {self._comments_table_name}
        ''')
        return [deserialize_comment(dict(row)) for row in cursor.fetchall()]

    def list_video_comments(self, video_id: str) -> List[Comment]:
        cursor = self._conn.cursor()
        cursor.execute(f'''
            SELECT * FROM {self._comments_table_name} WHERE videoId = ?
        ''', (video_id,))
        return [deserialize_comment(dict(row)) for row in cursor.fetchall()]

    def delete_video(self, video_id: str):
        cursor = self._conn.cursor()
        cursor.execute(f'''
            DELETE FROM {self._videos_table_name} WHERE id = ?
        ''', (video_id,))
        self._conn.commit()

    def delete_video_comments(self, video_id: str):
        cursor = self._conn.cursor()
        cursor.execute(f'''
            DELETE FROM {self._comments_table_name} WHERE videoId = ?
        ''', (video_id,))
        self._conn.commit()

    def query_to_dataframe(self, sql: str) -> pd.DataFrame:
        return self._query_to_dataframe(sql)

    def videos_to_dataframe(self) -> pd.DataFrame:
        return self._query_to_dataframe(f'SELECT * FROM {self._videos_table_name}')

    def comments_to_dataframe(self, video_id: Optional[str] = None) -> pd.DataFrame:
        if not video_id:
            return self._query_to_dataframe(f'SELECT * FROM {self._comments_table_name}')
        # 개인용 / READ ONLY 여서 SQLInjection 을 조심할 필요는 없겠으나... 추후 이 기능을 확장하려면 조심할 것.
        return self._query_to_dataframe(f'SELECT * FROM {self._comments_table_name} WHERE videoId = ?', (video_id, ))

    def _query_to_dataframe(self, sql: str, params: Iterable = tuple()) -> pd.DataFrame:
        return pd.read_sql_query(sql=sql, con=self._conn, params=tuple(params))
