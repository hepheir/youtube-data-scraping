from datetime import date
from urllib.parse import urlparse, parse_qs
from pathlib import Path

from youtube.api import YoutubeAPI
from youtube.db import DatabaseConnection


def get_video_id(url: str) -> str:
    """유튜브 url에서 영상 ID 만 추출"""
    parsed = urlparse(url)
    # 1. watch?v=...
    if parsed.query:
        if "v" in (qs := parse_qs(parsed.query)):
            return qs["v"][0]
    # 2. /shorts/VIDEO_ID
    if parsed.path.startswith("/shorts/"):
        return parsed.path.split("/shorts/")[1].split("/")[0]
    # 3. youtu.be/VIDEO_ID
    if parsed.netloc == "youtu.be":
        return parsed.path.lstrip("/")
    raise ValueError("비디오 ID를 찾을 수 없습니다.")


def update_video_data(db_path: Path, video_url: str, api_key: str, verbose: bool = True):
    """영상 URL과 API 키를 받아서 데이터베이스에 영상과 댓글 데이터를 업데이트한다."""

    api = YoutubeAPI(api_key=api_key)
    thread_count = 0
    comment_count = 0

    def print_status(msg: str | None = None):
        if not verbose:
            return
        if msg:
            print(msg, end='')
        else:
            print(f'\rThread {thread_count:5d}'
                  f'\tComment {comment_count:5d}'
                  f'\tQuota {api.quota:5d}',
                  end='  ')

    with DatabaseConnection(db_path) as db:
        api.quota = db.get_quota()
        video_id = get_video_id(video_url)
        video = api.get_video(video_id)
        db.save(video)
        print_status('\n')
        for thread in api.get_threads(video_id):
            db.save(thread)
            db.save(thread.snippet.topLevelComment)
            thread_count += 1
            comment_count += 1
            print_status()
            if thread.snippet.totalReplyCount != 0:
                for comment in api.get_comments(thread.id):
                    db.save(comment)
                    comment_count += 1
                    print_status()
        db.set_quota(date.today(), api.quota)
        print_status('\n')
