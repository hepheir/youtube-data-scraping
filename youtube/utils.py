from urllib.parse import urlparse, parse_qs

from youtube.models import *


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
