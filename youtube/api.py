from typing import Generator, Optional

from youtube.models import *


class YoutubeAPI:
    def __init__(self, api_key: str, quota: int = 10000):
        # Do lazy-loading
        from googleapiclient.discovery import build
        self.client = build('youtube', 'v3', developerKey=api_key)
        self.quota = quota

    def get_video(self, video_id: str) -> Video:
        """영상 ID를 통해 영상에 대한 기본적인 정보를 가져온다."""
        request = self.client.videos().list(
            part="snippet",
            id=video_id,
        )
        self.quota -= 1
        response = VideoListResponse.from_dict(request.execute())
        return response.items[0]

    def get_threads(self, video_id: str) -> Generator[CommentThread, None, None]:
        page_token = None
        while True:
            request = self.client.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                pageToken=page_token,
                maxResults=100,
            )
            self.quota -= 1
            response = CommentThreadListResponse.from_dict(request.execute())
            yield from response.items
            if not (page_token := response.nextPageToken):
                break

    def get_comments(self, thread_id: str) -> Generator[Comment, None, None]:
        page_token = None
        while True:
            request = self.client.comments().list(
                part="id,snippet",
                parentId=thread_id,
                pageToken=page_token,
                maxResults=100,
            )
            self.quota -= 1
            response = CommentListResponse.from_dict(request.execute())
            yield from response.items
            if not (page_token := response.nextPageToken):
                break
