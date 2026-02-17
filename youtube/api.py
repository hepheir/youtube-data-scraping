from typing import Generator, Optional

from youtube.models import *


class BaseYoutubeAPI:
    def __init__(self, api_key: str, quota: int = 10000):
        # Do lazy-loading
        from googleapiclient.discovery import build
        self.client = build('youtube', 'v3', developerKey=api_key)
        self.quota = quota

    def get_video(self, video_id: str) -> Video:
        """영상 ID를 통해 영상에 대한 기본적인 정보를 가져온다."""
        return self._get_video_list_response(video_id).items[0]

    def get_comment_threads_by_video(self, video_id: str) -> Generator[CommentThread, None, None]:
        page_token = None
        while True:
            comment_thread_list = self._get_comment_threads_list_response_by_video(
                video_id, page_token)
            yield from comment_thread_list.items
            page_token = comment_thread_list.nextPageToken
            if not page_token:
                break

    def get_comment_threads_by_parent_comment(self, parent_comment_id: str) -> Generator[CommentThread, None, None]:
        page_token = None
        while True:
            comment_thread_list = self._get_comment_threads_list_response_by_parent_comment(
                parent_comment_id, page_token)
            yield from comment_thread_list.items
            page_token = comment_thread_list.nextPageToken
            if not page_token:
                break

    def _get_video_list_response(self, video_id: str) -> VideoListResponse:
        request = self.client.videos().list(
            part="snippet",
            id=video_id,
        )
        self.quota -= 1
        data = request.execute()
        return VideoListResponse.from_dict(data)

    def _get_comment_threads_list_response_by_video(self, video_id: str, page_token: Optional[str]) -> CommentThreadListResponse:
        request = self.client.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            pageToken=page_token,
            maxResults=100,
        )
        self.quota -= 1
        data = request.execute()
        return CommentThreadListResponse.from_dict(data)

    def _get_comment_threads_list_response_by_parent_comment(self, parent_comment_id: str, page_token: Optional[str]) -> CommentThreadListResponse:
        request = self.client.commentThreads().list(
            part="snippet",
            id=parent_comment_id,
            pageToken=page_token,
            maxResults=100,
        )
        self.quota -= 1
        data = request.execute()
        return CommentThreadListResponse.from_dict(data)


class YoutubeAPI(BaseYoutubeAPI):
    def get_comment_threads(self, video_id: str) -> Generator[CommentThread, None, None]:
        """특정 동영상 안에 있는 모든 댓글 스레드를 불러온다."""
        for thread in self.get_comment_threads_by_video(video_id=video_id):
            yield thread
            if thread.snippet.totalReplyCount == 0:
                continue
            for sub_thread in self.get_comment_threads_by_parent_comment(thread.snippet.topLevelComment.id):
                yield sub_thread

    def get_comments(self, video_id: str) -> Generator[Comment, None, None]:
        """특정 동영상 안에 있는 모든 댓글을 불러온다."""
        for thread in self.get_comment_threads_by_video(video_id=video_id):
            yield thread.snippet.topLevelComment
