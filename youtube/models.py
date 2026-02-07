from dataclasses import dataclass
from typing import Dict, List, Optional


__all__ = [
    'Resource',
    'PageInfo',
    'VideoSnippet',
    'Video',
    'VideoListResponse',
    'CommentSnippet',
    'Comment',
    'CommentThreadSnippet',
    'CommentThreadReplies',
    'CommentThread',
    'CommentThreadListResponse',
]


@dataclass
class Resource:
    kind: str
    etag: str

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        return cls(**data)


@dataclass
class PageInfo:
    totalResults: int
    resultsPerPage: int

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        return cls(**data)


@dataclass
class VideoSnippet:
    publishedAt: str
    channelId: str
    title: str
    description: str
    thumbnails: Dict[str, Dict]
    channelTitle: str
    tags: Optional[List[str]]
    categoryId: str
    liveBroadcastContent: str
    defaultLanguage: str
    localized: Dict[str, str]
    defaultAudioLanguage: str

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['tags'] = data.get('tags')
        return cls(**data)


@dataclass
class Video(Resource):
    id: str
    snippet: VideoSnippet

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['snippet'] = VideoSnippet.from_dict(data['snippet'])
        return cls(**data)


@dataclass
class VideoListResponse(Resource):
    items: List[Video]
    pageInfo: PageInfo

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['items'] = list(map(Video.from_dict, data['items']))
        data['pageInfo'] = PageInfo(**data['pageInfo'])
        return cls(**data)


@dataclass
class CommentSnippet:
    channelId: str
    videoId: str
    textDisplay: str
    textOriginal: str
    parentId: Optional[str]
    authorDisplayName: str
    authorProfileImageUrl: str
    authorChannelUrl: str
    authorChannelId: Dict[str, str]
    canRate: bool
    viewerRating: str
    likeCount: int
    publishedAt: str
    updatedAt: str

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['parentId'] = data.get('parentId')
        return cls(**data)


@dataclass
class Comment(Resource):
    id: str
    snippet: CommentSnippet

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['snippet'] = CommentSnippet.from_dict(data['snippet'])
        return cls(**data)


@dataclass
class CommentThreadSnippet:
    channelId: str
    videoId: str
    topLevelComment: Comment
    canReply: bool
    totalReplyCount: int
    isPublic: bool

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['topLevelComment'] = Comment.from_dict(data['topLevelComment'])
        return cls(**data)


@dataclass
class CommentThreadReplies:
    comments: List[Comment]

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['comments'] = list(map(Comment.from_dict, data['comments']))
        return cls(**data)


@dataclass
class CommentThread(Resource):
    id: str
    snippet: CommentThreadSnippet
    replies: Optional[CommentThreadReplies]

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['snippet'] = CommentThreadSnippet.from_dict(data['snippet'])
        data['replies'] = data.get('replies')
        if data['replies'] is not None:
            data['replies'] = CommentThreadReplies.from_dict(data['replies'])
        return cls(**data)


@dataclass
class CommentThreadListResponse(Resource):
    items: List[CommentThread]
    pageInfo: PageInfo
    nextPageToken: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict):
        data = data.copy()
        data['items'] = list(map(CommentThread.from_dict, data['items']))
        data['pageInfo'] = PageInfo.from_dict(data['pageInfo'])
        data['nextPageToken'] = data.get('nextPageToken')
        return cls(**data)
