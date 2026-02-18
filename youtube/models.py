import abc
import json
from dataclasses import Field, dataclass
from typing import Any, Dict, List, Optional, Union, get_args, get_origin


__all__ = [
    'Table',
    'Deserializable',
    'Quota',
    'Resource',
    'PageInfo',
    'VideoSnippet',
    'Video',
    'VideoListResponse',
    'CommentSnippet',
    'Comment',
    'CommentListResponse',
    'CommentThreadSnippet',
    'CommentThreadReplies',
    'CommentThread',
    'CommentThreadListResponse',
]


class Table(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def table_name(cls) -> str:
        ...

    @classmethod
    @abc.abstractmethod
    def table_scheme(cls) -> Dict[str, str]:
        ...

    @abc.abstractmethod
    def serialize(self) -> Dict[str, Any]:
        ...


@dataclass
class Quota(Table):
    date: str
    value: int

    @classmethod
    def table_name(cls) -> str:
        return 'quota'

    @classmethod
    def table_scheme(cls) -> Dict[str, str]:
        return {
            'date': 'TEXT PRIMARY KEY',
            'value': 'INTEGER',
        }

    def serialize(self) -> Dict[str, Any]:
        return {
            'date': self.date,
            'value': self.value,
        }


class Deserializable:
    @classmethod
    def from_dict(cls, data: Dict, strict: bool = False):
        fields: Dict[str, Field] = cls.__dataclass_fields__  # type: ignore
        data = data.copy()
        validated_data = {}
        for field in fields.values():
            value = data.pop(field.name, None)
            validated_data[field.name] = cls._validate(field.name,
                                                       value,
                                                       field.type)
        if data and strict:
            raise ValueError(
                f"Unknown fields in data: {', '.join(data.keys())}"
            )
        return cls(**validated_data)

    @classmethod
    def _validate(cls, attr: str, value: Any, annot: Any) -> Any:
        if cls._is_optional(annot):
            return cls._validate_optional(attr, value, annot)
        if cls._is_list(annot):
            return cls._validate_list(attr, value, annot)
        if value is None:
            raise ValueError(
                f"Required attribute '{attr}' is None" +
                f" (type: {annot}" if not cls._is_optional(annot) else ''
            )
        if cls._is_deserializable(annot):
            value = annot.from_dict(value)
        return value

    @classmethod
    def _validate_optional(cls, attr: str, value: Any, annot: Any) -> Any:
        if value is None:
            return None
        return cls._validate(attr, value, get_args(annot)[0])

    @classmethod
    def _validate_list(cls, attr: str, value: Any, annot: Any) -> List[Any]:
        if not isinstance(value, list):
            raise ValueError(
                f"Expected a list for attribute '{attr}', got {type(value)}")
        item_type = get_args(annot)[0]
        return [cls._validate(f"{attr}[]", item, item_type) for item in value]

    @staticmethod
    def _is_list(annot: Any) -> bool:
        return get_origin(annot) is list or get_origin(annot) is List

    @staticmethod
    def _is_optional(annot: Any) -> bool:
        if get_origin(annot) is Union:
            # Check if NoneType is one of the arguments in the Union
            return type(None) in get_args(annot)
        return False

    @staticmethod
    def _is_deserializable(annot: Any) -> bool:
        return isinstance(annot, type) and issubclass(annot, Deserializable)


@dataclass
class Resource(Deserializable):
    kind: str
    etag: str


@dataclass
class PageInfo(Deserializable):
    totalResults: Optional[int]
    resultsPerPage: int


@dataclass
class VideoSnippet(Deserializable):
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


@dataclass
class Video(Table, Resource):
    id: str
    snippet: VideoSnippet

    @classmethod
    def table_name(cls) -> str:
        return 'videos'

    @classmethod
    def table_scheme(cls) -> Dict[str, str]:
        return {
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

    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'publishedAt': self.snippet.publishedAt,
            'channelId': self.snippet.channelId,
            'title': self.snippet.title,
            'description': self.snippet.description,
            'thumbnails': json.dumps(self.snippet.thumbnails, ensure_ascii=False),
            'channelTitle': self.snippet.channelTitle,
            'tags': json.dumps(self.snippet.tags, ensure_ascii=False),
            'categoryId': self.snippet.categoryId,
            'liveBroadcastContent': self.snippet.liveBroadcastContent,
            'defaultLanguage': self.snippet.defaultLanguage,
            'localized': json.dumps(self.snippet.localized, ensure_ascii=False),
            'defaultAudioLanguage': self.snippet.defaultAudioLanguage,
        }


@dataclass
class VideoListResponse(Resource):
    items: List[Video]
    pageInfo: PageInfo


@dataclass
class CommentSnippet(Deserializable):
    channelId: str
    textDisplay: str
    textOriginal: str
    parentId: Optional[str]
    authorDisplayName: str
    authorProfileImageUrl: str
    authorChannelUrl: Optional[str]
    authorChannelId: Optional[Dict[str, str]]
    canRate: bool
    viewerRating: str
    likeCount: int
    moderationStatus: Optional[str]
    publishedAt: str
    updatedAt: str


@dataclass
class Comment(Table, Resource):
    id: str
    snippet: CommentSnippet

    @classmethod
    def table_name(cls) -> str:
        return 'comments'

    @classmethod
    def table_scheme(cls) -> Dict[str, str]:
        return {
            'id': 'TEXT PRIMARY KEY',
            'channelId': 'TEXT',
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
            'moderationStatus': 'TEXT',
            'publishedAt': 'TEXT',
            'updatedAt': 'TEXT',
        }

    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'channelId': self.snippet.channelId,
            'textDisplay': self.snippet.textDisplay,
            'textOriginal': self.snippet.textOriginal,
            'parentId': self.snippet.parentId,
            'authorDisplayName': self.snippet.authorDisplayName,
            'authorProfileImageUrl': self.snippet.authorProfileImageUrl,
            'authorChannelUrl': self.snippet.authorChannelUrl,
            'authorChannelId': self.snippet.authorChannelId['value'] if self.snippet.authorChannelId else None,
            'canRate': self.snippet.canRate,
            'viewerRating': self.snippet.viewerRating,
            'likeCount': self.snippet.likeCount,
            'moderationStatus': self.snippet.moderationStatus,
            'publishedAt': self.snippet.publishedAt,
            'updatedAt': self.snippet.updatedAt,
        }


@dataclass
class CommentListResponse(Resource):
    pageInfo: PageInfo
    items: List[Comment]
    nextPageToken: Optional[str]


@dataclass
class CommentThreadSnippet(Deserializable):
    channelId: str
    videoId: str
    topLevelComment: Comment
    canReply: bool
    totalReplyCount: int
    isPublic: bool


@dataclass
class CommentThreadReplies(Deserializable):
    comments: List[Comment]


@dataclass
class CommentThread(Table, Resource):
    id: str
    snippet: CommentThreadSnippet
    replies: Optional[CommentThreadReplies]

    @classmethod
    def table_name(cls) -> str:
        return 'threads'

    @classmethod
    def table_scheme(cls) -> Dict[str, str]:
        return {
            'id': 'TEXT PRIMARY KEY',
            'channelId': 'TEXT',
            'videoId': 'TEXT',
            'topLevelCommentId': 'TEXT',
            'canReply': 'INTEGER',
            'totalReplyCount': 'INTEGER',
            'isPublic': 'INTEGER',
        }

    def serialize(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'channelId': self.snippet.channelId,
            'videoId': self.snippet.videoId,
            'topLevelCommentId': self.snippet.topLevelComment.id,
            'canReply': self.snippet.canReply,
            'totalReplyCount': self.snippet.totalReplyCount,
            'isPublic': self.snippet.isPublic,
        }


@dataclass
class CommentThreadListResponse(Resource):
    items: List[CommentThread]
    pageInfo: PageInfo
    nextPageToken: Optional[str]
