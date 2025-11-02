from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import uuid
from pydantic import BaseModel, Field, ValidationError

class EventType(str, Enum): # Defining list of Enums for the various defined event types
    USER_VIEW = "user_view"
    USER_CLICK = "user_click"
    USER_UPVOTE = "user_upvote"
    USER_DOWNVOTE = "user_downvote"
    USER_COMMENT = "user_comment"
    POST_CREATED = "post_created"
    POST_EDITED = "post_edited"
    POST_DELETED = "post_deleted"

class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class UserEvent(BaseEvent):
    user_id: str
    post_id: str
    subreddit: str
    session_id: str
    device_type: str # mobile, desktop, tablet
    duration_seconds: Optional[float] = None # for view events

class ContentEvent(BaseEvent):
    post_id: str
    author_id: str
    subreddit: str
    title: str
    content_type: str # text, link, image, video
    word_count: Optional[int] = None
