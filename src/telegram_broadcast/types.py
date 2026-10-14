from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from math import isfinite
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from aiogram.enums import ContentType


class BroadcastStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in (self.COMPLETED, self.CANCELLED, self.FAILED)


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    SENDING = "sending"
    SUCCESS = "success"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class BroadcastMedia:
    content_type: ContentType
    file_id: str | None = None
    url: str | None = None
    path_to_file: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.content_type, ContentType):
            raise TypeError("content_type must be an aiogram ContentType")
        supported = {
            ContentType.PHOTO,
            ContentType.VIDEO,
            ContentType.DOCUMENT,
            ContentType.AUDIO,
            ContentType.ANIMATION,
            ContentType.VOICE,
            ContentType.VIDEO_NOTE,
            ContentType.STICKER,
        }
        if self.content_type not in supported:
            raise ValueError(f"Unsupported media content type: {self.content_type}")
        sources = (self.file_id, self.url, self.path_to_file)
        if sum(source is not None for source in sources) != 1:
            raise ValueError("Provide exactly one of file_id, url or path_to_file")
        if self.file_id is not None and not self.file_id.strip():
            raise ValueError("file_id cannot be empty")
        if self.url is not None:
            url = urlsplit(self.url)
            if url.scheme not in ("http", "https") or not url.hostname:
                raise ValueError("url must be an HTTP(S) URL accessible to Telegram")
        if self.path_to_file is not None and not self.path_to_file.strip():
            raise ValueError("path_to_file cannot be empty")


@dataclass(frozen=True)
class BroadcastMessage:
    text: str | None = None
    media: BroadcastMedia | None = None
    parse_mode: Literal["HTML", "MarkdownV2"] | None = "HTML"

    def __post_init__(self) -> None:
        if not (self.text and self.text.strip()) and self.media is None:
            raise ValueError("Provide nonempty text or media")
        if self.parse_mode not in ("HTML", "MarkdownV2", None):
            raise ValueError("Unsupported parse_mode")
        limit = 1024 if self.media is not None else 4096
        if self.text and len(self.text) > limit:
            raise ValueError(f"Source text exceeds {limit} characters")
        if (
            self.media is not None
            and self.media.content_type
            in (
                ContentType.STICKER,
                ContentType.VIDEO_NOTE,
            )
            and self.text
        ):
            raise ValueError("This media type does not support a caption")


@dataclass(frozen=True)
class SentMessage:
    message_id: int
    media_file_id: str | None = None


@dataclass(frozen=True)
class NotificationRecipient:
    chat_id: int
    thread_id: int | None = None

    def __post_init__(self) -> None:
        for name, value in (("chat_id", self.chat_id), ("thread_id", self.thread_id)):
            if value is not None and (
                type(value) is not int or value == 0 or not -(2**63) <= value < 2**63
            ):
                raise ValueError(f"{name} must be a nonzero signed 64-bit integer")
        if self.thread_id is not None and self.thread_id < 0:
            raise ValueError("thread_id must be positive")


@dataclass(frozen=True)
class NotificationResult:
    chat_id: int
    thread_id: int | None
    message_id: int | None
    error: str | None
    updated_at: datetime | None


@dataclass(frozen=True)
class BroadcastResult:
    id: UUID
    bot_id: int
    title: str
    status: BroadcastStatus
    total: int
    success: int
    failed: int
    unknown: int
    error: str | None
    next_retry_at: datetime | None
    created_at: datetime
    updated_at: datetime
    notifications: tuple[NotificationResult, ...] = ()
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def processed(self) -> int:
        return self.success + self.failed + self.unknown

    @property
    def remaining(self) -> int:
        return self.total - self.processed


@dataclass(frozen=True)
class DeliveryResult:
    chat_id: int
    status: DeliveryStatus
    message_id: int | None
    error: str | None
    updated_at: datetime


@dataclass(frozen=True)
class RunPolicy:
    send_interval: float = 0.05
    report_interval: float = 5
    network_retry_delay: float = 5
    control_poll_interval: float = 0.5

    def __post_init__(self) -> None:
        if not all(
            isfinite(value)
            for value in (
                self.send_interval,
                self.report_interval,
                self.network_retry_delay,
                self.control_poll_interval,
            )
        ):
            raise ValueError("Intervals must be finite")
        if self.send_interval < 0 or self.report_interval < 0:
            raise ValueError("Intervals cannot be negative")
        if self.network_retry_delay < 0 or self.control_poll_interval <= 0:
            raise ValueError("Invalid retry or polling interval")
