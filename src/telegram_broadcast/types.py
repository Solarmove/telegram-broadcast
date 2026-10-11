from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from math import isfinite
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID


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


class MediaType(StrEnum):
    PHOTO = "photo"
    VIDEO = "video"


@dataclass(frozen=True)
class BroadcastMessage:
    text: str | None = None
    media_url: str | None = None
    media_type: MediaType | None = None
    parse_mode: Literal["HTML", "MarkdownV2"] | None = "HTML"

    def __post_init__(self) -> None:
        if not (self.text and self.text.strip()) and not self.media_url:
            raise ValueError("Provide nonempty text or media")
        if bool(self.media_url) != bool(self.media_type):
            raise ValueError("media_url and media_type must be provided together")
        if self.media_type is not None and not isinstance(self.media_type, MediaType):
            raise TypeError("media_type must be a MediaType")
        if self.media_url:
            url = urlsplit(self.media_url)
            if url.scheme not in ("http", "https") or not url.hostname:
                raise ValueError(
                    "media_url must be an HTTP(S) URL accessible to Telegram"
                )
        if self.parse_mode not in ("HTML", "MarkdownV2", None):
            raise ValueError("Unsupported parse_mode")
        limit = 1024 if self.media_url else 4096
        if self.text and len(self.text) > limit:
            raise ValueError(f"Source text exceeds {limit} characters")


@dataclass(frozen=True)
class SentMessage:
    message_id: int
    media_file_id: str | None = None


@dataclass(frozen=True)
class NotificationResult:
    chat_id: int
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
    report_interval: float = 30
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
