"""Report measurements: durable wall-clock duration and per-update throughput."""

from dataclasses import dataclass
from datetime import datetime

from .types import BroadcastResult


@dataclass(frozen=True)
class ProgressMetrics:
    elapsed: float
    average_speed: float
    current_speed: float
    remaining_seconds: float | None


class ProgressTracker:
    def __init__(self) -> None:
        self._previous: tuple[int, float] | None = None

    def sample(
        self, result: BroadcastResult, *, now: datetime, monotonic_time: float
    ) -> ProgressMetrics:
        end = result.finished_at or now
        elapsed = (
            max(0.0, (end - result.started_at).total_seconds())
            if result.started_at is not None
            else 0.0
        )
        average = result.processed / elapsed if elapsed > 0 else 0.0
        current = 0.0
        if self._previous is not None:
            previous_count, previous_time = self._previous
            interval = monotonic_time - previous_time
            if interval > 0:
                current = max(0, result.processed - previous_count) / interval
        self._previous = (result.processed, monotonic_time)
        remaining = result.remaining / average if average > 0 else None
        if result.remaining == 0:
            remaining = 0.0
        return ProgressMetrics(elapsed, average, current, remaining)
