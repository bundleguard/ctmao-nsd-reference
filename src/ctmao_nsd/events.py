"""Structured events emitted for observability."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from time import time
from uuid import UUID, uuid4


class EventKind(str, Enum):
    """Observable lifecycle transitions."""

    WORKER_STARTED = "worker_started"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    MEMORY_SYNCED = "memory_synced"
    WORKER_STOPPED = "worker_stopped"


@dataclass(frozen=True, slots=True)
class Event:
    """Immutable event suitable for logs or a future event bus."""

    kind: EventKind
    worker_id: str
    message: str
    task_id: UUID | None = None
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: float = field(default_factory=time)
