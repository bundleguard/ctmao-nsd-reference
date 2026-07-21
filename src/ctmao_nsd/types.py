"""Immutable messages and result types that cross execution boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from time import time
from typing import TypeAlias
from uuid import UUID, uuid4

Scalar: TypeAlias = str | int | float | bool | None


class ResultStatus(str, Enum):
    """Terminal state of an agent task."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    REJECTED = "rejected"


class CommandKind(str, Enum):
    """Commands accepted by a worker thread."""

    RUN = "run"
    STOP = "stop"


class EnvelopeKind(str, Enum):
    """Messages emitted from a worker to the orchestrator."""

    RESULT = "result"
    WORKER_FAILED = "worker_failed"
    WORKER_STOPPED = "worker_stopped"


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """Declarative, immutable work tree routed to a supervisor."""

    name: str
    value: Scalar = None
    children: tuple["TaskSpec", ...] = ()
    duration: float = 0.0
    should_fail: bool = False
    task_id: UUID = field(default_factory=uuid4)

    def __post_init__(self) -> None:
        """Validate input at the boundary before it reaches a worker."""
        if not self.name.strip():
            raise ValueError("task name must not be blank")
        if self.duration < 0:
            raise ValueError("task duration must not be negative")


@dataclass(frozen=True, slots=True)
class TaskResult:
    """Serializable outcome returned by a supervisor or child agent."""

    task_id: UUID
    name: str
    status: ResultStatus
    worker_id: str
    agent_path: tuple[str, ...]
    output: str | None = None
    error: str | None = None
    children: tuple["TaskResult", ...] = ()

    @property
    def succeeded(self) -> bool:
        """Return whether this node and its processing completed successfully."""
        return self.status is ResultStatus.SUCCEEDED


@dataclass(frozen=True, slots=True)
class SyncToken:
    """Single-use capability correlating a worker snapshot with an assignment."""

    worker_id: str
    token_id: UUID = field(default_factory=uuid4)
    issued_at: float = field(default_factory=time)


@dataclass(frozen=True, slots=True)
class MemorySnapshot:
    """Immutable, allowlisted projection of one worker's private memory."""

    worker_id: str
    revision: int
    entries: tuple[tuple[str, Scalar], ...]


@dataclass(frozen=True, slots=True)
class WorkerCommand:
    """Immutable orchestrator-to-worker message."""

    kind: CommandKind
    correlation_id: UUID = field(default_factory=uuid4)
    task: TaskSpec | None = None
    sync_token: SyncToken | None = None


@dataclass(frozen=True, slots=True)
class WorkerEnvelope:
    """Immutable worker-to-orchestrator message."""

    kind: EnvelopeKind
    worker_id: str
    correlation_id: UUID
    result: TaskResult | None = None
    snapshot: MemorySnapshot | None = None
    sync_token: SyncToken | None = None
    error: str | None = None
    emitted_at: float = field(default_factory=time)
