"""Global orchestration, result collection, and controlled memory sync."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping
from uuid import UUID

from .config import RuntimeConfig
from .events import Event, EventKind
from .memory import SharedMemoryHub
from .thread_manager import ThreadManager
from .types import (
    CommandKind,
    EnvelopeKind,
    SyncToken,
    TaskResult,
    TaskSpec,
    WorkerCommand,
    WorkerEnvelope,
)


class OrchestratorClosedError(RuntimeError):
    """Raised when work is submitted after shutdown."""


@dataclass(frozen=True, slots=True)
class OrchestrationReport:
    """Combined immutable outcome for a multi-worker orchestration run."""

    results: Mapping[str, TaskResult]
    memory_revisions: Mapping[str, int]
    events: tuple[Event, ...]


class Orchestrator:
    """Coordinates isolated worker runtimes without sharing their mutable state."""

    def __init__(
        self,
        worker_ids: tuple[str, ...] = ("A", "B"),
        config: RuntimeConfig | None = None,
    ) -> None:
        """Create an orchestrator; worker resources start lazily on first run."""
        self._worker_ids = worker_ids
        self._config = config or RuntimeConfig()
        self._outbound: asyncio.Queue[WorkerEnvelope] | None = None
        self._threads: ThreadManager | None = None
        self._memory = SharedMemoryHub()
        self._events: list[Event] = []
        self._started = False
        self._closed = False

    async def start(self) -> None:
        """Create cross-loop channels and start all worker runtimes."""
        if self._closed:
            raise OrchestratorClosedError("orchestrator is closed")
        if self._started:
            return
        loop = asyncio.get_running_loop()
        self._outbound = asyncio.Queue()
        self._threads = ThreadManager(
            self._worker_ids, self._config, loop, self._outbound
        )
        self._threads.start_all()
        self._started = True
        self._events.extend(
            Event(EventKind.WORKER_STARTED, worker_id, "worker is ready")
            for worker_id in self._worker_ids
        )

    async def run(self, assignments: Mapping[str, TaskSpec]) -> OrchestrationReport:
        """Run one root task per selected worker and synchronize their snapshots."""
        if self._closed:
            raise OrchestratorClosedError("orchestrator is closed")
        await self.start()
        assert self._threads is not None and self._outbound is not None
        unknown = set(assignments).difference(self._worker_ids)
        if unknown:
            raise KeyError(f"unknown workers: {sorted(unknown)}")

        pending: dict[UUID, str] = {}
        for worker_id, task in assignments.items():
            token = SyncToken(worker_id)
            command = WorkerCommand(
                kind=CommandKind.RUN, task=task, sync_token=token
            )
            pending[command.correlation_id] = worker_id
            self._events.append(
                Event(EventKind.TASK_STARTED, worker_id, "root task routed", task.task_id)
            )
            self._threads.submit(worker_id, command)

        results: dict[str, TaskResult] = {}
        overall_timeout = self._config.task_timeout + 2.0
        while pending:
            envelope = await asyncio.wait_for(
                self._outbound.get(), timeout=overall_timeout
            )
            if envelope.kind is EnvelopeKind.WORKER_FAILED:
                failed_correlation = next(
                    (
                        correlation_id
                        for correlation_id, assigned_worker in pending.items()
                        if assigned_worker == envelope.worker_id
                    ),
                    None,
                )
                if failed_correlation is not None:
                    pending.pop(failed_correlation)
                    raise RuntimeError(
                        f"worker {envelope.worker_id} failed: "
                        f"{envelope.error or 'unknown error'}"
                    )
                continue
            if envelope.correlation_id not in pending:
                continue
            worker_id = pending.pop(envelope.correlation_id)
            if (
                envelope.result is None
                or envelope.snapshot is None
                or envelope.sync_token is None
            ):
                raise RuntimeError("worker returned an incomplete result envelope")
            results[worker_id] = envelope.result
            self._memory.publish(envelope.snapshot, envelope.sync_token)
            event_kind = (
                EventKind.TASK_COMPLETED
                if envelope.result.succeeded
                else EventKind.TASK_FAILED
            )
            self._events.append(
                Event(
                    event_kind,
                    worker_id,
                    envelope.result.status.value,
                    envelope.result.task_id,
                )
            )
            self._events.append(
                Event(EventKind.MEMORY_SYNCED, worker_id, "snapshot accepted")
            )
        return OrchestrationReport(
            results=MappingProxyType(dict(results)),
            memory_revisions=MappingProxyType(self._memory.revisions()),
            events=tuple(self._events),
        )

    def synchronized_memory(self, worker_id: str) -> dict[str, object]:
        """Read the latest orchestrator-approved snapshot for one worker."""
        return dict(self._memory.read_worker(worker_id))

    def worker_identities(self) -> dict[str, int | None]:
        """Expose worker thread identities for diagnostics."""
        return self._threads.identities() if self._threads else {}

    async def close(self) -> None:
        """Cooperatively stop workers; calling close repeatedly is safe."""
        if self._closed:
            return
        if self._threads is not None:
            self._threads.stop_all()
            self._events.extend(
                Event(EventKind.WORKER_STOPPED, worker_id, "worker joined")
                for worker_id in self._worker_ids
            )
        self._closed = True

    async def __aenter__(self) -> "Orchestrator":
        """Start workers for an asynchronous context manager."""
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Always close workers when leaving the context."""
        await self.close()
