"""Global orchestration, result collection, and controlled memory sync."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from time import monotonic
from types import MappingProxyType
from typing import Mapping
from uuid import UUID

from .config import RuntimeConfig
from .events import Event, EventKind
from .memory import SharedMemoryHub
from .thread_manager import ThreadManager, WorkerUnavailableError
from .types import (
    CommandKind,
    EnvelopeKind,
    SyncToken,
    TaskResult,
    TaskSpec,
    WorkerCommand,
    WorkerEnvelope,
)

LOGGER = logging.getLogger(__name__)


class OrchestratorClosedError(RuntimeError):
    """Raised when work is submitted after shutdown."""


class OrchestratorBusyError(RuntimeError):
    """Raised when a second run overlaps an active orchestration run."""


class OrchestrationCancelledError(RuntimeError):
    """Raised when shutdown interrupts an active orchestration run."""


class OrchestrationDeadlineExceeded(TimeoutError):
    """Raised when the single absolute orchestration deadline expires."""


@dataclass(slots=True)
class _PendingRequest:
    worker_id: str
    future: asyncio.Future[WorkerEnvelope]


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
        self._run_active = False
        self._run_idle = asyncio.Event()
        self._run_idle.set()
        self._pending: dict[UUID, _PendingRequest] = {}
        self._failed_workers: dict[str, str] = {}
        self._dispatcher_task: asyncio.Task[None] | None = None

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
        self._dispatcher_task = asyncio.create_task(
            self._dispatch_outbound(), name="ctmao-envelope-dispatcher"
        )
        self._events.extend(
            Event(EventKind.WORKER_STARTED, worker_id, "worker is ready")
            for worker_id in self._worker_ids
        )

    async def run(self, assignments: Mapping[str, TaskSpec]) -> OrchestrationReport:
        """Run one root task per selected worker and synchronize their snapshots."""
        if self._closed:
            raise OrchestratorClosedError("orchestrator is closed")
        assignment_copy = dict(assignments)
        unknown = set(assignment_copy).difference(self._worker_ids)
        if unknown:
            raise KeyError(f"unknown workers: {sorted(unknown)}")
        if self._run_active or self._pending:
            raise OrchestratorBusyError(
                "orchestrator has active or draining work; wait before submitting more"
            )

        self._run_active = True
        self._run_idle.clear()
        try:
            return await self._run_once(assignment_copy)
        finally:
            self._run_active = False
            self._run_idle.set()

    async def _run_once(
        self, assignments: Mapping[str, TaskSpec]
    ) -> OrchestrationReport:
        """Execute one single-flight orchestration run."""
        await self.start()
        assert self._threads is not None and self._outbound is not None

        loop = asyncio.get_running_loop()
        execution_deadline = monotonic() + self._config.task_timeout
        futures: list[asyncio.Future[WorkerEnvelope]] = []
        for worker_id, task in assignments.items():
            if worker_id in self._failed_workers:
                raise WorkerUnavailableError(
                    f"worker {worker_id} failed: {self._failed_workers[worker_id]}"
                )
            token = SyncToken(worker_id)
            command = WorkerCommand(
                kind=CommandKind.RUN,
                task=task,
                sync_token=token,
                deadline=execution_deadline,
            )
            future: asyncio.Future[WorkerEnvelope] = loop.create_future()
            self._pending[command.correlation_id] = _PendingRequest(
                worker_id, future
            )
            futures.append(future)
            self._events.append(
                Event(EventKind.TASK_STARTED, worker_id, "root task routed", task.task_id)
            )
            try:
                self._threads.submit(worker_id, command)
            except Exception:
                self._pending.pop(command.correlation_id, None)
                future.cancel()
                raise

        collection_grace = max(0.1, self._config.thread_sync_interval * 2.0)
        remaining = execution_deadline + collection_grace - monotonic()
        try:
            envelopes = await asyncio.wait_for(
                asyncio.gather(*futures), timeout=max(0.0, remaining)
            )
        except asyncio.TimeoutError as exc:
            for future in futures:
                if not future.done():
                    future.cancel()
            raise OrchestrationDeadlineExceeded(
                "orchestration result deadline expired"
            ) from exc
        except BaseException:
            for future in futures:
                if not future.done():
                    future.cancel()
            raise

        results = {
            envelope.worker_id: envelope.result
            for envelope in envelopes
            if envelope.result is not None
        }
        return OrchestrationReport(
            results=MappingProxyType(dict(results)),
            memory_revisions=MappingProxyType(self._memory.revisions()),
            events=tuple(self._events),
        )

    async def _dispatch_outbound(self) -> None:
        """Route every worker envelope through one correlation authority."""
        assert self._outbound is not None
        while True:
            envelope = await self._outbound.get()
            if envelope.kind is EnvelopeKind.WORKER_FAILED:
                message = envelope.error or "unknown worker failure"
                self._failed_workers[envelope.worker_id] = message
                self._events.append(
                    Event(EventKind.WORKER_FAILED, envelope.worker_id, message)
                )
                self._fail_worker_requests(
                    envelope.worker_id,
                    WorkerUnavailableError(
                        f"worker {envelope.worker_id} failed: {message}"
                    ),
                )
                continue
            if envelope.kind is EnvelopeKind.WORKER_STOPPED:
                self._events.append(
                    Event(EventKind.WORKER_STOPPED, envelope.worker_id, "worker stopped")
                )
                self._fail_worker_requests(
                    envelope.worker_id,
                    OrchestrationCancelledError(
                        f"worker {envelope.worker_id} stopped during orchestration"
                    ),
                )
                continue

            request = self._pending.pop(envelope.correlation_id, None)
            if request is None:
                LOGGER.warning(
                    "Ignoring envelope with unknown correlation %s",
                    envelope.correlation_id,
                )
                continue
            if request.worker_id != envelope.worker_id:
                self._set_future_exception(
                    request.future,
                    RuntimeError("worker envelope violated correlation ownership"),
                )
                continue
            if (
                envelope.result is None
                or envelope.snapshot is None
                or envelope.sync_token is None
            ):
                self._set_future_exception(
                    request.future,
                    RuntimeError("worker returned an incomplete result envelope"),
                )
                continue
            try:
                self._memory.publish(envelope.snapshot, envelope.sync_token)
            except Exception as exc:
                self._set_future_exception(request.future, exc)
                continue
            event_kind = (
                EventKind.TASK_COMPLETED
                if envelope.result.succeeded
                else EventKind.TASK_FAILED
            )
            self._events.append(
                Event(
                    event_kind,
                    envelope.worker_id,
                    envelope.result.status.value,
                    envelope.result.task_id,
                )
            )
            self._events.append(
                Event(EventKind.MEMORY_SYNCED, envelope.worker_id, "snapshot accepted")
            )
            if not request.future.done():
                request.future.set_result(envelope)

    def _fail_worker_requests(self, worker_id: str, exc: Exception) -> None:
        """Fail and remove every pending correlation owned by one worker."""
        matches = [
            correlation_id
            for correlation_id, request in self._pending.items()
            if request.worker_id == worker_id
        ]
        for correlation_id in matches:
            request = self._pending.pop(correlation_id)
            self._set_future_exception(request.future, exc)

    @staticmethod
    def _set_future_exception(
        future: asyncio.Future[WorkerEnvelope], exc: Exception
    ) -> None:
        if not future.done():
            future.set_exception(exc)

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
        self._closed = True
        if self._threads is not None:
            shutdown_timeout = self._config.task_timeout + 2.0
            await self._threads.stop_all(shutdown_timeout)
            # Worker threads emit their lifecycle envelopes before terminating.
            # Yield twice so the orchestrator loop can enqueue and dispatch them.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        for request in tuple(self._pending.values()):
            self._set_future_exception(
                request.future,
                OrchestrationCancelledError("orchestrator closed during active work"),
            )
        self._pending.clear()
        await self._run_idle.wait()
        if self._dispatcher_task is not None:
            self._dispatcher_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._dispatcher_task
            self._dispatcher_task = None

    async def __aenter__(self) -> "Orchestrator":
        """Start workers for an asynchronous context manager."""
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Always close workers when leaving the context."""
        await self.close()
