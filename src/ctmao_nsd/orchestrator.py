"""Global orchestration, result collection, and controlled memory sync."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from types import MappingProxyType
from typing import Mapping
from uuid import UUID, uuid4

from .config import RuntimeConfig
from .agent_runtime import AgentRuntime
from .events import Event, EventKind
from .memory import SharedMemoryHub
from .normalized_events import (
    EventImportance,
    EventOrigin,
    EvidenceKind,
    NormalizedEvent,
    redact_text,
)
from .observability import EventBus
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
    run_id: UUID
    future: asyncio.Future[WorkerEnvelope]


@dataclass(frozen=True, slots=True)
class OrchestrationReport:
    """Combined immutable outcome for a multi-worker orchestration run."""

    results: Mapping[str, TaskResult]
    memory_revisions: Mapping[str, int]
    events: tuple[Event, ...]
    run_id: UUID | None = None
    normalized_events: tuple[NormalizedEvent, ...] = ()


class Orchestrator:
    """Coordinates isolated worker runtimes without sharing their mutable state."""

    def __init__(
        self,
        worker_ids: tuple[str, ...] = ("A", "B"),
        config: RuntimeConfig | None = None,
        *,
        event_bus: EventBus | None = None,
        runtime_factory: Callable[[], AgentRuntime] | None = None,
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
        self._close_requested = False
        self._close_lock = asyncio.Lock()
        self._run_active = False
        self._run_idle = asyncio.Event()
        self._run_idle.set()
        self._pending: dict[UUID, _PendingRequest] = {}
        self._failed_workers: dict[str, str] = {}
        self._dispatcher_task: asyncio.Task[None] | None = None
        self._session_id = uuid4()
        self._current_run_id: UUID | None = None
        self._event_sequence = 0
        self._normalized_by_run: dict[UUID, list[NormalizedEvent]] = {}
        self._known_events: dict[UUID, NormalizedEvent] = {}
        self._lifecycle_event_ids: dict[
            tuple[UUID, UUID, UUID, tuple[str, ...]], UUID
        ] = {}
        self._event_bus = event_bus
        self._runtime_factory = runtime_factory

    async def start(self) -> None:
        """Create cross-loop channels and start all worker runtimes."""
        if self._close_requested:
            raise OrchestratorClosedError("orchestrator is closed")
        if self._started:
            return
        loop = asyncio.get_running_loop()
        self._outbound = asyncio.Queue()
        self._threads = ThreadManager(
            self._worker_ids,
            self._config,
            loop,
            self._outbound,
            self._runtime_factory,
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
        for worker_id in self._worker_ids:
            self._record_normalized(
                "runtime.worker.started",
                "worker is ready",
                worker_id=worker_id,
                run_id=self._current_run_id,
            )

    async def run(self, assignments: Mapping[str, TaskSpec]) -> OrchestrationReport:
        """Run one root task per selected worker and synchronize their snapshots."""
        if self._close_requested:
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
        run_id = uuid4()
        self._current_run_id = run_id
        self._normalized_by_run[run_id] = []
        self._record_normalized(
            "orchestration.run.started",
            f"run started with {len(assignment_copy)} assignment(s)",
            run_id=run_id,
            importance=EventImportance.IMPORTANT,
        )
        try:
            return await self._run_once(assignment_copy, run_id)
        except (asyncio.CancelledError, OrchestrationCancelledError):
            self._record_normalized(
                "orchestration.run.cancelled",
                "run cancelled",
                run_id=run_id,
                importance=EventImportance.CRITICAL,
            )
            raise
        except Exception as exc:
            self._record_normalized(
                "orchestration.run.failed",
                f"run failed: {type(exc).__name__}",
                run_id=run_id,
                importance=EventImportance.CRITICAL,
            )
            raise
        finally:
            self._lifecycle_event_ids = {
                key: value
                for key, value in self._lifecycle_event_ids.items()
                if key[0] != run_id
            }
            self._current_run_id = None
            self._run_active = False
            self._run_idle.set()

    async def _run_once(
        self, assignments: Mapping[str, TaskSpec], run_id: UUID
    ) -> OrchestrationReport:
        """Execute one single-flight orchestration run."""
        await self.start()
        assert self._threads is not None and self._outbound is not None

        loop = asyncio.get_running_loop()
        execution_deadline = monotonic() + self._config.task_timeout
        futures: list[asyncio.Future[WorkerEnvelope]] = []
        self._validate_unique_task_ids(assignments)
        for task in assignments.values():
            self._validate_upstream_evidence(task)
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
                session_id=self._session_id,
                run_id=run_id,
            )
            future: asyncio.Future[WorkerEnvelope] = loop.create_future()
            self._pending[command.correlation_id] = _PendingRequest(
                worker_id, run_id, future
            )
            futures.append(future)
            self._events.append(
                Event(EventKind.TASK_STARTED, worker_id, "root task routed", task.task_id)
            )
            self._record_normalized(
                "task.assigned",
                f"root task assigned to worker {worker_id}",
                run_id=run_id,
                worker_id=worker_id,
                correlation_id=command.correlation_id,
                task_id=task.task_id,
                agent_path=(f"supervisor:{worker_id}",),
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
        self._record_normalized(
            "orchestration.run.completed",
            "run completed",
            run_id=run_id,
            importance=EventImportance.IMPORTANT,
        )
        return OrchestrationReport(
            results=MappingProxyType(dict(results)),
            memory_revisions=MappingProxyType(self._memory.revisions()),
            events=tuple(self._events),
            run_id=run_id,
            normalized_events=tuple(self._normalized_by_run[run_id]),
        )

    async def _dispatch_outbound(self) -> None:
        """Route every worker envelope through one correlation authority."""
        assert self._outbound is not None
        while True:
            envelope = await self._outbound.get()
            if envelope.kind is EnvelopeKind.EVENT:
                request = self._pending.get(envelope.correlation_id)
                if (
                    request is None
                    or request.worker_id != envelope.worker_id
                    or envelope.run_id != request.run_id
                    or envelope.observation is None
                ):
                    LOGGER.warning("Ignoring invalid worker observation envelope")
                    continue
                observation = envelope.observation
                importance = {
                    "debug": EventImportance.DEBUG,
                    "high": EventImportance.IMPORTANT,
                    "critical": EventImportance.CRITICAL,
                }.get(observation.importance, EventImportance.ROUTINE)
                try:
                    lifecycle_key = (
                        envelope.run_id or request.run_id,
                        envelope.correlation_id,
                        observation.task_id,
                        observation.agent_path,
                    ) if observation.task_id is not None else None
                    direct_cause = (
                        self._lifecycle_event_ids.get(lifecycle_key)
                        if lifecycle_key is not None
                        else None
                    )
                    if observation.event_type == "message.sent" and direct_cause is None:
                        raise ValueError(
                            "public message lacks a matching accepted task lifecycle"
                        )
                    event = self._record_normalized(
                        observation.event_type,
                        observation.summary,
                        run_id=envelope.run_id or self._current_run_id,
                        worker_id=envelope.worker_id,
                        correlation_id=envelope.correlation_id,
                        task_id=observation.task_id,
                        parent_task_id=observation.parent_task_id,
                        agent_path=observation.agent_path,
                        status=observation.status,
                        attributes=observation.attributes,
                        upstream_event_ids=observation.upstream_event_ids,
                        causation_event_id=direct_cause,
                        origin=EventOrigin.WORKER,
                        importance=importance,
                    )
                    if observation.event_type == "task.started" and lifecycle_key:
                        self._lifecycle_event_ids[lifecycle_key] = event.event_id
                except (TypeError, ValueError):
                    LOGGER.warning("Suppressing invalid worker observation")
                continue
            if envelope.kind is EnvelopeKind.WORKER_FAILED:
                message = envelope.error or "unknown worker failure"
                self._failed_workers[envelope.worker_id] = message
                self._events.append(
                    Event(EventKind.WORKER_FAILED, envelope.worker_id, message)
                )
                self._record_normalized(
                    "runtime.worker.failed",
                    message,
                    run_id=envelope.run_id or self._current_run_id,
                    worker_id=envelope.worker_id,
                    correlation_id=envelope.correlation_id,
                    importance=EventImportance.CRITICAL,
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
                self._record_normalized(
                    "runtime.worker.stopped",
                    "worker stopped",
                    run_id=envelope.run_id or self._current_run_id,
                    worker_id=envelope.worker_id,
                    correlation_id=envelope.correlation_id,
                    importance=EventImportance.IMPORTANT,
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
            if envelope.run_id != request.run_id:
                self._set_future_exception(
                    request.future,
                    RuntimeError("worker envelope violated run ownership"),
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
            self._record_normalized(
                "memory.snapshot.accepted",
                "memory snapshot accepted",
                run_id=envelope.run_id or self._current_run_id,
                worker_id=envelope.worker_id,
                correlation_id=envelope.correlation_id,
                task_id=envelope.result.task_id,
                status=envelope.result.status.value,
                attributes=(("revision", envelope.snapshot.revision),),
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

    @property
    def session_id(self) -> UUID:
        """Return the stable observability session identity."""
        return self._session_id

    def normalized_events(self, run_id: UUID) -> tuple[NormalizedEvent, ...]:
        """Return the run-scoped normalized events recorded so far."""
        return tuple(self._normalized_by_run.get(run_id, ()))

    def _record_normalized(
        self,
        event_type: str,
        summary: str,
        *,
        run_id: UUID | None = None,
        worker_id: str | None = None,
        correlation_id: UUID | None = None,
        task_id: UUID | None = None,
        parent_task_id: UUID | None = None,
        agent_path: tuple[str, ...] = (),
        status: str | None = None,
        attributes: tuple[tuple[str, object], ...] = (),
        upstream_event_ids: tuple[UUID, ...] = (),
        causation_event_id: UUID | None = None,
        origin: EventOrigin = EventOrigin.ORCHESTRATOR,
        importance: EventImportance = EventImportance.ROUTINE,
    ) -> NormalizedEvent:
        self._validate_event_references(
            event_type,
            causation_event_id=causation_event_id,
            upstream_event_ids=upstream_event_ids,
            correlation_id=correlation_id,
            task_id=task_id,
            agent_path=agent_path,
            worker_id=worker_id,
        )
        safe_summary, redactions = redact_text(summary[:4096])
        safe_worker_id = (
            redact_text(worker_id[:256])[0] if worker_id is not None else None
        )
        safe_agent_path = tuple(
            redact_text(component[:256])[0] for component in agent_path[:64]
        )
        safe_status = redact_text(status[:256])[0] if status is not None else None
        safe_attributes = tuple(
            (key, redact_text(value[:4096])[0] if isinstance(value, str) else value)
            for key, value in attributes[:64]
        )
        field_redacted = (
            safe_worker_id != worker_id
            or safe_agent_path != agent_path
            or safe_status != status
            or safe_attributes != attributes
        )
        if field_redacted and not redactions:
            redactions = (("redacted", True), ("redaction_rule", "structured-field"))
        self._event_sequence += 1
        provenance: tuple[tuple[str, object], ...] = (
            ("source", "ctmao-native-runtime"),
        )
        if upstream_event_ids:
            provenance += (
                ("source_event_ids", ",".join(str(item) for item in upstream_event_ids)),
            )
        event = NormalizedEvent(
            event_type=event_type,
            session_id=self._session_id,
            sequence=self._event_sequence,
            summary=safe_summary,
            origin=origin,
            evidence=EvidenceKind.OBSERVED,
            importance=importance,
            run_id=run_id,
            worker_id=safe_worker_id,
            correlation_id=correlation_id,
            task_id=task_id,
            parent_task_id=parent_task_id,
            causation_event_id=causation_event_id,
            agent_path=safe_agent_path,
            status=safe_status,
            attributes=safe_attributes,  # type: ignore[arg-type]
            provenance=provenance,
            redactions=redactions,
        )
        if run_id is not None:
            self._normalized_by_run.setdefault(run_id, []).append(event)
        self._known_events[event.event_id] = event
        if self._event_bus is not None:
            self._event_bus.publish(event)
        return event

    def _validate_upstream_evidence(self, task: TaskSpec) -> None:
        """Reject native provenance links that are not accepted session events."""
        unknown = set(task.upstream_event_ids).difference(self._known_events)
        if unknown:
            raise ValueError("task cites unknown upstream event evidence")
        if any(
            self._known_events[event_id].event_type != "message.sent"
            for event_id in task.upstream_event_ids
        ):
            raise ValueError("task upstream evidence must cite public messages")
        for child in task.children:
            self._validate_upstream_evidence(child)

    def _validate_unique_task_ids(self, assignments: Mapping[str, TaskSpec]) -> None:
        """Reject ambiguous task identities across a complete run submission."""
        seen: set[UUID] = set()

        def visit(node: TaskSpec) -> None:
            if node.task_id in seen:
                raise ValueError("task IDs must be unique across run assignments")
            seen.add(node.task_id)
            for child in node.children:
                visit(child)

        for task in assignments.values():
            visit(task)

    def _validate_event_references(
        self,
        event_type: str,
        *,
        causation_event_id: UUID | None,
        upstream_event_ids: tuple[UUID, ...],
        correlation_id: UUID | None,
        task_id: UUID | None,
        agent_path: tuple[str, ...],
        worker_id: str | None,
    ) -> None:
        """Validate causal links against events already accepted in this session."""
        referenced = upstream_event_ids + (
            (causation_event_id,) if causation_event_id is not None else ()
        )
        if any(event_id not in self._known_events for event_id in referenced):
            raise ValueError("event cites unknown or non-prior session evidence")
        if event_type != "message.sent":
            return
        if causation_event_id is None:
            raise ValueError("public messages require a direct lifecycle cause")
        cause = self._known_events[causation_event_id]
        if (
            cause.event_type != "task.started"
            or cause.worker_id != worker_id
            or cause.correlation_id != correlation_id
            or cause.task_id != task_id
            or cause.agent_path != agent_path
        ):
            raise ValueError("public message cause does not match its task lifecycle")
        if any(
            self._known_events[event_id].event_type != "message.sent"
            for event_id in upstream_event_ids
        ):
            raise ValueError("public message upstream evidence must cite messages")

    async def close(self) -> None:
        """Cooperatively stop workers; calling close repeatedly is safe."""
        async with self._close_lock:
            if self._closed:
                return
            self._close_requested = True
            shutdown_error: Exception | None = None
            if self._threads is not None:
                shutdown_timeout = self._config.task_timeout + 2.0
                try:
                    await self._threads.stop_all(shutdown_timeout)
                except Exception as exc:
                    # Keep lifecycle state retryable.  A later close can join a
                    # provider that finished after this bounded attempt.
                    shutdown_error = exc
                # Worker threads emit lifecycle envelopes before terminating.
                # Yield twice so the orchestrator can dispatch those already sent.
                await asyncio.sleep(0)
                await asyncio.sleep(0)
            for request in tuple(self._pending.values()):
                self._set_future_exception(
                    request.future,
                    OrchestrationCancelledError(
                        "orchestrator closed during active work"
                    ),
                )
            self._pending.clear()
            await self._run_idle.wait()
            if shutdown_error is not None:
                raise shutdown_error
            if self._dispatcher_task is not None:
                self._dispatcher_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._dispatcher_task
                self._dispatcher_task = None
            self._closed = True

    async def __aenter__(self) -> "Orchestrator":
        """Start workers for an asynchronous context manager."""
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Always close workers when leaving the context."""
        await self.close()
