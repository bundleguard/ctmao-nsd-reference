"""Worker-thread lifecycle and cross-loop message transport."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Iterable
from uuid import UUID, uuid4

from .config import RuntimeConfig
from .memory import ThreadLocalMemory
from .supervisor import SupervisorAgent
from .types import (
    CommandKind,
    EnvelopeKind,
    WorkerCommand,
    WorkerEnvelope,
)

LOGGER = logging.getLogger(__name__)


class WorkerUnavailableError(RuntimeError):
    """Raised when a command targets a stopped or failed worker runtime."""


class WorkerThread:
    """A non-daemon thread owning one asyncio loop, supervisor, and memory store."""

    def __init__(
        self,
        worker_id: str,
        config: RuntimeConfig,
        orchestrator_loop: asyncio.AbstractEventLoop,
        outbound: asyncio.Queue[WorkerEnvelope],
    ) -> None:
        """Prepare the thread without creating loop-bound worker resources."""
        self.worker_id = worker_id
        self._config = config
        self._orchestrator_loop = orchestrator_loop
        self._outbound = outbound
        self._worker_loop: asyncio.AbstractEventLoop | None = None
        self._inbox: asyncio.Queue[WorkerCommand] | None = None
        self._ready = threading.Event()
        self._accepting = threading.Event()
        self._active_correlation: UUID | None = None
        self._runner_task: asyncio.Task[None] | None = None
        self._stop_correlation: UUID | None = None
        self._stop_requested = threading.Event()
        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"ctmao-worker-{worker_id}",
            daemon=False,
        )

    @property
    def is_alive(self) -> bool:
        """Return whether the owned OS thread is alive."""
        return self._thread.is_alive()

    @property
    def thread_ident(self) -> int | None:
        """Expose the thread identity for diagnostics without exposing state."""
        return self._thread.ident

    def start(self, timeout: float = 2.0) -> None:
        """Start and wait until the worker's event-loop inbox exists."""
        self._thread.start()
        if not self._ready.wait(timeout):
            raise TimeoutError(f"worker {self.worker_id} did not become ready")

    def submit(self, command: WorkerCommand) -> None:
        """Transfer an immutable command into the worker-owned event loop."""
        if (
            not self._ready.is_set()
            or not self._accepting.is_set()
            or not self._thread.is_alive()
            or self._worker_loop is None
            or self._worker_loop.is_closed()
            or self._inbox is None
        ):
            raise WorkerUnavailableError(
                f"worker {self.worker_id} is not accepting commands"
            )
        try:
            self._worker_loop.call_soon_threadsafe(self._inbox.put_nowait, command)
        except RuntimeError as exc:
            raise WorkerUnavailableError(
                f"worker {self.worker_id} event loop is unavailable"
            ) from exc

    def join(self, timeout: float) -> None:
        """Wait a bounded interval for the non-daemon worker to stop."""
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError(f"worker {self.worker_id} did not stop")

    def request_stop(self, correlation_id: UUID | None = None) -> None:
        """Cancel the worker root coroutine from the orchestrator thread."""
        if not self._thread.is_alive():
            return
        self._accepting.clear()
        if self._stop_requested.is_set():
            return
        self._stop_requested.set()
        stop_correlation = correlation_id or uuid4()
        if self._worker_loop is None or self._worker_loop.is_closed():
            return
        self._worker_loop.call_soon_threadsafe(
            self._cancel_runner, stop_correlation
        )

    def _cancel_runner(self, correlation_id: UUID) -> None:
        """Record stop identity and cancel on the worker-owned event loop."""
        self._stop_correlation = correlation_id
        if self._runner_task is not None and not self._runner_task.done():
            self._runner_task.cancel()

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except Exception as exc:  # protect the process from one worker failure
            LOGGER.exception("Worker %s crashed", self.worker_id)
            self._accepting.clear()
            self._emit(
                WorkerEnvelope(
                    kind=EnvelopeKind.WORKER_FAILED,
                    worker_id=self.worker_id,
                    correlation_id=(
                        self._active_correlation
                        or WorkerCommand(CommandKind.STOP).correlation_id
                    ),
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
        finally:
            self._accepting.clear()

    async def _run(self) -> None:
        self._runner_task = asyncio.current_task()
        self._worker_loop = asyncio.get_running_loop()
        self._inbox = asyncio.Queue()
        memory = ThreadLocalMemory(
            self.worker_id,
            transferable_keys=("root_task", "last_task", "last_agent_depth"),
        )
        supervisor = SupervisorAgent(self.worker_id, memory, self._config)
        self._accepting.set()
        self._ready.set()
        try:
            while True:
                command = await self._inbox.get()
                self._active_correlation = command.correlation_id
                if command.kind is CommandKind.STOP:
                    self._accepting.clear()
                    self._emit(
                        WorkerEnvelope(
                            kind=EnvelopeKind.WORKER_STOPPED,
                            worker_id=self.worker_id,
                            correlation_id=command.correlation_id,
                        )
                    )
                    self._active_correlation = None
                    return
                try:
                    if command.task is None or command.sync_token is None:
                        raise ValueError("RUN command requires a task and sync token")
                    result = await supervisor.execute(
                        command.task, deadline=command.deadline
                    )
                    snapshot = memory.snapshot()
                    self._emit(
                        WorkerEnvelope(
                            kind=EnvelopeKind.RESULT,
                            worker_id=self.worker_id,
                            correlation_id=command.correlation_id,
                            result=result,
                            snapshot=snapshot,
                            sync_token=command.sync_token,
                        )
                    )
                except Exception as exc:
                    self._accepting.clear()
                    self._emit(
                        WorkerEnvelope(
                            kind=EnvelopeKind.WORKER_FAILED,
                            worker_id=self.worker_id,
                            correlation_id=command.correlation_id,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    return
                finally:
                    self._active_correlation = None
        except asyncio.CancelledError:
            if not self._stop_requested.is_set():
                raise
            self._emit(
                WorkerEnvelope(
                    kind=EnvelopeKind.WORKER_STOPPED,
                    worker_id=self.worker_id,
                    correlation_id=self._stop_correlation or uuid4(),
                )
            )
        finally:
            self._accepting.clear()
            self._runner_task = None

    def _emit(self, envelope: WorkerEnvelope) -> None:
        self._orchestrator_loop.call_soon_threadsafe(
            self._outbound.put_nowait, envelope
        )


class ThreadManager:
    """Owns the worker collection and provides bounded lifecycle operations."""

    def __init__(
        self,
        worker_ids: Iterable[str],
        config: RuntimeConfig,
        orchestrator_loop: asyncio.AbstractEventLoop,
        outbound: asyncio.Queue[WorkerEnvelope],
    ) -> None:
        """Construct named worker wrappers."""
        ids = tuple(worker_ids)
        if len(ids) != len(set(ids)):
            raise ValueError("worker identifiers must be unique")
        self._workers = {
            worker_id: WorkerThread(worker_id, config, orchestrator_loop, outbound)
            for worker_id in ids
        }

    @property
    def worker_ids(self) -> tuple[str, ...]:
        """Return configured worker identifiers in stable order."""
        return tuple(self._workers)

    def start_all(self) -> None:
        """Start every worker, cleaning up partial startup on failure."""
        started: list[WorkerThread] = []
        try:
            for worker in self._workers.values():
                worker.start()
                started.append(worker)
        except Exception:
            for worker in started:
                worker.submit(WorkerCommand(CommandKind.STOP))
                worker.join(2.0)
            raise

    def submit(self, worker_id: str, command: WorkerCommand) -> None:
        """Route a command to exactly one worker."""
        self._workers[worker_id].submit(command)

    async def stop_all(self, timeout: float) -> None:
        """Request cooperative shutdown and join all workers."""
        for worker in self._workers.values():
            if worker.is_alive:
                worker.request_stop()
        await asyncio.gather(
            *(
                asyncio.to_thread(worker.join, timeout)
                for worker in self._workers.values()
                if worker.is_alive
            )
        )

    def identities(self) -> dict[str, int | None]:
        """Return worker thread identities for observability and tests."""
        return {key: worker.thread_ident for key, worker in self._workers.items()}
