"""Worker-thread lifecycle and cross-loop message transport."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Iterable

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
        if not self._ready.is_set() or self._worker_loop is None or self._inbox is None:
            raise RuntimeError(f"worker {self.worker_id} is not ready")
        self._worker_loop.call_soon_threadsafe(self._inbox.put_nowait, command)

    def join(self, timeout: float) -> None:
        """Wait a bounded interval for the non-daemon worker to stop."""
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError(f"worker {self.worker_id} did not stop")

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except Exception as exc:  # protect the process from one worker failure
            LOGGER.exception("Worker %s crashed", self.worker_id)
            self._emit(
                WorkerEnvelope(
                    kind=EnvelopeKind.WORKER_FAILED,
                    worker_id=self.worker_id,
                    correlation_id=WorkerCommand(CommandKind.STOP).correlation_id,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )

    async def _run(self) -> None:
        self._worker_loop = asyncio.get_running_loop()
        self._inbox = asyncio.Queue()
        memory = ThreadLocalMemory(
            self.worker_id,
            transferable_keys=("root_task", "last_task", "last_agent_depth"),
        )
        supervisor = SupervisorAgent(self.worker_id, memory, self._config)
        self._ready.set()
        while True:
            command = await self._inbox.get()
            if command.kind is CommandKind.STOP:
                self._emit(
                    WorkerEnvelope(
                        kind=EnvelopeKind.WORKER_STOPPED,
                        worker_id=self.worker_id,
                        correlation_id=command.correlation_id,
                    )
                )
                return
            if command.task is None or command.sync_token is None:
                raise ValueError("RUN command requires a task and sync token")
            result = await supervisor.execute(command.task)
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

    def stop_all(self) -> None:
        """Request cooperative shutdown and join all workers."""
        for worker in self._workers.values():
            if worker.is_alive:
                worker.submit(WorkerCommand(CommandKind.STOP))
        for worker in self._workers.values():
            if worker.is_alive:
                worker.join(2.0)

    def identities(self) -> dict[str, int | None]:
        """Return worker thread identities for observability and tests."""
        return {key: worker.thread_ident for key, worker in self._workers.items()}
