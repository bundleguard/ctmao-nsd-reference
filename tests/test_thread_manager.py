"""Worker-runtime lifecycle and fatal-boundary regression tests."""

from __future__ import annotations

import asyncio
import threading
import unittest

from ctmao_nsd import DeterministicRuntime
from ctmao_nsd.config import RuntimeConfig
from ctmao_nsd.thread_manager import ThreadManager, WorkerThread, WorkerUnavailableError
from ctmao_nsd.types import CommandKind, EnvelopeKind, WorkerCommand, WorkerEnvelope


class WorkerThreadTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_construction_does_not_delay_worker_readiness(self) -> None:
        outbound: asyncio.Queue[WorkerEnvelope] = asyncio.Queue()
        release = threading.Event()

        def delayed_factory() -> DeterministicRuntime:
            release.wait(1.0)
            return DeterministicRuntime()

        worker = WorkerThread(
            "delayed-runtime",
            RuntimeConfig(),
            asyncio.get_running_loop(),
            outbound,
            delayed_factory,
        )
        await asyncio.wait_for(asyncio.to_thread(worker.start, 0.1), timeout=0.5)
        release.set()
        worker.request_stop()
        await asyncio.to_thread(worker.join, 1.0)

    async def test_startup_rollback_includes_worker_that_raises(self) -> None:
        class FailedStartWorker:
            is_alive = True

            def __init__(self) -> None:
                self.stop_requested = False
                self.joined = False

            def start(self) -> None:
                raise TimeoutError("not ready")

            def request_stop(self) -> None:
                self.stop_requested = True

            def join(self, timeout: float) -> None:
                self.joined = True

        manager = ThreadManager(
            ("A",), RuntimeConfig(), asyncio.get_running_loop(), asyncio.Queue()
        )
        failed = FailedStartWorker()
        manager._workers = {"A": failed}  # type: ignore[assignment]
        with self.assertRaises(TimeoutError):
            manager.start_all()
        self.assertTrue(failed.stop_requested)
        self.assertTrue(failed.joined)

    async def test_fatal_command_retains_correlation_and_worker_rejects_reuse(
        self,
    ) -> None:
        outbound: asyncio.Queue[WorkerEnvelope] = asyncio.Queue()
        worker = WorkerThread(
            "fatal-boundary",
            RuntimeConfig(),
            asyncio.get_running_loop(),
            outbound,
        )
        worker.start()

        malformed = WorkerCommand(CommandKind.RUN)
        worker.submit(malformed)
        envelope = await asyncio.wait_for(outbound.get(), timeout=1.0)

        self.assertEqual(envelope.kind, EnvelopeKind.WORKER_FAILED)
        self.assertEqual(envelope.correlation_id, malformed.correlation_id)
        await asyncio.to_thread(worker.join, 1.0)
        self.assertFalse(worker.is_alive)
        with self.assertRaises(WorkerUnavailableError):
            worker.submit(WorkerCommand(CommandKind.STOP))

    async def test_stopped_worker_rejects_new_commands(self) -> None:
        outbound: asyncio.Queue[WorkerEnvelope] = asyncio.Queue()
        worker = WorkerThread(
            "stopped-boundary",
            RuntimeConfig(),
            asyncio.get_running_loop(),
            outbound,
        )
        worker.start()

        stop = WorkerCommand(CommandKind.STOP)
        worker.submit(stop)
        envelope = await asyncio.wait_for(outbound.get(), timeout=1.0)
        self.assertEqual(envelope.kind, EnvelopeKind.WORKER_STOPPED)
        self.assertEqual(envelope.correlation_id, stop.correlation_id)
        await asyncio.to_thread(worker.join, 1.0)

        with self.assertRaises(WorkerUnavailableError):
            worker.submit(WorkerCommand(CommandKind.STOP))
