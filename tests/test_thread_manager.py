"""Worker-runtime lifecycle and fatal-boundary regression tests."""

from __future__ import annotations

import asyncio
import unittest

from ctmao_nsd.config import RuntimeConfig
from ctmao_nsd.thread_manager import WorkerThread, WorkerUnavailableError
from ctmao_nsd.types import CommandKind, EnvelopeKind, WorkerCommand, WorkerEnvelope


class WorkerThreadTests(unittest.IsolatedAsyncioTestCase):
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
