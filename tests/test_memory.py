"""Tests for worker ownership and orchestrator-mediated publication."""

from __future__ import annotations

import threading
import unittest

from ctmao_nsd.memory import (
    MemoryRevisionConflict,
    SharedMemoryHub,
    ThreadLocalMemory,
    ThreadOwnershipError,
)
from ctmao_nsd.types import MemorySnapshot, SyncToken


class MemoryTests(unittest.TestCase):
    def test_snapshot_exports_only_allowlisted_values(self) -> None:
        memory = ThreadLocalMemory("A", ("public",))
        memory.set("public", "visible")
        memory.set("private", "hidden")
        self.assertEqual(dict(memory.snapshot().entries), {"public": "visible"})

    def test_foreign_thread_cannot_access_private_memory(self) -> None:
        memory = ThreadLocalMemory("A", ("public",))
        captured: list[BaseException] = []

        def foreign_access() -> None:
            try:
                memory.set("public", "invalid")
            except BaseException as exc:
                captured.append(exc)

        thread = threading.Thread(target=foreign_access)
        thread.start()
        thread.join(1.0)
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(captured[0], ThreadOwnershipError)

    def test_hub_rejects_stale_revision_and_reused_token(self) -> None:
        hub = SharedMemoryHub()
        token = SyncToken("A")
        hub.publish(MemorySnapshot("A", 2, (("key", "new"),)), token)
        with self.assertRaises(ValueError):
            hub.publish(MemorySnapshot("A", 3, (("key", "newer"),)), token)
        with self.assertRaises(MemoryRevisionConflict):
            hub.publish(
                MemorySnapshot("A", 1, (("key", "old"),)), SyncToken("A")
            )
        self.assertEqual(hub.read_worker("A"), {"key": "new"})

    def test_hub_rejects_mismatched_capability(self) -> None:
        with self.assertRaises(ValueError):
            SharedMemoryHub().publish(MemorySnapshot("A", 1, ()), SyncToken("B"))
