"""Private worker memory and orchestrator-owned synchronized memory."""

from __future__ import annotations

from dataclasses import dataclass
from threading import get_ident
from typing import Iterable

from .types import MemorySnapshot, Scalar, SyncToken


class ThreadOwnershipError(RuntimeError):
    """Raised when private memory is touched outside its owner thread."""


class MemoryRevisionConflict(RuntimeError):
    """Raised when a stale worker snapshot attempts to replace a newer one."""


class ThreadLocalMemory:
    """Mutable memory whose methods enforce thread ownership at runtime."""

    def __init__(self, worker_id: str, transferable_keys: Iterable[str]) -> None:
        """Bind a private store to the thread constructing it."""
        self.worker_id = worker_id
        self._owner_ident = get_ident()
        self._transferable_keys = frozenset(transferable_keys)
        self._values: dict[str, Scalar] = {}
        self._revision = 0

    def _assert_owner(self) -> None:
        if get_ident() != self._owner_ident:
            raise ThreadOwnershipError(
                f"memory for {self.worker_id} accessed outside its owner thread"
            )

    def set(self, key: str, value: Scalar) -> None:
        """Set a private value and increment the local revision."""
        self._assert_owner()
        self._values[key] = value
        self._revision += 1

    def snapshot(self) -> MemorySnapshot:
        """Export only values explicitly approved for synchronization."""
        self._assert_owner()
        entries = tuple(
            sorted(
                (key, value)
                for key, value in self._values.items()
                if key in self._transferable_keys
            )
        )
        return MemorySnapshot(self.worker_id, self._revision, entries)


@dataclass(frozen=True, slots=True)
class PublishedMemory:
    """One accepted snapshot plus the token that authorized it."""

    snapshot: MemorySnapshot
    token: SyncToken


class SharedMemoryHub:
    """Authoritative cross-thread memory, exclusively owned by the orchestrator."""

    def __init__(self) -> None:
        """Create an empty revisioned store."""
        self._published: dict[str, PublishedMemory] = {}
        self._used_tokens: set[object] = set()

    def publish(self, snapshot: MemorySnapshot, token: SyncToken) -> None:
        """Accept a fresh snapshot through a matching, single-use token."""
        if token.worker_id != snapshot.worker_id:
            raise ValueError("sync token does not match snapshot worker")
        if token.token_id in self._used_tokens:
            raise ValueError("sync token has already been used")
        current = self._published.get(snapshot.worker_id)
        if current and snapshot.revision <= current.snapshot.revision:
            raise MemoryRevisionConflict("snapshot revision is not newer")
        self._published[snapshot.worker_id] = PublishedMemory(snapshot, token)
        self._used_tokens.add(token.token_id)

    def read_worker(self, worker_id: str) -> dict[str, Scalar]:
        """Return a detached view of the latest synchronized worker state."""
        published = self._published.get(worker_id)
        return dict(published.snapshot.entries) if published else {}

    def revisions(self) -> dict[str, int]:
        """Return published revisions for diagnostics."""
        return {
            worker_id: item.snapshot.revision
            for worker_id, item in self._published.items()
        }
