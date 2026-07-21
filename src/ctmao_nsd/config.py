"""Runtime policy for the CTMAO-NSD reference implementation."""

from __future__ import annotations

from dataclasses import dataclass

MAX_DELEGATION_DEPTH = 3
MAX_CHILDREN_PER_AGENT = 4
TASK_TIMEOUT = 5.0
THREAD_SYNC_INTERVAL = 0.05
MEMORY_SYNC_INTERVAL = 0.25


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """Configurable safety limits shared by orchestrator and workers."""

    max_delegation_depth: int = MAX_DELEGATION_DEPTH
    max_children_per_agent: int = MAX_CHILDREN_PER_AGENT
    task_timeout: float = TASK_TIMEOUT
    thread_sync_interval: float = THREAD_SYNC_INTERVAL
    memory_sync_interval: float = MEMORY_SYNC_INTERVAL

    def __post_init__(self) -> None:
        """Reject policies that could create ambiguous runtime behavior."""
        if self.max_delegation_depth < 1:
            raise ValueError("max_delegation_depth must be at least 1")
        if self.max_children_per_agent < 1:
            raise ValueError("max_children_per_agent must be at least 1")
        if self.task_timeout <= 0:
            raise ValueError("task_timeout must be positive")
        if self.thread_sync_interval <= 0 or self.memory_sync_interval <= 0:
            raise ValueError("sync intervals must be positive")
