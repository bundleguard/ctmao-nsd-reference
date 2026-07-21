"""Bounded delegation contexts and safety errors."""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from uuid import UUID

from .config import RuntimeConfig


class DelegationError(RuntimeError):
    """Base class for rejected delegation operations."""


class DelegationDepthExceeded(DelegationError):
    """Raised when a child would exceed the configured maximum depth."""


class DelegationWidthExceeded(DelegationError):
    """Raised when an agent requests too many direct children."""


class CircularDelegationDetected(DelegationError):
    """Raised when a task identifier reappears in its own ancestry."""


@dataclass(frozen=True, slots=True)
class DelegationContext:
    """Immutable lineage and absolute deadline for one execution-tree node."""

    depth: int
    lineage: tuple[UUID, ...]
    agent_path: tuple[str, ...]
    deadline: float
    config: RuntimeConfig

    @classmethod
    def root(
        cls,
        task_id: UUID,
        worker_id: str,
        config: RuntimeConfig,
        deadline: float | None = None,
    ) -> "DelegationContext":
        """Create the supervisor context at depth zero."""
        return cls(
            depth=0,
            lineage=(task_id,),
            agent_path=(f"supervisor:{worker_id}",),
            deadline=deadline if deadline is not None else monotonic() + config.task_timeout,
            config=config,
        )

    def for_child(self, task_id: UUID, agent_name: str) -> "DelegationContext":
        """Return a validated context for one direct child."""
        child_depth = self.depth + 1
        if child_depth > self.config.max_delegation_depth:
            raise DelegationDepthExceeded(
                f"delegation depth {child_depth} exceeds "
                f"maximum {self.config.max_delegation_depth}"
            )
        if task_id in self.lineage:
            raise CircularDelegationDetected(f"task {task_id} occurs twice in one lineage")
        return DelegationContext(
            depth=child_depth,
            lineage=(*self.lineage, task_id),
            agent_path=(*self.agent_path, agent_name),
            deadline=self.deadline,
            config=self.config,
        )

    def validate_child_count(self, count: int) -> None:
        """Reject a fan-out before any child coroutine is created."""
        if count > self.config.max_children_per_agent:
            raise DelegationWidthExceeded(
                f"requested {count} children; maximum is "
                f"{self.config.max_children_per_agent}"
            )

    def remaining_time(self) -> float:
        """Return time left on the execution tree's shared deadline."""
        return max(0.0, self.deadline - monotonic())
