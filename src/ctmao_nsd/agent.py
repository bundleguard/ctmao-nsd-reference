"""Abstract agent contract used by the reference runtime."""

from __future__ import annotations

from abc import ABC, abstractmethod

from .delegation import DelegationContext
from .types import TaskResult, TaskSpec


class Agent(ABC):
    """Base contract for an asynchronous agent owned by one worker runtime."""

    @abstractmethod
    async def execute(self, task: TaskSpec, context: DelegationContext) -> TaskResult:
        """Execute a task inside the supplied bounded delegation context."""
        raise NotImplementedError
