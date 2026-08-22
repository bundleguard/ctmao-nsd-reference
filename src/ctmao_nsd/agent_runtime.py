"""Provider-neutral task-intelligence boundary for deterministic or model runtimes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from uuid import UUID

from .types import Scalar


@dataclass(frozen=True, slots=True)
class RuntimeRequest:
    """Bounded node input passed to a thread-owned agent runtime."""

    task_id: UUID
    name: str
    value: Scalar
    duration: float
    should_fail: bool
    worker_id: str
    agent_path: tuple[str, ...]


@runtime_checkable
class AgentRuntime(Protocol):
    """Execution contract separated from orchestration and delegation policy."""

    async def execute(self, request: RuntimeRequest) -> str:
        """Execute one node without controlling its delegation tree."""
        ...


class DeterministicRuntime:
    """The unchanged v0.1.1 deterministic node behavior."""

    async def execute(self, request: RuntimeRequest) -> str:
        if request.duration:
            await asyncio.sleep(request.duration)
        if request.should_fail:
            raise RuntimeError(f"task {request.name!r} requested failure")
        return f"processed {request.name}: {request.value!r}"


@dataclass(frozen=True, slots=True)
class ModelRequest:
    """Provider-neutral structured model input; no orchestration authority."""

    task_id: UUID
    instruction: str
    value: Scalar
    worker_id: str
    agent_path: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ModelResponse:
    """Minimal structured response returned by an external model provider."""

    output: str
    provider: str
    model: str

    def __post_init__(self) -> None:
        if not self.output or not self.provider or not self.model:
            raise ValueError("model response fields must not be blank")


@runtime_checkable
class ModelProvider(Protocol):
    """Optional provider adapter implemented outside the orchestration core."""

    async def complete(self, request: ModelRequest) -> ModelResponse:
        ...


class ModelBackedRuntime:
    """Optional model-backed node runtime with no planning/delegation privileges."""

    def __init__(self, provider: ModelProvider) -> None:
        self._provider = provider

    async def execute(self, request: RuntimeRequest) -> str:
        response = await self._provider.complete(
            ModelRequest(
                request.task_id,
                request.name,
                request.value,
                request.worker_id,
                request.agent_path,
            )
        )
        return response.output
