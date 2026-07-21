"""Worker-local execution context propagated through asynchronous delegation."""

from __future__ import annotations

from contextvars import ContextVar

current_worker_id: ContextVar[str] = ContextVar("current_worker_id", default="unbound")
current_agent_path: ContextVar[tuple[str, ...]] = ContextVar(
    "current_agent_path", default=()
)
