"""Interactive, bounded, provider-backed agent conference application layer."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
import threading
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping
from uuid import UUID, uuid4

from .agent_runtime import ModelBackedRuntime, ModelProvider, ModelRequest, ModelResponse
from .config import RuntimeConfig
from .normalized_events import redact_text, utc_now_rfc3339
from .observability import EventBus
from .orchestrator import Orchestrator
from .types import TaskResult, TaskSpec


class ConferenceBusyError(RuntimeError):
    """Raised when a second conference is requested while one is active."""


@dataclass(frozen=True, slots=True)
class Participant:
    """Stable human-facing participant identity and public responsibility."""

    participant_id: str
    display_name: str
    responsibility: str


DEFAULT_PARTICIPANTS = (
    Participant("Researcher", "Researcher", "Identify facts, assumptions, and uncertainty."),
    Participant("Builder", "Builder", "Propose a concrete and implementable approach."),
    Participant("Reviewer", "Reviewer", "Challenge the proposals and identify risks."),
    Participant("Moderator", "Moderator", "Synthesize the discussion and next actions."),
)


class DemoConferenceProvider:
    """Safe local provider that proves message flow without pretending to be an LLM."""

    async def complete(self, request: ModelRequest) -> ModelResponse:
        await asyncio.sleep(0)
        content = str(request.value or "").strip()
        objective = content.splitlines()[0][:240] if content else "the objective"
        messages = {
            "Researcher": (
                f"For {objective!r}, I would first verify the requested outcome, "
                "available evidence, constraints, and any facts that may have changed. "
                "This is a deterministic demo contribution, not external research."
            ),
            "Builder": (
                f"For {objective!r}, I propose a small reversible implementation, "
                "clear acceptance checks, and a tested handoff before expansion. "
                "This is a deterministic demo proposal."
            ),
            "Reviewer": (
                "I reviewed the supplied Researcher and Builder messages. The main "
                "risks are treating assumptions as evidence, expanding scope without "
                "approval, and skipping failure-path tests."
            ),
            "Moderator": (
                "The conference agrees on a bounded next step: confirm the objective, "
                "implement the smallest reversible change, verify success and failure "
                "paths, and retain provenance for the final decision."
            ),
        }
        return ModelResponse(messages[request.worker_id], "demo", "deterministic-conference")


class InteractiveConference:
    """Coordinate genuine public participant outputs across three bounded rounds."""

    def __init__(
        self,
        event_bus: EventBus,
        provider: ModelProvider,
        *,
        provider_name: str,
        participants: tuple[Participant, ...] = DEFAULT_PARTICIPANTS,
        task_timeout: float = 90.0,
    ) -> None:
        ids = tuple(item.participant_id for item in participants)
        if len(ids) != len(set(ids)) or set(ids) != {
            "Researcher",
            "Builder",
            "Reviewer",
            "Moderator",
        }:
            raise ValueError("interactive conference requires the four standard roles")
        self._event_bus = event_bus
        self._provider = provider
        self._provider_name = provider_name
        self._participants = participants
        self._task_timeout = task_timeout
        self._condition = threading.Condition()
        self._status = "idle"
        self._conference_id: UUID | None = None
        self._started_at: str | None = None
        self._finished_at: str | None = None
        self._error: str | None = None
        self._future: Future[None] | None = None
        self._closed = False
        self._loop_ready = threading.Event()
        self._thread = threading.Thread(
            target=self._loop_main, name="ctmao-conference-control", daemon=True
        )
        self._thread.start()
        if not self._loop_ready.wait(2.0):
            raise RuntimeError("conference control loop did not start")

    def start(self, objective: str) -> Mapping[str, object]:
        """Schedule one conference without blocking an HTTP request thread."""
        objective = objective.strip()
        if not objective or len(objective) > 4000:
            raise ValueError("objective must contain between 1 and 4000 characters")
        with self._condition:
            if self._closed:
                raise RuntimeError("conference controller is closed")
            if self._status in {"queued", "running", "cancelling"}:
                raise ConferenceBusyError("a conference is already active")
            conference_id = uuid4()
            self._conference_id = conference_id
            self._status = "queued"
            self._started_at = utc_now_rfc3339()
            self._finished_at = None
            self._error = None
            future = asyncio.run_coroutine_threadsafe(
                self._run(conference_id, objective), self._loop
            )
            self._future = future
            future.add_done_callback(self._future_done)
            self._condition.notify_all()
            return self.snapshot()

    def cancel(self) -> Mapping[str, object]:
        with self._condition:
            future = self._future
            if self._status not in {"queued", "running"} or future is None:
                return self.snapshot()
            self._status = "cancelling"
            future.cancel()
            self._condition.notify_all()
            return self.snapshot()

    def snapshot(self) -> Mapping[str, object]:
        with self._condition:
            return MappingProxyType(
                {
                    "conference_id": str(self._conference_id) if self._conference_id else None,
                    "status": self._status,
                    "provider": self._provider_name,
                    "participants": [
                        {
                            "participant_id": item.participant_id,
                            "display_name": item.display_name,
                            "responsibility": item.responsibility,
                        }
                        for item in self._participants
                    ],
                    "started_at": self._started_at,
                    "finished_at": self._finished_at,
                    "error": self._error,
                }
            )

    def wait(self, timeout: float | None = None) -> bool:
        """Wait for a terminal state, primarily for tests and command-line demos."""
        with self._condition:
            return self._condition.wait_for(
                lambda: self._status in {"completed", "failed", "cancelled"}, timeout
            )

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            future = self._future
            if future is not None and self._status in {"queued", "running", "cancelling"}:
                future.cancel()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2.0)

    def _loop_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._loop.close()

    def _future_done(self, future: Future[None]) -> None:
        if future.cancelled():
            with self._condition:
                if self._status in {"queued", "running", "cancelling"}:
                    self._status = "cancelled"
                    self._finished_at = utc_now_rfc3339()
                    self._condition.notify_all()

    async def _run(self, conference_id: UUID, objective: str) -> None:
        with self._condition:
            if self._status in {"cancelling", "cancelled"}:
                return
            self._status = "running"
            self._condition.notify_all()
        config = RuntimeConfig(
            task_timeout=self._task_timeout,
            publish_agent_messages=True,
        )
        role_ids = tuple(item.participant_id for item in self._participants)
        try:
            async with Orchestrator(
                role_ids,
                config,
                event_bus=self._event_bus,
                runtime_factory=lambda: ModelBackedRuntime(self._provider),
            ) as orchestrator:
                first = await orchestrator.run(
                    {
                        "Researcher": _assignment(
                            "Research the objective",
                            objective,
                            "analysis",
                            "conference",
                        ),
                        "Builder": _assignment(
                            "Propose an implementation",
                            objective,
                            "proposal",
                            "conference",
                        ),
                    }
                )
                research = _public_output(first.results["Researcher"])
                proposal = _public_output(first.results["Builder"])
                review_context = _context(
                    objective,
                    (("Researcher", research), ("Builder", proposal)),
                )
                second = await orchestrator.run(
                    {
                        "Reviewer": _assignment(
                            "Review the participants' contributions",
                            review_context,
                            "review",
                            "Researcher,Builder,conference",
                        )
                    }
                )
                review = _public_output(second.results["Reviewer"])
                synthesis_context = _context(
                    objective,
                    (
                        ("Researcher", research),
                        ("Builder", proposal),
                        ("Reviewer", review),
                    ),
                )
                final = await orchestrator.run(
                    {
                        "Moderator": _assignment(
                            "Synthesize the conference",
                            synthesis_context,
                            "decision",
                            "conference",
                        )
                    }
                )
                _public_output(final.results["Moderator"])
            self._set_state("completed", finished=True)
        except asyncio.CancelledError:
            self._set_state("cancelled", finished=True)
            raise
        except Exception as exc:
            safe_error, _ = redact_text(f"{type(exc).__name__}: {exc}")
            self._set_state("failed", error=safe_error, finished=True)

    def _set_state(
        self, status: str, *, error: str | None = None, finished: bool = False
    ) -> None:
        with self._condition:
            self._status = status
            self._error = error
            if finished:
                self._finished_at = utc_now_rfc3339()
            self._condition.notify_all()


def _assignment(name: str, value: str, kind: str, recipient: str) -> TaskSpec:
    return TaskSpec(
        f"{name} root",
        children=(
            TaskSpec(
                name,
                value=value[:16_000],
                publish_output=True,
                message_kind=kind,
                recipient_id=recipient,
            ),
        ),
    )


def _public_output(result: TaskResult) -> str:
    if not result.succeeded or not result.children or not result.children[0].output:
        raise RuntimeError(f"participant {result.worker_id} did not return a public message")
    return result.children[0].output


def _context(objective: str, messages: tuple[tuple[str, str], ...]) -> str:
    parts = [f"Objective: {objective}", "Public conference messages:"]
    parts.extend(f"{speaker}: {message}" for speaker, message in messages)
    return "\n\n".join(parts)[:16_000]
