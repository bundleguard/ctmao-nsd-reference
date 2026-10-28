"""Host-fed bridge for observing a real Codex subagent conference.

The bridge never launches Codex or reads Codex storage.  A trusted Codex host
starts a session, publishes only explicit public subagent messages and status
facts, and closes the session.  Each accepted fact becomes a presentation-safe
normalized event before reaching the dashboard.
"""

from __future__ import annotations

import hashlib
import threading
from types import MappingProxyType
from typing import Mapping
from uuid import UUID, uuid4

from .interactive_conference import ConferenceBusyError, DEFAULT_PARTICIPANTS
from .normalized_events import (
    EventImportance,
    EventOrigin,
    EvidenceKind,
    NormalizedEvent,
    redact_text,
    utc_now_rfc3339,
)
from .observability import EventBus


CODEX_BRIDGE_PROTOCOL_VERSION = "1.1"


_PARTICIPANT_STATES = frozenset(
    {"waiting", "working", "contributed", "failed", "finished"}
)
_MESSAGE_KINDS = frozenset({"analysis", "proposal", "review", "decision", "statement"})
_TERMINAL_STATES = frozenset({"completed", "failed", "cancelled"})
_PARTICIPANT_TRANSITIONS = {
    "waiting": frozenset({"working", "failed"}),
    "working": frozenset({"failed"}),
    "contributed": frozenset({"finished"}),
    "failed": frozenset(),
    "finished": frozenset(),
}
_MAX_ACCEPTED_EVENTS = 256
_INITIAL_ROLES = ("Researcher", "Builder")
_UPSTREAM_ROLES = {
    "Researcher": (),
    "Builder": (),
    "Reviewer": _INITIAL_ROLES,
    "Moderator": (*_INITIAL_ROLES, "Reviewer"),
}


class CodexConferenceBridge:
    """Thread-safe control plane populated by an authorized Codex host.

    This class is deliberately host-fed.  It records observable public outputs;
    it does not infer private reasoning, delegation, or agent relationships.
    """

    def __init__(self, event_bus: EventBus, *, provider_name: str = "Codex native subagents") -> None:
        self._event_bus = event_bus
        self._provider_name = provider_name
        self._lock = threading.RLock()
        self._session_id = uuid4()
        self._conference_id: UUID | None = None
        self._sequence = 0
        self._status = "idle"
        self._objective: str | None = None
        self._stage = "Waiting for a Codex task"
        self._started_at: str | None = None
        self._finished_at: str | None = None
        self._error: str | None = None
        self._warning: str | None = None
        self._current_speaker: str | None = None
        self._participant_states = {
            item.participant_id: "waiting" for item in DEFAULT_PARTICIPANTS
        }
        self._participant_tasks = {
            item.participant_id: uuid4() for item in DEFAULT_PARTICIPANTS
        }
        self._participant_correlations = {
            item.participant_id: uuid4() for item in DEFAULT_PARTICIPANTS
        }
        self._accepted_events: dict[UUID, NormalizedEvent] = {}
        self._run_started_event_id: UUID | None = None
        self._participant_last_event_ids: dict[str, UUID] = {}
        self._participant_work_event_ids: dict[str, UUID] = {}
        self._participant_message_ids: dict[str, UUID] = {}

    def start(self, objective: str) -> Mapping[str, object]:
        objective = objective.strip()
        if not objective or len(objective) > 4000:
            raise ValueError("objective must contain between 1 and 4000 characters")
        safe_objective, _ = redact_text(objective, max_length=4000)
        with self._lock:
            if self._status in {"queued", "running", "cancelling"}:
                raise ConferenceBusyError("a conference is already active")
            self._conference_id = uuid4()
            self._status = "running"
            self._objective = safe_objective
            self._stage = "Preparing Codex subagents"
            self._started_at = utc_now_rfc3339()
            self._finished_at = None
            self._error = None
            self._warning = None
            self._current_speaker = None
            self._participant_states = {
                item.participant_id: "waiting" for item in DEFAULT_PARTICIPANTS
            }
            self._participant_tasks = {
                item.participant_id: uuid4() for item in DEFAULT_PARTICIPANTS
            }
            self._participant_correlations = {
                item.participant_id: uuid4() for item in DEFAULT_PARTICIPANTS
            }
            self._accepted_events = {}
            self._participant_last_event_ids = {}
            self._participant_work_event_ids = {}
            self._participant_message_ids = {}
            event = self._publish_locked(
                "orchestration.run.started",
                "Codex conference started",
                status="running",
                importance=EventImportance.IMPORTANT,
                correlation_id=self._conference_id,
                provenance=(("source_interface", "codex_plugin_bridge"),),
            )
            self._run_started_event_id = event.event_id
            return self._receipt(event)

    def cancel(self) -> Mapping[str, object]:
        with self._lock:
            if self._status not in {"queued", "running", "cancelling"}:
                return self.snapshot()
            self._require_event_capacity(terminal=True)
            self._status = "cancelled"
            self._stage = "Cancelled"
            self._finished_at = utc_now_rfc3339()
            self._current_speaker = None
            event = self._publish_locked(
                "orchestration.run.cancelled",
                "Codex conference cancelled",
                status="cancelled",
                importance=EventImportance.CRITICAL,
                correlation_id=self._conference_id,
                causation_event_id=self._run_started_event_id,
                provenance=(("source_interface", "codex_plugin_bridge"),),
            )
            return self._receipt(event)

    def update_participant(
        self,
        participant_id: str,
        state: str,
        *,
        source_ref: str,
        detail: str | None = None,
    ) -> Mapping[str, object]:
        with self._lock:
            self._require_active()
            self._require_participant(participant_id)
            if state not in _PARTICIPANT_STATES:
                raise ValueError("participant state is not supported")
            self._validate_participant_transition(participant_id, state)
            self._require_event_capacity()
            source_digest = _source_digest(source_ref)
            self._participant_states[participant_id] = state
            if state == "working":
                self._stage = f"{participant_id} is working"
                event_type = "task.started"
                importance = EventImportance.ROUTINE
            elif state == "failed":
                self._stage = f"{participant_id} could not contribute"
                event_type = "task.failed"
                importance = EventImportance.CRITICAL
                if detail:
                    safe_detail, _ = redact_text(detail, max_length=512)
                    self._warning = safe_detail
            else:
                event_type = "task.completed" if state in {"contributed", "finished"} else "task.observed"
                importance = EventImportance.ROUTINE
            event = self._publish_locked(
                event_type,
                f"{participant_id} {state}",
                worker_id=participant_id,
                correlation_id=self._participant_correlations[participant_id],
                causation_event_id=(
                    self._participant_last_event_ids.get(participant_id)
                    or self._run_started_event_id
                ),
                task_id=self._participant_tasks[participant_id],
                agent_path=(participant_id,),
                status=state,
                importance=importance,
                provenance=(
                    ("source_interface", "codex_plugin_bridge"),
                    ("source_ref_sha256", source_digest),
                ),
            )
            self._participant_last_event_ids[participant_id] = event.event_id
            if state == "working":
                self._participant_work_event_ids[participant_id] = event.event_id
            return self._receipt(event)

    def publish_message(
        self,
        participant_id: str,
        text: str,
        *,
        message_kind: str,
        source_ref: str,
        recipient_id: str = "conference",
        causation_event_id: UUID | str | None = None,
        source_event_ids: tuple[UUID | str, ...] = (),
    ) -> Mapping[str, object]:
        with self._lock:
            self._require_active()
            self._require_participant(participant_id)
            if message_kind not in _MESSAGE_KINDS:
                raise ValueError("message kind is not supported")
            if participant_id in self._participant_message_ids:
                raise ValueError("participant already published a public message")
            if not isinstance(recipient_id, str) or not recipient_id.strip() or len(recipient_id) > 256:
                raise ValueError("recipient_id must be a bounded non-blank string")
            safe_text, redactions = redact_text(text, max_length=4096)
            safe_recipient, recipient_redactions = redact_text(
                recipient_id.strip(), max_length=256
            )
            redactions = _merge_redactions(redactions, recipient_redactions)
            if not text.strip() or safe_text == "[REDACTED]":
                raise ValueError("public message is blank or could not be presented safely")
            if not safe_recipient.strip():
                raise ValueError("recipient_id could not be presented safely")
            source_digest = _source_digest(source_ref)
            explicit_sources = self._validated_message_sources(
                participant_id, source_event_ids
            )
            direct_cause = self._validated_message_cause(
                participant_id, causation_event_id
            )
            self._require_event_capacity()
            self._participant_states[participant_id] = "contributed"
            self._current_speaker = participant_id
            self._stage = f"{participant_id} contributed"
            event = self._publish_locked(
                "message.sent",
                safe_text,
                worker_id=participant_id,
                correlation_id=self._participant_correlations[participant_id],
                causation_event_id=direct_cause,
                task_id=self._participant_tasks[participant_id],
                agent_path=(participant_id,),
                status="sent",
                importance=(
                    EventImportance.IMPORTANT
                    if message_kind == "decision"
                    else EventImportance.ROUTINE
                ),
                attributes=(
                    ("message_kind", message_kind),
                    ("recipient_id", safe_recipient),
                    ("speaker_id", participant_id),
                ),
                provenance=(
                    ("adapter", "ctmao_nsd.codex_plugin_bridge"),
                    ("adapter_version", CODEX_BRIDGE_PROTOCOL_VERSION),
                    ("source_interface", "codex_host_public_message"),
                    ("source_ref_sha256", source_digest),
                    ("source_event_ids", ",".join(str(item) for item in explicit_sources)),
                ),
                redactions=redactions,
            )
            self._participant_last_event_ids[participant_id] = event.event_id
            self._participant_message_ids[participant_id] = event.event_id
            return self._receipt(event)

    def finish(
        self,
        status: str = "completed",
        *,
        source_ref: str,
        detail: str | None = None,
    ) -> Mapping[str, object]:
        if status not in _TERMINAL_STATES:
            raise ValueError("terminal status is not supported")
        with self._lock:
            self._require_active()
            self._require_event_capacity(terminal=True)
            source_digest = _source_digest(source_ref)
            self._status = status
            self._stage = status.capitalize()
            self._finished_at = utc_now_rfc3339()
            self._current_speaker = None
            for participant_id, participant_state in tuple(self._participant_states.items()):
                if participant_state == "contributed":
                    self._participant_states[participant_id] = "finished"
            if detail:
                safe_detail, _ = redact_text(detail, max_length=512)
                if status == "failed":
                    self._error = safe_detail
                else:
                    self._warning = safe_detail
            event = self._publish_locked(
                f"orchestration.run.{status}",
                f"Codex conference {status}",
                status=status,
                importance=(
                    EventImportance.IMPORTANT
                    if status == "completed"
                    else EventImportance.CRITICAL
                ),
                correlation_id=self._conference_id,
                causation_event_id=self._latest_event_id(),
                provenance=(
                    ("source_interface", "codex_plugin_bridge"),
                    ("source_ref_sha256", source_digest),
                ),
            )
            return self._receipt(event)

    def snapshot(self) -> Mapping[str, object]:
        with self._lock:
            return MappingProxyType(
                {
                    "conference_id": str(self._conference_id) if self._conference_id else None,
                    "bridge_protocol_version": CODEX_BRIDGE_PROTOCOL_VERSION,
                    "status": self._status,
                    "provider": self._provider_name,
                    "mode": "codex",
                    "objective": self._objective,
                    "stage": self._stage,
                    "current_speaker": self._current_speaker,
                    "warning": self._warning,
                    "error": self._error,
                    "started_at": self._started_at,
                    "finished_at": self._finished_at,
                    "participants": [
                        {
                            "participant_id": item.participant_id,
                            "display_name": item.display_name,
                            "responsibility": item.responsibility,
                            "state": self._participant_states[item.participant_id],
                        }
                        for item in DEFAULT_PARTICIPANTS
                    ],
                }
            )

    def _require_active(self) -> None:
        if self._status not in {"queued", "running", "cancelling"} or self._conference_id is None:
            raise ValueError("no active Codex conference")

    def _require_participant(self, participant_id: str) -> None:
        if participant_id not in self._participant_states:
            raise ValueError("unknown conference participant")

    def _validate_participant_transition(
        self, participant_id: str, state: str
    ) -> None:
        current = self._participant_states[participant_id]
        if state not in _PARTICIPANT_TRANSITIONS[current]:
            raise ValueError(
                f"participant state transition {current!r} to {state!r} is not allowed"
            )

    def _receipt(self, event: NormalizedEvent) -> Mapping[str, object]:
        """Return public state plus the accepted event identity for causal feeding."""
        return MappingProxyType({**dict(self.snapshot()), "accepted_event_id": str(event.event_id)})

    def _validated_source_event_ids(
        self, source_event_ids: tuple[UUID | str, ...]
    ) -> tuple[UUID, ...]:
        if len(source_event_ids) > 16:
            raise ValueError("source_event_ids cannot contain more than 16 entries")
        validated: list[UUID] = []
        for value in source_event_ids:
            event_id = _event_uuid(value, "source_event_id")
            if event_id not in self._accepted_events:
                raise ValueError("source_event_id is not an accepted event in this conference")
            if event_id not in validated:
                validated.append(event_id)
        return tuple(validated)

    def _validated_message_sources(
        self,
        participant_id: str,
        source_event_ids: tuple[UUID | str, ...],
    ) -> tuple[UUID, ...]:
        validated = self._validated_source_event_ids(source_event_ids)
        expected_roles = _UPSTREAM_ROLES[participant_id]
        expected = tuple(
            self._participant_message_ids[role]
            for role in expected_roles
            if role in self._participant_message_ids
        )
        if participant_id == "Reviewer" and not expected:
            raise ValueError(
                "Reviewer requires at least one Researcher or Builder message"
            )
        if participant_id == "Moderator" and not expected:
            raise ValueError("Moderator requires at least one participant message")
        if len(validated) != len(expected) or set(validated) != set(expected):
            raise ValueError(
                f"{participant_id} source_event_ids do not exactly match available upstream messages"
            )
        for event_id in validated:
            event = self._accepted_events[event_id]
            speaker_id = dict(event.attributes).get("speaker_id")
            if event.event_type != "message.sent" or speaker_id not in expected_roles:
                raise ValueError(
                    "source_event_ids must identify prior messages from expected roles"
                )
        return validated

    def _validated_message_cause(
        self, participant_id: str, value: UUID | str | None
    ) -> UUID:
        work_event_id = self._participant_work_event_ids.get(participant_id)
        if (
            work_event_id is None
            or self._participant_states[participant_id] != "working"
        ):
            raise ValueError(
                "participant must have an accepted working event before publishing"
            )
        event_id = (
            work_event_id
            if value is None
            else self._validated_causation_event_id(value)
        )
        if event_id != work_event_id:
            raise ValueError(
                "causation_event_id must be the participant's accepted working event"
            )
        cause = self._accepted_events[event_id]
        if (
            cause.event_type != "task.started"
            or cause.worker_id != participant_id
            or cause.correlation_id
            != self._participant_correlations[participant_id]
            or cause.task_id != self._participant_tasks[participant_id]
            or cause.agent_path != (participant_id,)
        ):
            raise ValueError("message causation does not match participant lifecycle")
        return event_id

    def _validated_causation_event_id(
        self, value: UUID | str | None
    ) -> UUID | None:
        if value is None:
            return None
        event_id = _event_uuid(value, "causation_event_id")
        if event_id not in self._accepted_events:
            raise ValueError("causation_event_id is not an accepted event in this conference")
        return event_id

    def _latest_event_id(self) -> UUID | None:
        return next(reversed(self._accepted_events), self._run_started_event_id)

    def _require_event_capacity(self, *, terminal: bool = False) -> None:
        limit = _MAX_ACCEPTED_EVENTS if terminal else _MAX_ACCEPTED_EVENTS - 1
        if len(self._accepted_events) >= limit:
            raise ValueError("conference event capacity reached")

    def _publish_locked(
        self,
        event_type: str,
        summary: str,
        *,
        worker_id: str | None = None,
        correlation_id: UUID | None = None,
        causation_event_id: UUID | None = None,
        task_id: UUID | None = None,
        agent_path: tuple[str, ...] = (),
        status: str | None = None,
        importance: EventImportance = EventImportance.ROUTINE,
        attributes: tuple[tuple[str, object], ...] = (),
        provenance: tuple[tuple[str, object], ...] = (),
        redactions: tuple[tuple[str, object], ...] = (),
    ) -> NormalizedEvent:
        if self._conference_id is None:
            raise RuntimeError("conference identity is unavailable")
        terminal_event = (
            event_type.startswith("orchestration.run.")
            and event_type != "orchestration.run.started"
        )
        self._require_event_capacity(terminal=terminal_event)
        self._sequence += 1
        event = NormalizedEvent(
                event_type=event_type,
                session_id=self._session_id,
                sequence=self._sequence,
                summary=summary,
                origin=EventOrigin.ADAPTER,
                evidence=EvidenceKind.OBSERVED,
                importance=importance,
                run_id=self._conference_id,
                worker_id=worker_id,
                correlation_id=correlation_id,
                causation_event_id=causation_event_id,
                task_id=task_id,
                agent_path=agent_path,
                status=status,
                attributes=attributes,
                provenance=provenance,
                redactions=redactions,
            )
        self._accepted_events[event.event_id] = event
        self._event_bus.publish(event)
        return event


def _source_digest(source_ref: str) -> str:
    if not isinstance(source_ref, str) or not source_ref.strip() or len(source_ref) > 512:
        raise ValueError("source_ref must be a bounded non-blank string")
    return hashlib.sha256(source_ref.encode("utf-8")).hexdigest()[:24]


def _event_uuid(value: UUID | str, field: str) -> UUID:
    try:
        return value if isinstance(value, UUID) else UUID(value)
    except (AttributeError, TypeError, ValueError):
        raise ValueError(f"{field} must be an RFC 9562 UUID") from None


def _merge_redactions(
    first: tuple[tuple[str, object], ...],
    second: tuple[tuple[str, object], ...],
) -> tuple[tuple[str, object], ...]:
    left = dict(first)
    right = dict(second)
    kinds = {
        kind
        for value in (left.get("redaction_kinds"), right.get("redaction_kinds"))
        if isinstance(value, str)
        for kind in value.split(",")
        if kind
    }
    return (
        ("redacted", bool(left.get("redacted")) or bool(right.get("redacted"))),
        (
            "redaction_count",
            int(left.get("redaction_count", 0)) + int(right.get("redaction_count", 0)),
        ),
        ("redaction_kinds", ",".join(sorted(kinds))),
        ("truncated", bool(left.get("truncated")) or bool(right.get("truncated"))),
    )
