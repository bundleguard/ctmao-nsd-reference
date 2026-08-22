"""Strict host-fed adapters for externally observed runtime records.

The Codex adapter consumes snapshots supplied by a supported host integration;
it does not scrape Codex storage, invoke private APIs, or infer hidden state.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Protocol, runtime_checkable
from uuid import UUID, NAMESPACE_URL, uuid5

from .normalized_events import (
    EventImportance,
    EventOrigin,
    EvidenceKind,
    NormalizedEvent,
    redact_text,
)

MAX_SNAPSHOT_BYTES = 2_000_000
MAX_TURNS = 100
MAX_ITEMS_PER_TURN = 1_000
MAX_JSONL_RECORD_BYTES = 65_536
ADAPTER_VERSION = "1.0"


@dataclass(frozen=True, slots=True)
class AdapterResult:
    """One immutable conversion result with sanitized accounting only."""

    events: tuple[NormalizedEvent, ...]
    skipped_items: int = 0
    duplicate_items: int = 0
    conflicting_items: int = 0


@runtime_checkable
class ExternalRuntimeAdapter(Protocol):
    """Provider-neutral contract for converting an observed host snapshot."""

    def adapt(self, snapshot: str | bytes | Mapping[str, object]) -> AdapterResult:
        """Validate and convert a bounded snapshot without external I/O."""
        ...


class CodexThreadSnapshotAdapter:
    """Convert Codex host task snapshots into normalized observed events.

    Supported input is the schema-v1 object returned by a host's documented
    task-reading capability. Message bodies are excluded unless explicitly
    enabled. Reasoning records are always ignored.
    """

    def __init__(self, *, include_message_text: bool = False) -> None:
        self.include_message_text = include_message_text
        self._source_digests: dict[str, str] = {}
        self._sequence_by_session: dict[UUID, int] = {}

    def reset(self) -> None:
        """Forget replay state when the host intentionally starts a new feed."""
        self._source_digests.clear()
        self._sequence_by_session.clear()

    def adapt(self, snapshot: str | bytes | Mapping[str, object]) -> AdapterResult:
        root = _snapshot_object(snapshot)
        if root.get("schemaVersion") != 1:
            raise ValueError("unsupported Codex snapshot schemaVersion")
        thread = _mapping(root.get("thread"), "thread")
        if thread.get("kind") != "codex":
            raise ValueError("snapshot thread kind must be codex")
        thread_id = _bounded_text(thread.get("id"), "thread.id", 256)
        session_id = uuid5(NAMESPACE_URL, f"codex-thread:{thread_id}")
        turns = root.get("turns")
        if not isinstance(turns, list) or len(turns) > MAX_TURNS:
            raise ValueError("turns must be a bounded list")

        ordered_turns = sorted(
            (_mapping(turn, "turn") for turn in turns),
            key=lambda turn: (
                _unix_seconds(turn.get("startedAt"), "turn.startedAt"),
                _bounded_text(turn.get("id"), "turn.id", 256),
            ),
        )
        emitted: list[NormalizedEvent] = []
        skipped = 0
        duplicates = 0
        conflicts = 0
        observed_at = int(datetime.now(timezone.utc).timestamp())
        for turn in ordered_turns:
            converted, turn_skipped, turn_duplicates, turn_conflicts = self._adapt_turn(
                turn, session_id, thread_id, observed_at
            )
            emitted.extend(converted)
            skipped += turn_skipped
            duplicates += turn_duplicates
            conflicts += turn_conflicts
        return AdapterResult(tuple(emitted), skipped, duplicates, conflicts)

    def _adapt_turn(
        self,
        turn: Mapping[str, object],
        session_id: UUID,
        thread_id: str,
        observed_at: int,
    ) -> tuple[list[NormalizedEvent], int, int, int]:
        turn_id = _bounded_text(turn.get("id"), "turn.id", 256)
        run_id = uuid5(session_id, f"codex-turn:{turn_id}")
        started = _unix_seconds(turn.get("startedAt"), "turn.startedAt")
        status = _bounded_text(turn.get("status"), "turn.status", 64)
        items = turn.get("items")
        if not isinstance(items, list) or len(items) > MAX_ITEMS_PER_TURN:
            raise ValueError("turn.items must be a bounded list")

        events: list[NormalizedEvent] = []
        duplicates = 0
        conflicts = 0
        skipped = 0
        start_event = self._event(
            session_id,
            run_id,
            turn_id,
            "turn-start",
            "task.started",
            "Codex task started.",
            started,
            task_id=run_id,
            status="started",
            importance=EventImportance.IMPORTANT,
            agent_path=("codex",),
            thread_id=thread_id,
            timestamp_basis="source",
        )
        if start_event is _CONFLICT:
            conflicts += 1
        elif start_event is None:
            duplicates += 1
        else:
            events.append(start_event)

        for index, raw_item in enumerate(items):
            item = _mapping(raw_item, "turn item")
            item_type = item.get("type")
            if item_type == "reasoning":
                skipped += 1
                continue
            mapped = self._adapt_item(
                item,
                index=index,
                session_id=session_id,
                run_id=run_id,
                turn_id=turn_id,
                thread_id=thread_id,
                occurred_at=observed_at,
            )
            if mapped is None:
                skipped += 1
            elif mapped is _DUPLICATE:
                duplicates += 1
            elif mapped is _CONFLICT:
                conflicts += 1
            else:
                events.append(mapped)

        terminal_type, terminal_status, importance = _terminal_mapping(status)
        completed_value = turn.get("completedAt")
        completed = (
            _unix_seconds(completed_value, "turn.completedAt")
            if completed_value is not None
            else observed_at
        )
        terminal = self._event(
            session_id,
            run_id,
            turn_id,
            f"turn-terminal:{terminal_status}",
            terminal_type,
            f"Codex task {terminal_status}.",
            completed,
            task_id=run_id,
            status=terminal_status,
            importance=importance,
            agent_path=("codex",),
            thread_id=thread_id,
            timestamp_basis="source" if completed_value is not None else "adapter_observed",
        )
        if terminal is _CONFLICT:
            conflicts += 1
        elif terminal is None:
            duplicates += 1
        else:
            events.append(terminal)
        return events, skipped, duplicates, conflicts

    def _adapt_item(
        self,
        item: Mapping[str, object],
        *,
        index: int,
        session_id: UUID,
        run_id: UUID,
        turn_id: str,
        thread_id: str,
        occurred_at: int,
    ) -> NormalizedEvent | object | None:
        item_type = item.get("type")
        item_id = item.get("id")
        source_id = (
            _bounded_text(item_id, "item.id", 256)
            if item_id is not None
            else f"index-{index}"
        )
        event_type: str
        summary: str
        agent_path: tuple[str, ...]
        status: str | None = None
        importance = EventImportance.ROUTINE
        attributes: tuple[tuple[str, str | int | float | bool | None], ...] = ()

        if item_type in ("userMessage", "agentMessage"):
            is_agent = item_type == "agentMessage"
            event_type = "message.sent" if is_agent else "message.received"
            agent_path = ("codex",) if is_agent else ("user",)
            text = _message_text(item)
            if self.include_message_text and text:
                summary = text
            else:
                summary = "Codex agent message observed." if is_agent else "User message observed."
            phase = item.get("phase")
            if isinstance(phase, str) and phase:
                attributes = (("phase", phase[:256]),)
        elif item_type == "fileChange":
            agent_path = ("codex",)
            status = _optional_status(item.get("status")) or "completed"
            suffix = _tool_phase(status, default="completed")
            event_type = f"tool.{suffix}"
            if suffix in ("failed", "cancelled"):
                importance = EventImportance.IMPORTANT
            changes = item.get("changes")
            count = len(changes) if isinstance(changes, list) else 0
            summary = f"Codex file change {suffix}; {count} change(s) reported."
            attributes = (("change_count", count), ("tool", "workspace.file_change"))
        elif item_type == "mcpToolCall":
            tool = _bounded_text(item.get("tool"), "tool", 256)
            server = _bounded_text(item.get("server"), "server", 256)
            status = _optional_status(item.get("status")) or "observed"
            suffix = _tool_phase(status, default="observed")
            event_type = f"tool.{suffix}"
            importance = (
                EventImportance.IMPORTANT if suffix == "failed" else EventImportance.ROUTINE
            )
            agent_path = ("codex",)
            summary = f"Codex tool {tool} {suffix}."
            attributes = (("server", server), ("tool", tool))
        else:
            return None

        event = self._event(
            session_id,
            run_id,
            turn_id,
            source_id,
            event_type,
            summary,
            occurred_at,
            task_id=run_id,
            status=status,
            importance=importance,
            agent_path=agent_path,
            attributes=attributes,
            thread_id=thread_id,
            timestamp_basis="adapter_observed",
        )
        if event is _CONFLICT:
            return _CONFLICT
        return event if event is not None else _DUPLICATE

    def _event(
        self,
        session_id: UUID,
        run_id: UUID,
        turn_id: str,
        source_id: str,
        event_type: str,
        summary: str,
        timestamp: int,
        *,
        task_id: UUID,
        status: str | None,
        importance: EventImportance,
        agent_path: tuple[str, ...],
        thread_id: str,
        timestamp_basis: str,
        attributes: tuple[tuple[str, str | int | float | bool | None], ...] = (),
    ) -> NormalizedEvent | object | None:
        source_key = f"{thread_id}\0{turn_id}\0{source_id}\0{event_type}"
        safe_summary, redactions = redact_text(summary[:4096])
        safe_status = redact_text(status[:256])[0] if status is not None else None
        safe_attributes = tuple(
            (key, redact_text(value[:4096])[0] if isinstance(value, str) else value)
            for key, value in attributes
        )
        if (
            (safe_status != status or safe_attributes != attributes)
            and not dict(redactions).get("redacted")
        ):
            redactions = (("redacted", True), ("redaction_rule", "structured-field"))
        semantic_digest = _semantic_digest(
            event_type, safe_summary, safe_status, safe_attributes
        )
        previous = self._source_digests.get(source_key)
        if previous is not None:
            return None if previous == semantic_digest else _CONFLICT
        sequence = self._sequence_by_session.get(session_id, 0) + 1
        event = NormalizedEvent(
            event_type=event_type,
            session_id=session_id,
            sequence=sequence,
            summary=safe_summary,
            origin=EventOrigin.ADAPTER,
            evidence=EvidenceKind.OBSERVED,
            importance=importance,
            run_id=run_id,
            task_id=task_id,
            agent_path=agent_path,
            status=safe_status,
            attributes=safe_attributes,
            provenance=(
                ("adapter", "codex.thread_snapshot"),
                ("adapter_version", ADAPTER_VERSION),
                ("source_digest", sha256(source_key.encode("utf-8")).hexdigest()),
                ("timestamp_basis", timestamp_basis),
            ),
            redactions=redactions,
            occurred_at=_rfc3339(timestamp),
            event_id=uuid5(session_id, source_key),
        )
        self._source_digests[source_key] = semantic_digest
        self._sequence_by_session[session_id] = sequence
        return event


class CodexExecJsonlAdapter:
    """Normalize currently observed ``codex exec --json`` record shapes.

    This parser owns no subprocess and no credentials. A host launches Codex,
    keeps stderr separate, and passes complete stdout JSONL records here. The
    host must pin or probe a compatible CLI version; this prototype does not
    claim a versioned external-schema guarantee.
    """

    def __init__(self, *, include_message_text: bool = False) -> None:
        self.include_message_text = include_message_text
        self._thread_source_id: str | None = None
        self._session_id: UUID | None = None
        self._run_id: UUID | None = None
        self._turn_number = 0
        self._sequence = 0
        self._source_digests: dict[str, str] = {}

    def reset(self) -> None:
        """Forget the active stream and all bounded-lifetime replay state."""
        self._thread_source_id = None
        self._session_id = None
        self._run_id = None
        self._turn_number = 0
        self._sequence = 0
        self._source_digests.clear()

    def ingest_line(self, line: str | bytes) -> AdapterResult:
        record = _jsonl_record(line)
        record_type = _bounded_text(record.get("type"), "type", 128)
        if record_type == "thread.started":
            thread_id = _bounded_text(record.get("thread_id"), "thread_id", 256)
            if thread_id != self._thread_source_id:
                self._run_id = None
                self._turn_number = 0
                self._sequence = 0
                self._source_digests.clear()
            self._thread_source_id = thread_id
            self._session_id = uuid5(NAMESPACE_URL, f"codex-exec-thread:{thread_id}")
            return AdapterResult(())
        if self._session_id is None or self._thread_source_id is None:
            raise ValueError("thread.started must precede Codex exec events")
        if record_type == "turn.started":
            if self._run_id is not None:
                raise ValueError("a Codex turn is already active")
            self._turn_number += 1
            self._run_id = uuid5(self._session_id, f"turn:{self._turn_number}")
            event = self._record_event(
                f"turn:{self._turn_number}:started",
                "task.started",
                "Codex turn started.",
                status="started",
                importance=EventImportance.IMPORTANT,
            )
            return _single_event_result(event)
        if self._run_id is None:
            raise ValueError("turn.started must precede turn item or terminal events")
        if record_type in ("turn.completed", "turn.failed"):
            terminal = "completed" if record_type.endswith("completed") else "failed"
            event = self._record_event(
                f"turn:{self._turn_number}:{terminal}",
                f"task.{terminal}",
                f"Codex turn {terminal}.",
                status=terminal,
                importance=(
                    EventImportance.IMPORTANT
                    if terminal == "completed"
                    else EventImportance.CRITICAL
                ),
            )
            result = _single_event_result(event)
            if event is not None and event is not _CONFLICT:
                self._run_id = None
            return result
        if record_type in ("item.started", "item.completed"):
            item = _mapping(record.get("item"), "item")
            item_type = _bounded_text(item.get("type"), "item.type", 128)
            if item_type == "reasoning":
                return AdapterResult((), skipped_items=1)
            event = self._exec_item(record_type, item)
            if event is _DUPLICATE:
                return AdapterResult((), duplicate_items=1)
            if event is _CONFLICT:
                return AdapterResult((), conflicting_items=1)
            return AdapterResult((event,) if isinstance(event, NormalizedEvent) else (), skipped_items=event is None)
        # Error and unknown extension records are not execution facts. Hosts can
        # surface them through a separate sanitized adapter-health channel.
        return AdapterResult((), skipped_items=1)

    def _exec_item(
        self, lifecycle: str, item: Mapping[str, object]
    ) -> NormalizedEvent | object | None:
        item_id = _bounded_text(item.get("id"), "item.id", 256)
        item_type = _bounded_text(item.get("type"), "item.type", 128)
        completed = lifecycle == "item.completed"
        if item_type == "agent_message":
            if not completed:
                return None
            text = item.get("text")
            summary = (
                text[:4096]
                if self.include_message_text and isinstance(text, str) and text
                else "Codex agent message observed."
            )
            event_type = "message.sent"
            status = "completed"
            attributes = ()
        elif item_type in (
            "command_execution",
            "mcp_tool_call",
            "file_change",
            "web_search",
        ):
            explicit_status = _optional_status(item.get("status"))
            phase = _tool_phase(
                explicit_status,
                default="completed" if completed else "started",
            )
            event_type = f"tool.{phase}"
            status = phase
            summary = f"Codex {item_type.replace('_', ' ')} {phase}."
            attributes = (("tool_kind", item_type),)
        else:
            return None
        event = self._record_event(
            f"item:{item_id}:{lifecycle}",
            event_type,
            summary,
            status=status,
            importance=EventImportance.ROUTINE,
            attributes=attributes,
        )
        if event is _CONFLICT:
            return _CONFLICT
        return event if event is not None else _DUPLICATE

    def _record_event(
        self,
        source_key: str,
        event_type: str,
        summary: str,
        *,
        status: str,
        importance: EventImportance,
        attributes: tuple[tuple[str, str | int | float | bool | None], ...] = (),
    ) -> NormalizedEvent | object | None:
        assert self._session_id is not None and self._run_id is not None
        scoped_key = f"{self._thread_source_id}\0{source_key}"
        safe_summary, redactions = redact_text(summary)
        semantic_digest = _semantic_digest(event_type, safe_summary, status, attributes)
        previous = self._source_digests.get(scoped_key)
        if previous is not None:
            return None if previous == semantic_digest else _CONFLICT
        sequence = self._sequence + 1
        event = NormalizedEvent(
            event_type=event_type,
            session_id=self._session_id,
            sequence=sequence,
            summary=safe_summary,
            origin=EventOrigin.ADAPTER,
            evidence=EvidenceKind.OBSERVED,
            importance=importance,
            run_id=self._run_id,
            task_id=self._run_id,
            agent_path=("codex",),
            status=status,
            attributes=attributes,
            provenance=(
                ("adapter", "codex.exec_jsonl"),
                ("adapter_version", ADAPTER_VERSION),
                ("source_digest", sha256(scoped_key.encode("utf-8")).hexdigest()),
            ),
            redactions=redactions,
            event_id=uuid5(self._session_id, scoped_key),
        )
        self._source_digests[scoped_key] = semantic_digest
        self._sequence = sequence
        return event


_DUPLICATE = object()
_CONFLICT = object()


def _single_event_result(event: NormalizedEvent | object | None) -> AdapterResult:
    if isinstance(event, NormalizedEvent):
        return AdapterResult((event,))
    if event is _CONFLICT:
        return AdapterResult((), conflicting_items=1)
    return AdapterResult((), duplicate_items=1)


def _semantic_digest(
    event_type: str,
    summary: str,
    status: str | None,
    attributes: tuple[tuple[str, str | int | float | bool | None], ...],
) -> str:
    encoded = json.dumps(
        [event_type, summary, status, attributes],
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _jsonl_record(value: str | bytes) -> Mapping[str, object]:
    if isinstance(value, str):
        try:
            raw = value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("Codex JSONL record is not valid UTF-8 JSON") from exc
    elif isinstance(value, bytes):
        raw = value
    else:
        raise ValueError("Codex JSONL record must be text or bytes")
    if not raw or len(raw) > MAX_JSONL_RECORD_BYTES:
        raise ValueError("Codex JSONL record exceeds the size limit")
    try:
        parsed = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Codex JSONL record is not valid UTF-8 JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Codex JSONL record must contain one object")
    return parsed


def _snapshot_object(value: str | bytes | Mapping[str, object]) -> Mapping[str, object]:
    if isinstance(value, str):
        try:
            raw = value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("snapshot is not valid JSON") from exc
    elif isinstance(value, bytes):
        raw = value
    elif isinstance(value, Mapping):
        try:
            raw = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            raise ValueError("snapshot must contain finite JSON values") from exc
    else:
        raise ValueError("snapshot must be JSON text, bytes, or a mapping")
    if len(raw) > MAX_SNAPSHOT_BYTES:
        raise ValueError("snapshot exceeds the size limit")
    if isinstance(value, Mapping):
        return value
    try:
        parsed = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("snapshot is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("snapshot JSON must contain one object")
    return parsed


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("snapshot contains a duplicate object key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value} is not supported")


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _bounded_text(value: object, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{field} must be a non-empty bounded string")
    return value


def _optional_status(value: object) -> str | None:
    return value[:64] if isinstance(value, str) and value else None


def _tool_phase(status: str | None, *, default: str) -> str:
    return {
        "completed": "completed",
        "succeeded": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "in_progress": "started",
        "inProgress": "started",
        "running": "started",
        "started": "started",
    }.get(status, default if status is None else "observed")


def _unix_seconds(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative Unix timestamp")
    try:
        datetime.fromtimestamp(value, timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError(f"{field} is outside the supported timestamp range") from exc
    return value


def _rfc3339(value: int) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def _message_text(item: Mapping[str, object]) -> str:
    if item.get("type") == "agentMessage":
        text = item.get("text")
        return text if isinstance(text, str) else ""
    content = item.get("content")
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for raw_part in content:
        if not isinstance(raw_part, Mapping) or raw_part.get("type") != "text":
            continue
        text = raw_part.get("text")
        if isinstance(text, str):
            parts.append(text)
    return "\n".join(parts)


def _terminal_mapping(status: str) -> tuple[str, str, EventImportance]:
    if status == "completed":
        return "task.completed", "completed", EventImportance.IMPORTANT
    if status in ("cancelled", "canceled"):
        return "task.cancelled", "cancelled", EventImportance.IMPORTANT
    if status in ("failed", "error"):
        return "task.failed", "failed", EventImportance.CRITICAL
    return "task.observed", status, EventImportance.ROUTINE
