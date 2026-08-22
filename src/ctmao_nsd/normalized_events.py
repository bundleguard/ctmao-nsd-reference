"""Provider-neutral, immutable observability events and strict JSON codecs.

The normalized protocol deliberately carries lifecycle facts, not prompts or
private agent reasoning.  Its serialized form is a small RFC 8259 JSON object
whose timestamps use the UTC ``Z`` form of RFC 3339.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping, TypeAlias
from uuid import UUID, uuid4

SCHEMA_VERSION = "1.0"
MAX_EVENT_BYTES = 65_536
MAX_SUMMARY_CHARS = 4_096
MAX_METADATA_ENTRIES = 64
MAX_METADATA_CHARS = 16_384

JsonScalar: TypeAlias = str | int | float | bool | None
Metadata: TypeAlias = tuple[tuple[str, JsonScalar], ...]

_RFC3339_UTC = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<time>\d{2}:\d{2}:\d{2})(?P<fraction>\.\d{1,6})?Z$"
)
_EVENT_TYPE = re.compile(
    r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$"
)
_METADATA_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,127}$")


class EventOrigin(str, Enum):
    """Component that emitted an event or deterministic projection."""

    WORKER = "worker"
    ORCHESTRATOR = "orchestrator"
    PROJECTION = "projection"
    ADAPTER = "adapter"


class EvidenceKind(str, Enum):
    """Whether an event was directly observed or deterministically derived."""

    OBSERVED = "observed"
    DERIVED = "derived"


class EventImportance(str, Enum):
    """Provider-neutral presentation importance."""

    DEBUG = "debug"
    ROUTINE = "routine"
    IMPORTANT = "important"
    CRITICAL = "critical"


def utc_now_rfc3339() -> str:
    """Return the current UTC time in RFC 3339 millisecond ``Z`` form."""

    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def validate_rfc3339_utc(value: str) -> str:
    """Validate and return a strict RFC 3339 UTC timestamp.

    Offsets equivalent to UTC are intentionally rejected so the wire format has
    one canonical representation.  Leap seconds are also rejected because
    ``datetime`` cannot represent them reliably.
    """

    if not isinstance(value, str) or not _RFC3339_UTC.fullmatch(value):
        raise ValueError("timestamp must be RFC 3339 UTC using the Z suffix")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("timestamp is not a valid calendar instant") from exc
    if parsed.tzinfo != timezone.utc:
        raise ValueError("timestamp must identify UTC")
    return value


_SUPPORTED_UUID_VERSIONS = frozenset((4, 5, 7))


def _validate_uuid(value: UUID, field_name: str) -> None:
    if not isinstance(value, UUID) or value.version not in _SUPPORTED_UUID_VERSIONS:
        raise ValueError(f"{field_name} must be an RFC 9562 UUIDv4, UUIDv5, or UUIDv7")


def _validate_optional_uuid(value: UUID | None, field_name: str) -> None:
    if value is not None:
        _validate_uuid(value, field_name)


def _validate_scalar(value: object, field_name: str) -> None:
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    raise ValueError(f"{field_name} must contain only finite RFC 8259 scalars")


def _normalize_metadata(value: Metadata, field_name: str) -> Metadata:
    if not isinstance(value, tuple):
        raise ValueError(f"{field_name} must be an immutable tuple")
    if len(value) > MAX_METADATA_ENTRIES:
        raise ValueError(f"{field_name} has too many entries")
    normalized: list[tuple[str, JsonScalar]] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(f"{field_name} entries must be two-item tuples")
        key, scalar = item
        if not isinstance(key, str) or not _METADATA_KEY.fullmatch(key):
            raise ValueError(f"{field_name} contains an invalid key")
        if key in seen:
            raise ValueError(f"{field_name} contains duplicate key {key!r}")
        _validate_scalar(scalar, field_name)
        if isinstance(scalar, str) and len(scalar) > MAX_SUMMARY_CHARS:
            raise ValueError(f"{field_name} contains an oversized string")
        seen.add(key)
        normalized.append((key, scalar))
    normalized.sort(key=lambda pair: pair[0])
    result = tuple(normalized)
    encoded = json.dumps(
        dict(result), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    if len(encoded.encode("utf-8")) > MAX_METADATA_CHARS:
        raise ValueError(f"{field_name} is too large")
    return result


def _optional_text(value: str | None, field_name: str, maximum: int = 256) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{field_name} must be a non-empty bounded string")


@dataclass(frozen=True, slots=True)
class NormalizedEvent:
    """Immutable schema-v1 event suitable for persistence and projection."""

    event_type: str
    session_id: UUID
    sequence: int
    summary: str
    origin: EventOrigin
    evidence: EvidenceKind
    importance: EventImportance = EventImportance.ROUTINE
    run_id: UUID | None = None
    worker_id: str | None = None
    correlation_id: UUID | None = None
    task_id: UUID | None = None
    parent_task_id: UUID | None = None
    causation_event_id: UUID | None = None
    agent_path: tuple[str, ...] = ()
    status: str | None = None
    attributes: Metadata = ()
    provenance: Metadata = ()
    redactions: Metadata = ()
    occurred_at: str = field(default_factory=utc_now_rfc3339)
    event_id: UUID = field(default_factory=uuid4)
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"unsupported schema version {self.schema_version!r}")
        if not isinstance(self.event_type, str) or not _EVENT_TYPE.fullmatch(
            self.event_type
        ):
            raise ValueError("event_type must be a lowercase namespaced identifier")
        if len(self.event_type) > 128:
            raise ValueError("event_type is too long")
        _validate_uuid(self.event_id, "event_id")
        _validate_uuid(self.session_id, "session_id")
        for name in (
            "run_id",
            "correlation_id",
            "task_id",
            "parent_task_id",
            "causation_event_id",
        ):
            _validate_optional_uuid(getattr(self, name), name)
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int):
            raise ValueError("sequence must be an integer")
        if self.sequence < 1:
            raise ValueError("sequence must be positive")
        if not isinstance(self.summary, str) or len(self.summary) > MAX_SUMMARY_CHARS:
            raise ValueError("summary must be a bounded string")
        if not isinstance(self.origin, EventOrigin):
            raise ValueError("origin must be an EventOrigin")
        if not isinstance(self.evidence, EvidenceKind):
            raise ValueError("evidence must be an EvidenceKind")
        if not isinstance(self.importance, EventImportance):
            raise ValueError("importance must be an EventImportance")
        _optional_text(self.worker_id, "worker_id")
        _optional_text(self.status, "status")
        if not isinstance(self.agent_path, tuple) or len(self.agent_path) > 64:
            raise ValueError("agent_path must be a bounded immutable tuple")
        for component in self.agent_path:
            _optional_text(component, "agent_path component")
        validate_rfc3339_utc(self.occurred_at)
        object.__setattr__(
            self, "attributes", _normalize_metadata(self.attributes, "attributes")
        )
        object.__setattr__(
            self, "provenance", _normalize_metadata(self.provenance, "provenance")
        )
        object.__setattr__(
            self, "redactions", _normalize_metadata(self.redactions, "redactions")
        )
        encoded = json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > MAX_EVENT_BYTES:
            raise ValueError("serialized event exceeds the size limit")

    def to_dict(self) -> dict[str, object]:
        """Return the explicit RFC 8259 wire representation."""

        return {
            "agent_path": list(self.agent_path),
            "attributes": dict(self.attributes),
            "causation_event_id": _uuid_text(self.causation_event_id),
            "correlation_id": _uuid_text(self.correlation_id),
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "evidence": self.evidence.value,
            "importance": self.importance.value,
            "occurred_at": self.occurred_at,
            "origin": self.origin.value,
            "parent_task_id": _uuid_text(self.parent_task_id),
            "provenance": dict(self.provenance),
            "redactions": dict(self.redactions),
            "run_id": _uuid_text(self.run_id),
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "session_id": str(self.session_id),
            "status": self.status,
            "summary": self.summary,
            "task_id": _uuid_text(self.task_id),
            "worker_id": self.worker_id,
        }

    def to_json(self) -> str:
        """Serialize deterministically without non-finite JSON extensions."""

        encoded = json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if len(encoded.encode("utf-8")) > MAX_EVENT_BYTES:
            raise ValueError("serialized event exceeds the size limit")
        return encoded

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "NormalizedEvent":
        """Parse a strict schema-v1 mapping, rejecting missing or extra keys."""

        if not isinstance(value, Mapping):
            raise ValueError("event must be a mapping")
        expected = set(_WIRE_FIELDS)
        supplied = set(value)
        if supplied != expected:
            missing = sorted(expected - supplied)
            extra = sorted(supplied - expected)
            raise ValueError(f"invalid event fields; missing={missing}, extra={extra}")
        if value["schema_version"] != SCHEMA_VERSION:
            raise ValueError("unsupported schema version")
        return cls(
            event_type=_required_text(value["event_type"], "event_type"),
            session_id=_parse_uuid(value["session_id"], "session_id"),
            sequence=_required_int(value["sequence"], "sequence"),
            summary=_required_text(value["summary"], "summary", allow_empty=True),
            origin=_parse_enum(EventOrigin, value["origin"], "origin"),
            evidence=_parse_enum(EvidenceKind, value["evidence"], "evidence"),
            importance=_parse_enum(
                EventImportance, value["importance"], "importance"
            ),
            run_id=_parse_optional_uuid(value["run_id"], "run_id"),
            worker_id=_parse_optional_text(value["worker_id"], "worker_id"),
            correlation_id=_parse_optional_uuid(
                value["correlation_id"], "correlation_id"
            ),
            task_id=_parse_optional_uuid(value["task_id"], "task_id"),
            parent_task_id=_parse_optional_uuid(
                value["parent_task_id"], "parent_task_id"
            ),
            causation_event_id=_parse_optional_uuid(
                value["causation_event_id"], "causation_event_id"
            ),
            agent_path=_parse_agent_path(value["agent_path"]),
            status=_parse_optional_text(value["status"], "status"),
            attributes=_parse_metadata(value["attributes"], "attributes"),
            provenance=_parse_metadata(value["provenance"], "provenance"),
            redactions=_parse_metadata(value["redactions"], "redactions"),
            occurred_at=_required_text(value["occurred_at"], "occurred_at"),
            event_id=_parse_uuid(value["event_id"], "event_id"),
            schema_version=SCHEMA_VERSION,
        )

    @classmethod
    def from_json(cls, value: str | bytes | bytearray) -> "NormalizedEvent":
        """Parse strict UTF-8 JSON with duplicate-key and size checks."""

        if isinstance(value, str):
            try:
                raw = value.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError("event is not valid RFC 8259 JSON") from exc
        elif isinstance(value, (bytes, bytearray)):
            raw = bytes(value)
        else:
            raise ValueError("JSON input must be text or bytes")
        if len(raw) > MAX_EVENT_BYTES:
            raise ValueError("serialized event exceeds the size limit")
        try:
            text = raw.decode("utf-8")
            parsed = json.loads(
                text,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_json_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("event is not valid RFC 8259 JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError("event JSON must contain one object")
        return cls.from_dict(parsed)


def _uuid_text(value: UUID | None) -> str | None:
    return str(value) if value is not None else None


def _parse_uuid(value: object, field_name: str) -> UUID:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a UUID string")
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"{field_name} must be a UUID string") from exc
    if str(parsed) != value or parsed.version not in _SUPPORTED_UUID_VERSIONS:
        raise ValueError(
            f"{field_name} must be a canonical RFC 9562 UUIDv4, UUIDv5, or UUIDv7 string"
        )
    return parsed


def _parse_optional_uuid(value: object, field_name: str) -> UUID | None:
    return None if value is None else _parse_uuid(value, field_name)


def _required_text(
    value: object, field_name: str, *, allow_empty: bool = False
) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ValueError(f"{field_name} must be a string")
    return value


def _parse_optional_text(value: object, field_name: str) -> str | None:
    return None if value is None else _required_text(value, field_name)


def _required_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _parse_enum(enum_type: type[Enum], value: object, field_name: str) -> Any:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} has an unknown value") from exc


def _parse_agent_path(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError("agent_path must be an array of strings")
    return tuple(value)


def _parse_metadata(value: object, field_name: str) -> Metadata:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return tuple((key, scalar) for key, scalar in value.items())


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value!r} is forbidden")


_WIRE_FIELDS = (
    "agent_path",
    "attributes",
    "causation_event_id",
    "correlation_id",
    "event_id",
    "event_type",
    "evidence",
    "importance",
    "occurred_at",
    "origin",
    "parent_task_id",
    "provenance",
    "redactions",
    "run_id",
    "schema_version",
    "sequence",
    "session_id",
    "status",
    "summary",
    "task_id",
    "worker_id",
)


_SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "assignment",
        re.compile(
            r"(?i)\b(api[_-]?key|access[_-]?token|auth[_-]?token|client[_-]?secret|"
            r"password|passwd|secret|token)\b(\s*[:=]\s*)([^\s,;]+)"
        ),
    ),
    ("bearer", re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")),
    (
        "github_token",
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    ),
    ("openai_token", re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}\b")),
)


def redact_text(value: str, *, max_length: int = MAX_SUMMARY_CHARS) -> tuple[str, Metadata]:
    """Redact common credential forms and return non-sensitive metadata.

    Any invalid input, invalid policy value, or unexpected redaction failure is
    handled fail-closed by replacing the entire value.
    """

    try:
        if not isinstance(value, str) or isinstance(max_length, bool):
            raise ValueError("invalid redaction input")
        if not isinstance(max_length, int) or max_length < 16:
            raise ValueError("invalid redaction limit")
        result = value
        counts: dict[str, int] = {}
        for kind, pattern in _SECRET_PATTERNS:
            if kind == "assignment":
                result, count = pattern.subn(r"\1\2[REDACTED]", result)
            else:
                result, count = pattern.subn("[REDACTED]", result)
            if count:
                counts[kind] = count
        truncated = len(result) > max_length
        if truncated:
            result = result[: max_length - 1] + "…"
        metadata: Metadata = (
            ("redacted", bool(counts)),
            ("redaction_count", sum(counts.values())),
            ("redaction_kinds", ",".join(sorted(counts))),
            ("truncated", truncated),
        )
        return result, metadata
    except Exception:
        return "[REDACTED]", (
            ("redacted", True),
            ("redaction_count", 1),
            ("redaction_kinds", "fail_closed"),
            ("truncated", False),
        )
