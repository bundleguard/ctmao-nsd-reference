"""Deterministic projection of normalized runtime evidence into conference turns."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
import math
from types import MappingProxyType
from typing import Iterable, Mapping
from uuid import UUID

from .normalized_events import (
    EventImportance,
    EventOrigin,
    EvidenceKind,
    NormalizedEvent,
    redact_text,
)

TRANSFORMATION_NAME = "ctmao_nsd.deterministic_lifecycle_projection"
TRANSFORMATION_VERSION = "1.0"


class ConferenceMode(str, Enum):
    """Whether conference output is disabled, textual, or eligible for speech."""

    OFF = "off"
    TEXT = "text"
    SPEECH = "speech"
    BRIEFING = "briefing"


class Verbosity(str, Enum):
    """Amount of source-backed runtime activity projected for a listener."""

    MINIMAL = "minimal"
    NORMAL = "normal"
    DETAILED = "detailed"


@dataclass(frozen=True, slots=True)
class ConferenceTurn:
    """One immutable utterance derived from identified runtime evidence."""

    turn_id: str
    session_id: UUID
    speaker_id: str
    text: str
    source_event_ids: tuple[UUID, ...]
    evidence: EvidenceKind
    sequence: int
    run_id: UUID | None = None
    worker_id: str | None = None
    agent_path: tuple[str, ...] = ()
    importance: EventImportance = EventImportance.ROUTINE
    transformation_name: str = TRANSFORMATION_NAME
    transformation_version: str = TRANSFORMATION_VERSION
    lifecycle_valid: bool = True
    queued_at: str | None = None
    source_occurred_at: str | None = None
    redactions: tuple[tuple[str, str | int | float | bool | None], ...] = ()
    speech_hints: tuple[tuple[str, str | int | float | bool | None], ...] = ()
    source_evidence: EvidenceKind = EvidenceKind.OBSERVED

    def __post_init__(self) -> None:
        if not self.speaker_id.strip():
            raise ValueError("speaker_id must not be blank")
        if not self.text.strip():
            raise ValueError("conference turns must contain text")
        if not self.source_event_ids:
            raise ValueError("conference turns require observed or derived source evidence")
        if self.evidence not in (EvidenceKind.OBSERVED, EvidenceKind.DERIVED):
            raise ValueError("unsupported conference-turn evidence kind")
        if self.sequence < 0:
            raise ValueError("sequence must not be negative")
        if not self.transformation_name or not self.transformation_version:
            raise ValueError("conference turns require transformation provenance")

    @property
    def utterance_id(self) -> str:
        """Protocol spelling for the stable deterministic turn identifier."""
        return self.turn_id

    @property
    def agent_id(self) -> str:
        """Return the logical speaker identity used by transcript and speech."""
        return self.speaker_id


@dataclass(frozen=True, slots=True)
class VoiceAssignment:
    """Deterministic provider voice and exhaustion-safe prosody for one speaker."""

    speaker_id: str
    voice_id: str | None
    muted: bool = False
    exhausted: bool = False
    rate: float = 1.0
    pitch: float = 0.0

    def __post_init__(self) -> None:
        if not self.speaker_id.strip():
            raise ValueError("speaker_id must not be blank")
        if self.muted and self.voice_id is not None:
            raise ValueError("a muted assignment cannot have a voice")
        if not math.isfinite(self.rate) or not 0.5 <= self.rate <= 2.0:
            raise ValueError("voice rate must be finite and between 0.5 and 2.0")
        if not math.isfinite(self.pitch) or not -1.0 <= self.pitch <= 1.0:
            raise ValueError("voice pitch must be finite and between -1.0 and 1.0")


_MINIMAL_SUFFIXES = ("completed", "failed", "cancelled", "timed_out", "rejected")
_NORMAL_NAMESPACES = (
    "orchestration.run.",
    "runtime.worker.",
    "run.",
    "worker.",
    "task.",
    "delegation.",
    "message.",
    "tool.",
    "finding.",
)


def project_conference_turns(
    events: Iterable[NormalizedEvent],
    *,
    mode: ConferenceMode = ConferenceMode.TEXT,
    verbosity: Verbosity = Verbosity.NORMAL,
) -> tuple[ConferenceTurn, ...]:
    """Purely project normalized evidence; never invent an unsourced utterance."""
    if mode is ConferenceMode.OFF:
        return ()

    projected: list[ConferenceTurn] = []
    seen_event_ids: set[UUID] = set()
    unique: list[NormalizedEvent] = []
    for event in events:
        if event.event_id in seen_event_ids:
            continue
        seen_event_ids.add(event.event_id)
        unique.append(event)
    if len({event.session_id for event in unique}) == 1:
        unique.sort(key=lambda item: (item.sequence, str(item.event_id)))
    for event in unique:
        if not _include_event(event.event_type, verbosity):
            continue
        text, redactions = redact_text(event.summary.strip())
        if not text:
            continue
        speaker_id = _speaker_for(event)
        digest = sha256(
            f"{event.session_id}\0{event.event_id}\0{speaker_id}\0{text}".encode("utf-8")
        ).hexdigest()
        projected.append(
            ConferenceTurn(
                turn_id=digest,
                session_id=event.session_id,
                speaker_id=speaker_id,
                text=text,
                source_event_ids=(event.event_id,),
                evidence=EvidenceKind.DERIVED,
                sequence=event.sequence,
                run_id=event.run_id,
                worker_id=event.worker_id,
                agent_path=event.agent_path,
                importance=event.importance,
                source_occurred_at=event.occurred_at,
                redactions=redactions,
                source_evidence=event.evidence,
            )
        )
    return tuple(projected)


def project_message_turns(
    events: Iterable[NormalizedEvent],
) -> tuple[ConferenceTurn, ...]:
    """Project only genuine public agent messages for the human conference."""
    return project_conference_turns(
        (event for event in events if event.event_type == "message.sent"),
        mode=ConferenceMode.TEXT,
        verbosity=Verbosity.NORMAL,
    )


def project_briefing(events: Iterable[NormalizedEvent]) -> tuple[ConferenceTurn, ...]:
    """Create one deterministic source-backed run briefing from terminal evidence."""
    unique = {event.event_id: event for event in events}
    ordered = tuple(sorted(unique.values(), key=lambda item: (item.sequence, str(item.event_id))))
    if not ordered:
        return ()
    terminal = next(
        (
            event
            for event in reversed(ordered)
            if event.event_type.startswith("orchestration.run.")
            and event.event_type.rsplit(".", 1)[-1]
            in ("completed", "failed", "cancelled")
        ),
        None,
    )
    if terminal is None:
        return ()
    ordered = tuple(
        event
        for event in ordered
        if event.session_id == terminal.session_id and event.run_id == terminal.run_id
    )
    task_terminals = [
        event
        for event in ordered
        if event.event_type.rsplit(".", 1)[-1]
        in ("completed", "failed", "timed_out", "rejected")
        and event.event_type.startswith("task.")
    ]
    succeeded = sum(event.event_type.endswith(".completed") for event in task_terminals)
    failed = len(task_terminals) - succeeded
    workers = len({event.worker_id for event in ordered if event.worker_id})
    state = terminal.event_type.rsplit(".", 1)[-1]
    text = f"Run {state}. {workers} workers reported. {succeeded} tasks completed"
    text += f" and {failed} did not succeed." if failed else "."
    text, redactions = redact_text(text)
    source_ids = tuple(event.event_id for event in (*task_terminals, terminal))
    digest = sha256(
        ("briefing\0" + "\0".join(str(item) for item in source_ids)).encode("utf-8")
    ).hexdigest()
    return (
        ConferenceTurn(
            turn_id=digest,
            session_id=terminal.session_id,
            speaker_id="orchestrator",
            text=text,
            source_event_ids=source_ids,
            evidence=EvidenceKind.DERIVED,
            sequence=terminal.sequence,
            run_id=terminal.run_id,
            importance=terminal.importance,
            queued_at=terminal.occurred_at,
            redactions=redactions,
            source_evidence=(
                EvidenceKind.DERIVED
                if any(event.evidence is EvidenceKind.DERIVED for event in ordered)
                else EvidenceKind.OBSERVED
            ),
        ),
    )


def _include_event(event_type: str, verbosity: Verbosity) -> bool:
    if verbosity is Verbosity.DETAILED:
        return event_type.startswith(_NORMAL_NAMESPACES)
    if not event_type.startswith(_NORMAL_NAMESPACES):
        return False
    if verbosity is Verbosity.MINIMAL:
        terminal = event_type.rsplit(".", 1)[-1]
        return terminal in _MINIMAL_SUFFIXES
    if event_type.startswith("tool."):
        return event_type.endswith(".failed")
    # Normal mode suppresses noisy progress/debug extensions while retaining
    # the key lifecycle vocabulary required for an understandable conference.
    return not event_type.endswith((".progress", ".heartbeat", ".debug"))


def _speaker_for(event: NormalizedEvent) -> str:
    if event.event_type == "message.sent":
        speaker_id = dict(event.attributes).get("speaker_id")
        if isinstance(speaker_id, str) and speaker_id.strip():
            return speaker_id
    if event.origin in (EventOrigin.ORCHESTRATOR, EventOrigin.PROJECTION):
        return "orchestrator"
    if event.agent_path:
        return "/".join(event.agent_path)
    if event.worker_id:
        return f"worker:{event.worker_id}"
    return "orchestrator"


def assign_voices(
    session_id: UUID | str,
    speaker_ids: Iterable[str],
    catalog: Iterable[str],
    *,
    orchestrator_voice: str | None = None,
    overrides: Mapping[str, str | None] | None = None,
    muted_speakers: Iterable[str] = (),
) -> Mapping[str, VoiceAssignment]:
    """Assign voices reproducibly, independent of speaker and catalog ordering."""
    speakers = tuple(sorted(set(speaker_ids)))
    if any(not speaker.strip() for speaker in speakers):
        raise ValueError("speaker identifiers must not be blank")
    voices = tuple(sorted({voice for voice in catalog if voice.strip()}))
    override_map = dict(overrides or {})
    muted = frozenset(muted_speakers)
    result: dict[str, VoiceAssignment] = {}
    used: set[str] = set()

    if "orchestrator" in speakers:
        selected = override_map.get("orchestrator", orchestrator_voice)
        if selected is None and "orchestrator" not in muted and voices:
            selected = min(
                voices,
                key=lambda voice: sha256(
                    f"{session_id}\0orchestrator\0{voice}".encode("utf-8")
                ).digest(),
            )
        assignment = _explicit_assignment("orchestrator", selected, muted)
        result["orchestrator"] = assignment
        if assignment.voice_id is not None:
            used.add(assignment.voice_id)

    for speaker in speakers:
        if speaker == "orchestrator":
            continue
        if speaker in muted or (speaker in override_map and override_map[speaker] is None):
            result[speaker] = VoiceAssignment(speaker, None, muted=True)
            continue
        if speaker in override_map:
            voice = override_map[speaker]
            assert voice is not None
            if not voice.strip():
                raise ValueError("voice overrides must not be blank")
            result[speaker] = VoiceAssignment(speaker, voice)
            used.add(voice)
            continue

        ranked = sorted(
            voices,
            key=lambda voice: sha256(
                f"{session_id}\0{speaker}\0{voice}".encode("utf-8")
            ).digest(),
        )
        available = next((voice for voice in ranked if voice not in used), None)
        exhausted = available is None and bool(ranked)
        voice = available if available is not None else (ranked[0] if ranked else None)
        if voice is None:
            result[speaker] = VoiceAssignment(speaker, None, muted=True, exhausted=True)
            continue
        if available is not None:
            used.add(voice)
        rate, pitch = _fallback_prosody(session_id, speaker) if exhausted else (1.0, 0.0)
        result[speaker] = VoiceAssignment(
            speaker, voice, exhausted=exhausted, rate=rate, pitch=pitch
        )

    return MappingProxyType(result)


def _explicit_assignment(
    speaker: str, voice: str | None, muted: frozenset[str]
) -> VoiceAssignment:
    if speaker in muted or voice is None:
        return VoiceAssignment(speaker, None, muted=True)
    if not voice.strip():
        raise ValueError("reserved voices must not be blank")
    return VoiceAssignment(speaker, voice)


def _fallback_prosody(session_id: UUID | str, speaker: str) -> tuple[float, float]:
    digest = sha256(f"{session_id}\0{speaker}\0fallback".encode("utf-8")).digest()
    rate = 0.94 + (digest[0] / 255.0) * 0.12
    pitch = -0.15 + (digest[1] / 255.0) * 0.30
    return round(rate, 3), round(pitch, 3)
