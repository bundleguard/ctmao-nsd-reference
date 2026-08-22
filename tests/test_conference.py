"""Tests for evidence-backed conference projection and voice assignment."""

from __future__ import annotations

import unittest
from uuid import UUID

from ctmao_nsd.conference import (
    ConferenceMode,
    ConferenceTurn,
    Verbosity,
    assign_voices,
    project_conference_turns,
    project_briefing,
)
from ctmao_nsd.normalized_events import EventOrigin, EvidenceKind, NormalizedEvent


SESSION = UUID("00000000-0000-4000-8000-000000000001")
EVENT = UUID("00000000-0000-4000-8000-000000000002")


class ConferenceTests(unittest.TestCase):
    def test_projector_is_pure_deterministic_and_source_backed(self) -> None:
        event = NormalizedEvent(
            "task.completed",
            SESSION,
            4,
            "Task compile completed.",
            EventOrigin.WORKER,
            EvidenceKind.OBSERVED,
            worker_id="A",
            agent_path=("supervisor:A", "child:compile:1"),
            event_id=EVENT,
        )
        first = project_conference_turns((event,))
        second = project_conference_turns((event,))
        self.assertEqual(first, second)
        self.assertEqual(first[0].source_event_ids, (EVENT,))
        self.assertEqual(first[0].speaker_id, "supervisor:A/child:compile:1")
        self.assertEqual(first[0].utterance_id, first[0].turn_id)
        self.assertEqual(first[0].evidence, EvidenceKind.DERIVED)
        self.assertEqual(first[0].source_evidence, EvidenceKind.OBSERVED)

    def test_off_and_minimal_modes_filter_without_fabrication(self) -> None:
        started = NormalizedEvent(
            "task.started", SESSION, 1, "Task started.", EventOrigin.WORKER,
            EvidenceKind.DERIVED, event_id=EVENT,
        )
        self.assertEqual(project_conference_turns((started,), mode=ConferenceMode.OFF), ())
        self.assertEqual(
            project_conference_turns((started,), verbosity=Verbosity.MINIMAL), ()
        )

    def test_duplicate_delivery_is_projected_once(self) -> None:
        event = NormalizedEvent(
            "worker.started", SESSION, 1, "Worker started.", EventOrigin.WORKER,
            EvidenceKind.OBSERVED, worker_id="A", event_id=EVENT,
        )
        self.assertEqual(len(project_conference_turns((event, event))), 1)

    def test_turn_rejects_missing_source(self) -> None:
        with self.assertRaises(ValueError):
            ConferenceTurn("turn", SESSION, "orchestrator", "hello", (), EvidenceKind.DERIVED, 1)

    def test_voice_assignment_is_order_independent_and_reserves_orchestrator(self) -> None:
        left = assign_voices(
            SESSION, ("worker:B", "orchestrator", "worker:A"), ("v3", "v1", "v2"),
            orchestrator_voice="host",
        )
        right = assign_voices(
            SESSION, ("worker:A", "worker:B", "orchestrator"), ("v2", "v3", "v1"),
            orchestrator_voice="host",
        )
        self.assertEqual(dict(left), dict(right))
        self.assertEqual(left["orchestrator"].voice_id, "host")
        self.assertNotEqual(left["worker:A"].voice_id, left["worker:B"].voice_id)

    def test_voice_overrides_mute_and_exhaustion_fallback(self) -> None:
        assigned = assign_voices(
            SESSION,
            ("orchestrator", "A", "B", "C", "muted", "override"),
            ("only",),
            orchestrator_voice="host",
            overrides={"override": "custom"},
            muted_speakers=("muted",),
        )
        self.assertTrue(assigned["muted"].muted)
        self.assertEqual(assigned["override"].voice_id, "custom")
        exhausted = [assigned[key] for key in ("A", "B", "C") if assigned[key].exhausted]
        self.assertEqual(len(exhausted), 2)
        self.assertTrue(all(item.voice_id == "only" for item in exhausted))

    def test_projection_redacts_before_transcript_or_speech(self) -> None:
        event = NormalizedEvent(
            "task.failed", SESSION, 1, "api_key=secret-value", EventOrigin.WORKER,
            EvidenceKind.OBSERVED, event_id=EVENT,
        )
        projected = project_conference_turns((event,))
        self.assertEqual(projected[0].text, "api_key=[REDACTED]")
        self.assertIn(("redacted", True), projected[0].redactions)

    def test_briefing_uses_terminal_source_events(self) -> None:
        task = NormalizedEvent(
            "task.completed", SESSION, 1, "done", EventOrigin.WORKER,
            EvidenceKind.OBSERVED, worker_id="A", event_id=EVENT,
        )
        terminal = NormalizedEvent(
            "orchestration.run.completed", SESSION, 2, "run done",
            EventOrigin.ORCHESTRATOR, EvidenceKind.OBSERVED,
            event_id=UUID("00000000-0000-4000-8000-000000000003"),
        )
        briefing = project_briefing((task, terminal))
        self.assertEqual(len(briefing), 1)
        self.assertEqual(briefing[0].speaker_id, "orchestrator")
        self.assertEqual(briefing[0].evidence, EvidenceKind.DERIVED)
        self.assertEqual(briefing[0].source_event_ids, (EVENT, terminal.event_id))

    def test_briefing_filters_other_runs_and_duplicate_events(self) -> None:
        other_run = UUID("00000000-0000-4000-8000-000000000010")
        current_run = UUID("00000000-0000-4000-8000-000000000011")
        old = NormalizedEvent(
            "task.failed", SESSION, 1, "old", EventOrigin.WORKER,
            EvidenceKind.OBSERVED, run_id=other_run, event_id=EVENT,
        )
        current = NormalizedEvent(
            "task.completed", SESSION, 2, "current", EventOrigin.WORKER,
            EvidenceKind.OBSERVED, run_id=current_run,
            event_id=UUID("00000000-0000-4000-8000-000000000012"),
        )
        terminal = NormalizedEvent(
            "orchestration.run.completed", SESSION, 3, "done",
            EventOrigin.ORCHESTRATOR, EvidenceKind.OBSERVED, run_id=current_run,
            event_id=UUID("00000000-0000-4000-8000-000000000013"),
        )
        briefing = project_briefing((old, current, current, terminal))[0]
        self.assertEqual(briefing.source_event_ids, (current.event_id, terminal.event_id))
        self.assertIn("1 tasks completed", briefing.text)

    def test_orchestrator_origin_is_spoken_by_orchestrator(self) -> None:
        event = NormalizedEvent(
            "task.assigned", SESSION, 1, "assigned", EventOrigin.ORCHESTRATOR,
            EvidenceKind.OBSERVED, worker_id="A", agent_path=("supervisor:A",),
            event_id=EVENT,
        )
        self.assertEqual(project_conference_turns((event,))[0].speaker_id, "orchestrator")


if __name__ == "__main__":
    unittest.main()
