"""Loopback dashboard security and state endpoint tests."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from ctmao_nsd import CodexConferenceBridge, EventBus
from ctmao_nsd.conference import project_message_turns
from ctmao_nsd.live_ui import LiveDashboard, _turn_dict
from ctmao_nsd.normalized_events import EventOrigin, EvidenceKind, NormalizedEvent


class LiveDashboardTests(unittest.TestCase):
    def test_browser_harness_explains_direct_file_launch(self) -> None:
        page = (
            Path(__file__).with_name("browser") / "dashboard.html"
        ).read_text(encoding="utf-8")
        self.assertIn("location.protocol==='file:'", page)
        self.assertIn("tests\\run_browser_dashboard_smoke.ps1", page)
        self.assertIn("This test page needs its local server", page)

    def test_sse_peer_disconnects_end_the_stream_without_handler_failure(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        try:
            for failure in (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                with self.subTest(failure=failure.__name__):
                    handler = object.__new__(dashboard._handler_type())
                    handler._headers = Mock()
                    handler.headers = {}
                    handler.wfile = Mock()
                    handler.wfile.write.side_effect = failure("peer disconnected")
                    handler._stream_events()
                    handler.wfile.write.assert_called_once_with(b": heartbeat\n\n")
        finally:
            dashboard.close()
            bus.close()

    def test_turn_serialization_resolves_inspectable_source_evidence_fail_closed(self) -> None:
        session_id = uuid4()
        correlation_id = uuid4()
        task_id = uuid4()
        cause = NormalizedEvent(
            event_type="task.started",
            session_id=session_id,
            sequence=1,
            summary="task started",
            origin=EventOrigin.ADAPTER,
            evidence=EvidenceKind.OBSERVED,
            worker_id="Researcher",
            correlation_id=correlation_id,
            task_id=task_id,
            agent_path=("Researcher",),
        )
        event = NormalizedEvent(
            event_type="message.sent",
            session_id=session_id,
            sequence=2,
            summary="source-backed message",
            origin=EventOrigin.ADAPTER,
            evidence=EvidenceKind.OBSERVED,
            worker_id="Researcher",
            correlation_id=correlation_id,
            task_id=task_id,
            agent_path=("Researcher",),
            causation_event_id=cause.event_id,
            attributes=(("speaker_id", "Researcher"),),
            provenance=(("source_interface", "test-host"),),
        )
        turn = project_message_turns((event,))[0]
        serialized = _turn_dict(turn, (cause, event))
        self.assertEqual(serialized["source_event_ids"], [str(event.event_id)])
        self.assertEqual(
            [item["event_id"] for item in serialized["source_evidence_records"]],
            [str(event.event_id), str(cause.event_id)],
        )
        message_record, cause_record = serialized["source_evidence_records"]
        self.assertEqual(message_record["causation_event_id"], str(cause.event_id))
        self.assertEqual(cause_record["event_type"], "task.started")
        with self.assertRaises(ValueError):
            _turn_dict(turn, (event,))

    def test_active_provenance_registry_is_bounded(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        session_id = uuid4()
        run_id = uuid4()
        try:
            for sequence in range(1, 520):
                dashboard._accept_event(
                    NormalizedEvent(
                        event_type="task.observed",
                        session_id=session_id,
                        run_id=run_id,
                        sequence=sequence,
                        summary="bounded diagnostic",
                        origin=EventOrigin.ADAPTER,
                        evidence=EvidenceKind.OBSERVED,
                    )
                )
            self.assertEqual(len(dashboard._active_events), 512)
            self.assertEqual(
                min(event.sequence for event in dashboard._active_events.values()), 8
            )
        finally:
            dashboard.close()
            bus.close()

    def test_moderator_utterance_exposes_complete_four_role_causal_graph(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("Resolve the whole graph")
            messages: dict[str, object] = {}
            for role, kind, text, sources in (
                ("Researcher", "analysis", "Evidence.", ()),
                ("Builder", "proposal", "Implementation.", ()),
                (
                    "Reviewer",
                    "review",
                    "Validated.",
                    ("Researcher", "Builder"),
                ),
                (
                    "Moderator",
                    "decision",
                    "Decision.",
                    ("Researcher", "Builder", "Reviewer"),
                ),
            ):
                bridge.update_participant(role, "working", source_ref=f"{role}-thread")
                messages[role] = bridge.publish_message(
                    role,
                    text,
                    message_kind=kind,
                    source_ref=f"{role}-final",
                    source_event_ids=tuple(
                        messages[source]["accepted_event_id"] for source in sources
                    ),
                )
            events = bus.replay()
            moderator_event = next(
                event
                for event in events
                if event.event_type == "message.sent"
                and dict(event.attributes).get("speaker_id") == "Moderator"
            )
            turn = project_message_turns((moderator_event,))[0]
            serialized = _turn_dict(turn, events)
            records = serialized["source_evidence_records"]
            self.assertEqual(
                sum(item["event_type"] == "message.sent" for item in records), 4
            )
            self.assertEqual(
                sum(item["event_type"] == "task.started" for item in records), 4
            )
            self.assertIn("orchestration.run.started", {
                item["event_type"] for item in records
            })
            builder_start = next(
                event
                for event in events
                if event.event_type == "task.started" and event.worker_id == "Builder"
            )
            incomplete = tuple(
                event for event in events if event.event_id != builder_start.event_id
            )
            with self.assertRaisesRegex(ValueError, "dangling"):
                _turn_dict(turn, incomplete)
        finally:
            bus.close()

    def test_codex_mode_rejects_delayed_event_from_previous_conference(self) -> None:
        active_id = uuid4()

        class Control:
            def snapshot(self) -> dict[str, object]:
                return {
                    "conference_id": str(active_id),
                    "mode": "codex",
                    "status": "running",
                }

        bus = EventBus()
        dashboard = LiveDashboard(bus, control=Control())
        try:
            stale = NormalizedEvent(
                event_type="message.sent",
                session_id=uuid4(),
                sequence=1,
                summary="stale public message",
                origin=EventOrigin.ADAPTER,
                evidence=EvidenceKind.OBSERVED,
                run_id=uuid4(),
                attributes=(
                    ("message_kind", "analysis"),
                    ("recipient_id", "conference"),
                    ("speaker_id", "Researcher"),
                ),
            )
            dashboard._accept_event(stale)
            self.assertEqual(dashboard._messages_after(0), ())
            self.assertEqual(dashboard.projection.snapshot().run_status, "idle")
        finally:
            dashboard.close()
            bus.close()

    def test_close_before_start_is_safe_and_start_after_close_is_rejected(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        dashboard.close()
        try:
            with self.assertRaises(RuntimeError):
                dashboard.start()
        finally:
            bus.close()

    def test_ipv6_loopback_is_rejected_until_v1_supports_it(self) -> None:
        bus = EventBus()
        try:
            with self.assertRaises(ValueError):
                LiveDashboard(bus, host="::1")
        finally:
            bus.close()

    def test_loopback_token_and_no_store_state_endpoint(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        dashboard.start()
        try:
            base = dashboard.url.rsplit("/", 1)[0]
            with self.assertRaises(HTTPError) as denied:
                urlopen(base + "/api/state?token=wrong", timeout=1.0)
            self.assertEqual(denied.exception.code, 403)
            response = urlopen(
                base + f"/api/state?token={dashboard.token}", timeout=1.0
            )
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            payload = json.loads(response.read())
            self.assertEqual(payload["run_status"], "idle")
        finally:
            dashboard.close()
            bus.close()

    def test_dashboard_exposes_conference_controls_and_provenance(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        dashboard.start()
        try:
            with urlopen(dashboard.url, timeout=1.0) as response:
                page = response.read().decode("utf-8")
            self.assertIn("Start conference", page)
            self.assertIn("Public contributions and final decision", page)
            self.assertIn("Evidence flow", page)
            self.assertIn("Technical events and task tree", page)
            self.assertIn("Voice for ${id}", page)
            self.assertIn("Accepted linked evidence", page)
            self.assertIn("Required provenance links complete · host-reported", page)
            self.assertIn("Host-relative provenance", page)
            self.assertIn("resetConference", page)
            self.assertIn("Show full message", page)
            self.assertIn("Watching a Codex task", page)
            self.assertIn("Enable audio", page)
            self.assertNotIn("p.turns.forEach", page)
            self.assertNotIn("verified events", page.lower())
        finally:
            dashboard.close()
            bus.close()

    def test_dashboard_has_responsive_accessible_interaction_contract(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        dashboard.start()
        try:
            with urlopen(dashboard.url, timeout=1.0) as response:
                page = response.read().decode("utf-8")
            self.assertIn('@media(max-width:650px)', page)
            self.assertIn('@media(prefers-reduced-motion:reduce)', page)
            self.assertIn('aria-live="polite"', page)
            self.assertIn('aria-pressed="false"', page)
            self.assertIn('audioEnabled=false', page)
            self.assertIn("Latest contribution", page)
            self.assertIn("utterance.onstart", page)
            self.assertIn("messageEventByRole", page)
            self.assertIn("root.source_event_ids", page)
            self.assertIn("classList.add('accepted')", page)
            self.assertIn("edge-research-moderate", page)
            self.assertIn("edge-build-moderate", page)
            self.assertIn("all[index%all.length]", page)
            self.assertIn("Independent parallel phase", page)
            self.assertIn("Steer, approve, and review changes in Codex", page)
        finally:
            dashboard.close()
            bus.close()

    def test_authenticated_control_endpoint_starts_and_cancels(self) -> None:
        class Control:
            def __init__(self) -> None:
                self.objective = None

            def start(self, objective: str) -> dict[str, object]:
                self.objective = objective
                return {"status": "queued"}

            def cancel(self) -> dict[str, object]:
                return {"status": "cancelling"}

            def snapshot(self) -> dict[str, object]:
                return {"status": "idle", "provider": "test"}

        bus = EventBus()
        control = Control()
        dashboard = LiveDashboard(bus, control=control)
        dashboard.start()
        try:
            base = dashboard.url.rsplit("/", 1)[0]
            request = Request(
                base + f"/api/runs?token={dashboard.token}",
                data=json.dumps({"objective": "Discuss a safe release"}).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=1.0) as response:
                self.assertEqual(response.status, 202)
            self.assertEqual(control.objective, "Discuss a safe release")

            cancel = Request(
                base + f"/api/runs/cancel?token={dashboard.token}",
                data=b"{}",
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlopen(cancel, timeout=1.0) as response:
                self.assertEqual(json.loads(response.read())["status"], "cancelling")
        finally:
            dashboard.close()
            bus.close()

    def test_control_endpoint_rejects_foreign_origin(self) -> None:
        class Control:
            def start(self, objective: str) -> dict[str, object]:
                raise AssertionError("must not be called")

            def cancel(self) -> dict[str, object]:
                raise AssertionError("must not be called")

            def snapshot(self) -> dict[str, object]:
                return {}

        bus = EventBus()
        dashboard = LiveDashboard(bus, control=Control())
        dashboard.start()
        try:
            base = dashboard.url.rsplit("/", 1)[0]
            request = Request(
                base + f"/api/runs?token={dashboard.token}",
                data=b'{"objective":"x"}',
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Origin": "https://attacker.invalid",
                },
            )
            with self.assertRaises(HTTPError) as denied:
                urlopen(request, timeout=1.0)
            self.assertEqual(denied.exception.code, 403)
        finally:
            dashboard.close()
            bus.close()

    def test_bridge_message_endpoint_is_explicit_and_bounded(self) -> None:
        class Control:
            def __init__(self) -> None:
                self.received = None

            def start(self, objective: str) -> dict[str, object]:
                return {"status": "running"}

            def cancel(self) -> dict[str, object]:
                return {"status": "cancelled"}

            def snapshot(self) -> dict[str, object]:
                return {"status": "running", "provider": "Codex"}

            def publish_message(self, participant_id: str, text: str, **values: object) -> dict[str, object]:
                self.received = (participant_id, text, values)
                return {"status": "running"}

        bus = EventBus()
        control = Control()
        dashboard = LiveDashboard(bus, control=control)
        dashboard.start()
        try:
            base = dashboard.url.rsplit("/", 1)[0]
            request = Request(
                base + f"/api/bridge/messages?token={dashboard.token}",
                data=json.dumps(
                    {
                        "participant_id": "Researcher",
                        "text": "Observed result",
                        "message_kind": "analysis",
                        "source_ref": "child-thread-1",
                    }
                ).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=2.0) as response:
                self.assertEqual(response.status, 202)
            self.assertEqual(control.received[0:2], ("Researcher", "Observed result"))
            self.assertEqual(control.received[2]["message_kind"], "analysis")
        finally:
            dashboard.close()
            bus.close()


if __name__ == "__main__":
    unittest.main()
