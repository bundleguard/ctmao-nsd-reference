"""Codex host bridge and source-backed public-message tests."""

from __future__ import annotations

import json
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ctmao_nsd import (
    CODEX_BRIDGE_PROTOCOL_VERSION,
    CodexConferenceBridge,
    EventBus,
)
from ctmao_nsd.conference import project_message_turns
from ctmao_nsd.live_ui import LiveDashboard, _turn_dict
from examples.codex_conference_bridge import _post, _write_session_file


class CodexConferenceBridgeTests(unittest.TestCase):
    def test_plugin_http_client_four_role_handshake_receipts_and_finish(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        dashboard = LiveDashboard(bus, control=bridge)
        session_file = None
        try:
            bridge.start("Complete the plugin HTTP compatibility smoke")
            dashboard.start()
            session_file = _write_session_file(dashboard.url)
            origin = dashboard.url.split("/?", 1)[0]
            for supplied in (None, "0.9"):
                headers = {"Content-Type": "application/json"}
                if supplied is not None:
                    headers["X-CTMAO-Bridge-Protocol"] = supplied
                request = Request(
                    origin + f"/api/bridge/participants?token={dashboard.token}",
                    data=b"{}", headers=headers, method="POST",
                )
                with self.assertRaises(HTTPError) as caught:
                    urlopen(request, timeout=2.0)
                with caught.exception as response:
                    self.assertEqual(response.code, 409)
                    self.assertIn("incompatible", json.loads(response.read())["error"])

            def send(endpoint: str, **payload: object) -> dict[str, object]:
                output = StringIO()
                with redirect_stdout(output):
                    _post(
                        str(session_file), "/api/bridge/" + endpoint, payload,
                        plugin_protocol_version=CODEX_BRIDGE_PROTOCOL_VERSION,
                    )
                receipt = json.loads(output.getvalue())
                self.assertIn("accepted_event_id", receipt)
                self.assertEqual(
                    receipt["bridge_protocol_version"], CODEX_BRIDGE_PROTOCOL_VERSION
                )
                return receipt

            messages = {}
            work = {}
            for role, kind, sources in (
                ("Researcher", "analysis", ()),
                ("Builder", "proposal", ()),
                ("Reviewer", "review", ("Researcher", "Builder")),
                ("Moderator", "decision", ("Researcher", "Builder", "Reviewer")),
            ):
                work[role] = send(
                    "participants", participant_id=role, state="working",
                    source_ref=f"http-smoke/{role}/working",
                )["accepted_event_id"]
                messages[role] = send(
                    "messages", participant_id=role, message_kind=kind,
                    text=f"{role} public smoke result.",
                    source_ref=f"http-smoke/{role}/message",
                    causation_event_id=work[role],
                    source_event_ids=[messages[source] for source in sources],
                )["accepted_event_id"]
            terminal = send("finish", status="completed", source_ref="http-smoke/finish")
            self.assertEqual(terminal["status"], "completed")
            self.assertEqual(len(set(messages.values())), 4)
            events = {str(event.event_id): event for event in bus.replay()}
            for role, event_id in messages.items():
                self.assertEqual(str(events[event_id].causation_event_id), work[role])
            self.assertEqual(
                set(dict(events[messages["Moderator"]].provenance)["source_event_ids"].split(",")),
                {messages["Researcher"], messages["Builder"], messages["Reviewer"]},
            )
        finally:
            if session_file is not None:
                session_file.unlink(missing_ok=True)
            dashboard.close()
            bus.close()

    def test_sequence_remains_monotonic_across_conferences_in_one_session(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("First run")
            bridge.finish(source_ref="first-finish")
            bridge.start("Second run")
            sequences = [event.sequence for event in bus.replay()]
            self.assertEqual(sequences, [1, 2, 3])
        finally:
            bus.close()

    def test_objective_is_redacted_before_snapshot_presentation(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            state = bridge.start("Investigate password=hunter2")
            self.assertNotIn("hunter2", state["objective"])
            self.assertIn("[REDACTED]", state["objective"])
        finally:
            bus.close()

    def test_host_client_rejects_loopback_prefix_with_remote_authority(self) -> None:
        with tempfile.TemporaryDirectory() as raw_directory:
            session_file = Path(raw_directory) / "session.json"
            session_file.write_text(
                json.dumps(
                    {
                        "origin": "http://127.0.0.1:80@example.com",
                        "token": "private-token",
                        "bridge_protocol_version": CODEX_BRIDGE_PROTOCOL_VERSION,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(SystemExit, "origin is not loopback"):
                _post(
                    str(session_file),
                    "/api/bridge/messages",
                    {"text": "must not leave loopback"},
                    plugin_protocol_version=CODEX_BRIDGE_PROTOCOL_VERSION,
                )

    def test_bridge_protocol_is_explicit_and_mismatch_fails_closed(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            state = bridge.start("Review compatibility")
            self.assertEqual(
                state["bridge_protocol_version"], CODEX_BRIDGE_PROTOCOL_VERSION
            )
            with tempfile.TemporaryDirectory() as raw_directory:
                session_file = Path(raw_directory) / "session.json"
                session_file.write_text(
                    json.dumps(
                        {
                            "origin": "http://127.0.0.1:9",
                            "token": "private-token",
                            "bridge_protocol_version": "0.9",
                        }
                    ),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(SystemExit, "session protocol is incompatible"):
                    _post(
                        str(session_file),
                        "/api/bridge/messages",
                        {"text": "must not be sent"},
                        plugin_protocol_version=CODEX_BRIDGE_PROTOCOL_VERSION,
                    )
                with self.assertRaisesRegex(SystemExit, "plugin protocol 0.9"):
                    _post(
                        str(session_file),
                        "/api/bridge/messages",
                        {"text": "must not be sent"},
                        plugin_protocol_version="0.9",
                    )
        finally:
            bus.close()

    def test_public_subagent_messages_are_normalized_and_source_backed(self) -> None:
        bus = EventBus()
        events = []
        delivered = threading.Event()

        def accept(event: object) -> None:
            events.append(event)
            if event.event_type == "message.sent":
                delivered.set()

        token = bus.subscribe(accept)
        bridge = CodexConferenceBridge(bus)
        try:
            started = bridge.start("Fix the failing tests")
            self.assertEqual(started["mode"], "codex")
            bridge.update_participant(
                "Researcher", "working", source_ref="codex-child-thread-1"
            )
            bridge.publish_message(
                "Researcher",
                "The failing test is isolated to the transport boundary.",
                message_kind="analysis",
                source_ref="codex-child-thread-1:final",
            )
            self.assertTrue(delivered.wait(2.0))
            message = next(event for event in events if event.event_type == "message.sent")
            self.assertEqual(dict(message.attributes)["speaker_id"], "Researcher")
            self.assertEqual(message.summary, "The failing test is isolated to the transport boundary.")
            provenance = dict(message.provenance)
            self.assertEqual(provenance["source_interface"], "codex_host_public_message")
            self.assertNotIn("codex-child-thread-1", json.dumps(message.to_dict()))
        finally:
            bus.unsubscribe(token)
            bus.close()

    def test_finish_clears_current_speaker_and_preserves_task_state(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("Review this change")
            bridge.update_participant(
                "Researcher", "working", source_ref="researcher-thread"
            )
            research = bridge.publish_message(
                "Researcher",
                "The focused evidence is available.",
                message_kind="analysis",
                source_ref="researcher-final",
            )
            bridge.update_participant(
                "Moderator", "working", source_ref="moderator-thread"
            )
            bridge.publish_message(
                "Moderator",
                "Proceed after the focused tests pass.",
                message_kind="decision",
                source_ref="moderator-final",
                source_event_ids=(research["accepted_event_id"],),
            )
            state = bridge.finish(source_ref="main-thread-final")
            self.assertEqual(state["status"], "completed")
            self.assertIsNone(state["current_speaker"])
            moderator = next(
                item for item in state["participants"] if item["participant_id"] == "Moderator"
            )
            self.assertEqual(moderator["state"], "finished")
        finally:
            bus.close()

    def test_end_to_end_four_role_flow_retains_exact_evidence_and_finishes(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("Trace a real conference")
            research_work = bridge.update_participant(
                "Researcher", "working", source_ref="research-thread"
            )
            research = bridge.publish_message(
                "Researcher",
                "Observed evidence.",
                message_kind="analysis",
                source_ref="research-final",
                causation_event_id=research_work["accepted_event_id"],
            )
            build_work = bridge.update_participant(
                "Builder", "working", source_ref="builder-thread"
            )
            builder = bridge.publish_message(
                "Builder",
                "Implemented proposal.",
                message_kind="proposal",
                source_ref="builder-final",
                causation_event_id=build_work["accepted_event_id"],
            )
            review_work = bridge.update_participant(
                "Reviewer", "working", source_ref="review-thread"
            )
            reviewer = bridge.publish_message(
                "Reviewer",
                "Reviewed both inputs.",
                message_kind="review",
                source_ref="review-final",
                causation_event_id=review_work["accepted_event_id"],
                source_event_ids=(
                    research["accepted_event_id"], builder["accepted_event_id"]
                ),
            )
            moderator_work = bridge.update_participant(
                "Moderator", "working", source_ref="moderator-thread"
            )
            moderator = bridge.publish_message(
                "Moderator",
                "Decision based on the conference evidence.",
                message_kind="decision",
                source_ref="moderator-final",
                causation_event_id=moderator_work["accepted_event_id"],
                source_event_ids=(
                    research["accepted_event_id"],
                    builder["accepted_event_id"],
                    reviewer["accepted_event_id"],
                ),
            )
            terminal = bridge.finish(
                status="completed",
                source_ref="conference-finish",
                detail="All four public roles completed.",
            )
            events = {str(event.event_id): event for event in bus.replay()}
            for work, message in (
                (research_work, research),
                (build_work, builder),
                (review_work, reviewer),
                (moderator_work, moderator),
            ):
                work_event = events[work["accepted_event_id"]]
                message_event = events[message["accepted_event_id"]]
                self.assertEqual(message_event.causation_event_id, work_event.event_id)
                self.assertEqual(message_event.correlation_id, work_event.correlation_id)
            self.assertEqual(
                set(dict(events[reviewer["accepted_event_id"]].provenance)["source_event_ids"].split(",")),
                {research["accepted_event_id"], builder["accepted_event_id"]},
            )
            self.assertEqual(
                set(dict(events[moderator["accepted_event_id"]].provenance)["source_event_ids"].split(",")),
                {
                    research["accepted_event_id"],
                    builder["accepted_event_id"],
                    reviewer["accepted_event_id"],
                },
            )
            self.assertEqual(terminal["status"], "completed")
            self.assertIsNone(terminal["current_speaker"])
            self.assertTrue(
                all(
                    participant["state"] == "finished"
                    for participant in terminal["participants"]
                )
            )
            with self.assertRaises(ValueError):
                bridge.publish_message(
                    "Reviewer",
                    "Unknown source.",
                    message_kind="review",
                    source_ref="review-final-2",
                    source_event_ids=("00000000-0000-4000-8000-000000000001",),
                )
        finally:
            bus.close()

    def test_rejects_messages_without_role_correct_causal_evidence(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("Reject fabricated causality")
            with self.assertRaisesRegex(ValueError, "working event"):
                bridge.publish_message(
                    "Researcher",
                    "No lifecycle.",
                    message_kind="analysis",
                    source_ref="research-final",
                )
            research_work = bridge.update_participant(
                "Researcher", "working", source_ref="research-thread"
            )
            builder_work = bridge.update_participant(
                "Builder", "working", source_ref="builder-thread"
            )
            with self.assertRaisesRegex(ValueError, "participant's accepted working"):
                bridge.publish_message(
                    "Researcher",
                    "Wrong direct cause.",
                    message_kind="analysis",
                    source_ref="research-final",
                    causation_event_id=builder_work["accepted_event_id"],
                )
            research = bridge.publish_message(
                "Researcher",
                "Observed evidence.",
                message_kind="analysis",
                source_ref="research-final",
                causation_event_id=research_work["accepted_event_id"],
            )
            with self.assertRaisesRegex(ValueError, "exactly match"):
                bridge.publish_message(
                    "Builder",
                    "Initial roles cannot cite one another.",
                    message_kind="proposal",
                    source_ref="builder-final",
                    source_event_ids=(research["accepted_event_id"],),
                )
            builder = bridge.publish_message(
                "Builder",
                "Independent proposal.",
                message_kind="proposal",
                source_ref="builder-final",
            )
            bridge.update_participant(
                "Reviewer", "working", source_ref="review-thread"
            )
            with self.assertRaisesRegex(ValueError, "exactly match"):
                bridge.publish_message(
                    "Reviewer",
                    "Incomplete join.",
                    message_kind="review",
                    source_ref="review-final",
                    source_event_ids=(research["accepted_event_id"],),
                )
            review = bridge.publish_message(
                "Reviewer",
                "Complete join.",
                message_kind="review",
                source_ref="review-final",
                source_event_ids=(
                    research["accepted_event_id"], builder["accepted_event_id"]
                ),
            )
            bridge.update_participant(
                "Moderator", "working", source_ref="moderator-thread"
            )
            with self.assertRaisesRegex(ValueError, "exactly match"):
                bridge.publish_message(
                    "Moderator",
                    "Missing review.",
                    message_kind="decision",
                    source_ref="moderator-final",
                    source_event_ids=(
                        research["accepted_event_id"], builder["accepted_event_id"]
                    ),
                )
            bridge.publish_message(
                "Moderator",
                "All evidence present.",
                message_kind="decision",
                source_ref="moderator-final",
                source_event_ids=(
                    research["accepted_event_id"],
                    builder["accepted_event_id"],
                    review["accepted_event_id"],
                ),
            )
        finally:
            bus.close()

    def test_event_registry_is_bounded_and_reserves_terminal_capacity(self) -> None:
        bus = EventBus(replay_capacity=512)
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("Bound event retention")
            for index in range(254):
                with bridge._lock:
                    bridge._publish_locked(
                        "task.observed", f"bounded diagnostic {index}"
                    )
            with self.assertRaisesRegex(ValueError, "capacity reached"):
                with bridge._lock:
                    bridge._publish_locked("task.observed", "overflow")
            result = bridge.finish(status="cancelled", source_ref="terminal")
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(len(bus.replay()), 256)
        finally:
            bus.close()

    def test_duplicate_contribution_and_state_rewind_are_rejected_without_hiding_reviewer(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("Keep downstream provenance consistent")
            research_work = bridge.update_participant(
                "Researcher", "working", source_ref="research-thread"
            )
            with self.assertRaisesRegex(ValueError, "transition"):
                bridge.update_participant(
                    "Researcher", "working", source_ref="replacement-work"
                )
            research = bridge.publish_message(
                "Researcher",
                "Stable evidence.",
                message_kind="analysis",
                source_ref="research-final",
                causation_event_id=research_work["accepted_event_id"],
            )
            with self.assertRaisesRegex(ValueError, "already published"):
                bridge.publish_message(
                    "Researcher",
                    "Duplicate evidence.",
                    message_kind="analysis",
                    source_ref="research-final-duplicate",
                )
            for rewind in ("waiting", "working", "failed", "contributed"):
                with self.assertRaisesRegex(ValueError, "transition"):
                    bridge.update_participant(
                        "Researcher", rewind, source_ref=f"rewind-{rewind}"
                    )

            bridge.update_participant(
                "Builder", "working", source_ref="builder-thread"
            )
            builder = bridge.publish_message(
                "Builder",
                "Stable proposal.",
                message_kind="proposal",
                source_ref="builder-final",
            )
            review_work = bridge.update_participant(
                "Reviewer", "working", source_ref="review-thread"
            )
            review = bridge.publish_message(
                "Reviewer",
                "Both stable inputs remain inspectable.",
                message_kind="review",
                source_ref="review-final",
                causation_event_id=review_work["accepted_event_id"],
                source_event_ids=(
                    research["accepted_event_id"], builder["accepted_event_id"]
                ),
            )
            events = bus.replay()
            review_event = next(
                event
                for event in events
                if str(event.event_id) == review["accepted_event_id"]
            )
            turn = project_message_turns((review_event,))[0]
            serialized = _turn_dict(turn, events)
            root_record = serialized["source_evidence_records"][0]
            self.assertEqual(
                set(root_record["source_event_ids"]),
                {research["accepted_event_id"], builder["accepted_event_id"]},
            )
            self.assertEqual(
                root_record["causation_event_id"], review_work["accepted_event_id"]
            )
            self.assertEqual(serialized["text"], review_event.summary)
        finally:
            bus.close()

    def test_recipient_metadata_is_redacted_before_event_and_ui_presentation(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        ui_bus = EventBus()
        dashboard = LiveDashboard(ui_bus)
        try:
            bridge.start("Redact structured presentation metadata")
            bridge.update_participant(
                "Researcher", "working", source_ref="research-thread"
            )
            bridge.publish_message(
                "Researcher",
                "Safe public evidence.",
                message_kind="analysis",
                source_ref="research-final",
                recipient_id="password=hunter2",
            )
            events = bus.replay()
            message = next(
                event for event in events if event.event_type == "message.sent"
            )
            self.assertNotIn("hunter2", message.to_json())
            self.assertEqual(
                dict(message.attributes)["recipient_id"], "password=[REDACTED]"
            )
            for event in events:
                dashboard._accept_event(event)
            payload = json.loads(dashboard._messages_after(0)[-1][1])
            encoded = json.dumps(payload)
            self.assertNotIn("hunter2", encoded)
            self.assertEqual(
                payload["messages"][0]["recipient_id"], "password=[REDACTED]"
            )
            self.assertTrue(
                payload["messages"][0]["source_evidence_records"][0]
                ["redactions"]["redacted"]
            )
        finally:
            dashboard.close()
            ui_bus.close()
            bus.close()

    def test_rejects_unknown_participants_and_unsafe_messages(self) -> None:
        bus = EventBus()
        bridge = CodexConferenceBridge(bus)
        try:
            bridge.start("Check validation")
            with self.assertRaises(ValueError):
                bridge.publish_message(
                    "InventedAgent",
                    "message",
                    message_kind="analysis",
                    source_ref="source",
                )
            with self.assertRaises(ValueError):
                bridge.publish_message(
                    "Researcher",
                    " ",
                    message_kind="analysis",
                    source_ref="source",
                )
            with self.assertRaises(ValueError):
                bridge.update_participant(
                    "Researcher", "thinking privately", source_ref="source"
                )
        finally:
            bus.close()


if __name__ == "__main__":
    unittest.main()
