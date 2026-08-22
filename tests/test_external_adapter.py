"""Contract tests for the host-fed Codex task snapshot adapter."""

from __future__ import annotations

import json
import unittest

from ctmao_nsd import CodexExecJsonlAdapter, CodexThreadSnapshotAdapter
from ctmao_nsd.conference import project_conference_turns
from ctmao_nsd.normalized_events import EventOrigin, EvidenceKind


def snapshot(*, status: str = "completed", message: str = "Observed answer") -> dict:
    return {
        "schemaVersion": 1,
        "thread": {
            "id": "019f86cb-fca8-7ad0-aff2-35dcbe49220c",
            "kind": "codex",
            "hostId": "local",
            "title": "Example",
        },
        "turns": [
            {
                "id": "019f8b09-ab21-7631-bfa7-8e221561915e",
                "status": status,
                "error": None,
                "startedAt": 1_784_744_094,
                "completedAt": 1_784_744_114 if status == "completed" else None,
                "items": [
                    {
                        "type": "userMessage",
                        "id": "item-user",
                        "content": [{"type": "text", "text": "User request"}],
                    },
                    {
                        "type": "reasoning",
                        "id": "item-reasoning",
                        "summary": ["private reasoning summary"],
                        "content": ["private scratchpad"],
                    },
                    {
                        "type": "agentMessage",
                        "id": "item-agent",
                        "text": message,
                        "phase": "final_answer",
                    },
                    {
                        "type": "mcpToolCall",
                        "id": "item-tool",
                        "server": "codex_apps",
                        "tool": "github.get_repo",
                        "status": "completed",
                        "arguments": {"secret": "must-not-cross"},
                    },
                    {
                        "type": "fileChange",
                        "id": "item-file",
                        "status": "completed",
                        "changes": [{"path": "private/path", "diff": "secret diff"}],
                    },
                ],
            }
        ],
    }


class CodexThreadSnapshotAdapterTests(unittest.TestCase):
    def test_maps_only_observable_records_and_drops_reasoning(self) -> None:
        result = CodexThreadSnapshotAdapter(include_message_text=True).adapt(snapshot())
        types = [event.event_type for event in result.events]
        self.assertEqual(
            types,
            [
                "task.started",
                "message.received",
                "message.sent",
                "tool.completed",
                "tool.completed",
                "task.completed",
            ],
        )
        serialized = "\n".join(event.to_json() for event in result.events)
        self.assertNotIn("private reasoning", serialized)
        self.assertNotIn("scratchpad", serialized)
        self.assertNotIn("must-not-cross", serialized)
        self.assertNotIn("secret diff", serialized)
        self.assertEqual(result.skipped_items, 1)
        self.assertTrue(all(event.origin is EventOrigin.ADAPTER for event in result.events))
        self.assertTrue(all(event.evidence is EvidenceKind.OBSERVED for event in result.events))

    def test_message_text_is_private_by_default_and_opt_in_is_redacted(self) -> None:
        hidden = CodexThreadSnapshotAdapter().adapt(
            snapshot(message="api_key=very-secret")
        )
        hidden_json = "\n".join(event.to_json() for event in hidden.events)
        self.assertNotIn("very-secret", hidden_json)
        self.assertNotIn("api_key", hidden_json)

        visible = CodexThreadSnapshotAdapter(include_message_text=True).adapt(
            snapshot(message="api_key=very-secret")
        )
        visible_json = "\n".join(event.to_json() for event in visible.events)
        self.assertNotIn("very-secret", visible_json)
        self.assertIn("[REDACTED]", visible_json)

    def test_replay_is_deduplicated_with_stable_source_ids(self) -> None:
        adapter = CodexThreadSnapshotAdapter()
        first = adapter.adapt(snapshot())
        second = adapter.adapt(snapshot())
        self.assertTrue(first.events)
        self.assertEqual(second.events, ())
        self.assertEqual(second.duplicate_items, len(first.events))
        self.assertEqual(len({event.event_id for event in first.events}), len(first.events))

    def test_conflicting_snapshot_replay_is_quarantined(self) -> None:
        adapter = CodexThreadSnapshotAdapter(include_message_text=True)
        adapter.adapt(snapshot(message="first answer"))
        conflict = adapter.adapt(snapshot(message="corrected answer"))
        self.assertEqual(conflict.conflicting_items, 1)
        self.assertNotIn(
            "corrected answer", "\n".join(event.to_json() for event in conflict.events)
        )

    def test_failed_tool_records_are_not_labeled_completed(self) -> None:
        value = snapshot()
        value["turns"][0]["items"][3]["status"] = "failed"
        value["turns"][0]["items"][4]["status"] = "failed"
        result = CodexThreadSnapshotAdapter().adapt(value)
        failed = [event for event in result.events if event.event_type == "tool.failed"]
        self.assertEqual(len(failed), 2)
        self.assertTrue(all(event.status == "failed" for event in failed))

    def test_missing_item_timestamp_uses_adapter_observation_time(self) -> None:
        value = snapshot()
        value["turns"][0]["completedAt"] = None
        result = CodexThreadSnapshotAdapter().adapt(value)
        start = result.events[0]
        message = next(e for e in result.events if e.event_type == "message.sent")
        terminal = result.events[-1]
        self.assertEqual(dict(start.provenance)["timestamp_basis"], "source")
        self.assertEqual(dict(message.provenance)["timestamp_basis"], "adapter_observed")
        self.assertEqual(dict(terminal.provenance)["timestamp_basis"], "adapter_observed")
        self.assertNotEqual(terminal.occurred_at, start.occurred_at)

    def test_conference_speaks_actual_opted_in_agent_message(self) -> None:
        result = CodexThreadSnapshotAdapter(include_message_text=True).adapt(snapshot())
        turns = project_conference_turns(result.events)
        agent_turn = next(turn for turn in turns if turn.text == "Observed answer")
        self.assertEqual(agent_turn.speaker_id, "codex")
        self.assertEqual(len(agent_turn.source_event_ids), 1)

    def test_rejects_wrong_schema_kind_and_oversized_snapshot(self) -> None:
        adapter = CodexThreadSnapshotAdapter()
        bad_schema = snapshot()
        bad_schema["schemaVersion"] = 2
        with self.assertRaises(ValueError):
            adapter.adapt(bad_schema)
        bad_kind = snapshot()
        bad_kind["thread"]["kind"] = "chat"
        with self.assertRaises(ValueError):
            adapter.adapt(bad_kind)
        with self.assertRaises(ValueError):
            adapter.adapt("x" * 2_000_001)

    def test_json_and_mapping_inputs_have_equivalent_events(self) -> None:
        left = CodexThreadSnapshotAdapter().adapt(snapshot()).events
        right = CodexThreadSnapshotAdapter().adapt(json.dumps(snapshot())).events
        self.assertEqual(
            [(event.event_id, event.to_dict()) for event in left],
            [(event.event_id, event.to_dict()) for event in right],
        )

    def test_explicit_nonterminal_status_does_not_fabricate_completion(self) -> None:
        result = CodexThreadSnapshotAdapter().adapt(snapshot(status="inProgress"))
        types = {event.event_type for event in result.events}
        self.assertIn("task.observed", types)
        self.assertNotIn("task.completed", types)
        self.assertNotIn("task.failed", types)

    def test_external_status_and_tool_metadata_are_redacted(self) -> None:
        value = snapshot(status="api_key=turn-secret")
        value["turns"][0]["items"][3]["server"] = "Bearer server-secret"
        value["turns"][0]["items"][3]["tool"] = "api_key=tool-secret"
        result = CodexThreadSnapshotAdapter().adapt(value)
        serialized = "\n".join(event.to_json() for event in result.events)
        for secret in ("turn-secret", "server-secret", "tool-secret"):
            self.assertNotIn(secret, serialized)

    def test_rejects_duplicate_keys_nonfinite_and_invalid_utf8(self) -> None:
        adapter = CodexThreadSnapshotAdapter()
        with self.assertRaises(ValueError):
            adapter.adapt('{"schemaVersion":1,"schemaVersion":1}')
        with self.assertRaises(ValueError):
            adapter.adapt('{"schemaVersion":NaN}')
        with self.assertRaises(ValueError):
            adapter.adapt(b"\xff")


class CodexExecJsonlAdapterTests(unittest.TestCase):
    def test_official_jsonl_lifecycle_and_agent_message(self) -> None:
        adapter = CodexExecJsonlAdapter(include_message_text=True)
        self.assertEqual(
            adapter.ingest_line(
                '{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}'
            ).events,
            (),
        )
        started = adapter.ingest_line('{"type":"turn.started"}').events[0]
        tool = adapter.ingest_line(
            '{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"secret command","status":"in_progress"}}'
        ).events[0]
        message = adapter.ingest_line(
            '{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"Verified result"}}'
        ).events[0]
        terminal = adapter.ingest_line(
            '{"type":"turn.completed","usage":{"input_tokens":10}}'
        ).events[0]
        self.assertEqual(
            [started.event_type, tool.event_type, message.event_type, terminal.event_type],
            ["task.started", "tool.started", "message.sent", "task.completed"],
        )
        self.assertEqual(message.summary, "Verified result")
        serialized = "\n".join(
            event.to_json() for event in (started, tool, message, terminal)
        )
        self.assertNotIn("secret command", serialized)
        self.assertNotIn("input_tokens", serialized)

    def test_reasoning_is_always_suppressed_and_replay_is_deduplicated(self) -> None:
        adapter = CodexExecJsonlAdapter(include_message_text=True)
        adapter.ingest_line(
            '{"type":"thread.started","thread_id":"thread-1"}'
        )
        adapter.ingest_line('{"type":"turn.started"}')
        reasoning = adapter.ingest_line(
            '{"type":"item.completed","item":{"id":"r1","type":"reasoning","text":"hidden"}}'
        )
        line = '{"type":"item.completed","item":{"id":"m1","type":"agent_message","text":"answer"}}'
        first = adapter.ingest_line(line)
        replay = adapter.ingest_line(line)
        self.assertEqual(reasoning.events, ())
        self.assertEqual(reasoning.skipped_items, 1)
        self.assertEqual(len(first.events), 1)
        self.assertEqual(replay.duplicate_items, 1)

    def test_failed_command_and_conflicting_replay_are_truthful(self) -> None:
        adapter = CodexExecJsonlAdapter(include_message_text=True)
        adapter.ingest_line('{"type":"thread.started","thread_id":"thread-1"}')
        adapter.ingest_line('{"type":"turn.started"}')
        failed = adapter.ingest_line(
            '{"type":"item.completed","item":{"id":"c1","type":"command_execution","status":"failed","command":"private"}}'
        ).events[0]
        self.assertEqual((failed.event_type, failed.status), ("tool.failed", "failed"))
        adapter.ingest_line(
            '{"type":"item.completed","item":{"id":"m1","type":"agent_message","text":"first"}}'
        )
        conflict = adapter.ingest_line(
            '{"type":"item.completed","item":{"id":"m1","type":"agent_message","text":"changed"}}'
        )
        self.assertEqual(conflict.conflicting_items, 1)

    def test_turn_lifecycle_and_reset_bound_state(self) -> None:
        adapter = CodexExecJsonlAdapter()
        thread = '{"type":"thread.started","thread_id":"thread-1"}'
        adapter.ingest_line(thread)
        adapter.ingest_line('{"type":"turn.started"}')
        with self.assertRaises(ValueError):
            adapter.ingest_line('{"type":"turn.started"}')
        adapter.ingest_line('{"type":"turn.completed"}')
        with self.assertRaises(ValueError):
            adapter.ingest_line(
                '{"type":"item.completed","item":{"id":"m1","type":"agent_message"}}'
            )
        adapter.reset()
        adapter.ingest_line(thread)
        self.assertEqual(
            adapter.ingest_line('{"type":"turn.started"}').events[0].sequence, 1
        )

    def test_requires_explicit_thread_and_turn_lifecycle(self) -> None:
        adapter = CodexExecJsonlAdapter()
        with self.assertRaises(ValueError):
            adapter.ingest_line('{"type":"turn.started"}')
        adapter.ingest_line(
            '{"type":"thread.started","thread_id":"thread-1"}'
        )
        with self.assertRaises(ValueError):
            adapter.ingest_line(
                '{"type":"item.completed","item":{"id":"m1","type":"agent_message"}}'
            )

    def test_hostile_jsonl_records_are_rejected(self) -> None:
        adapter = CodexExecJsonlAdapter()
        for record in (
            b"\xff",
            "[]",
            '{"type":"turn.started","type":"turn.completed"}',
            '{"type":NaN}',
            "x" * 65_537,
        ):
            with self.subTest(record=repr(record)[:40]):
                with self.assertRaises(ValueError):
                    adapter.ingest_line(record)


if __name__ == "__main__":
    unittest.main()
