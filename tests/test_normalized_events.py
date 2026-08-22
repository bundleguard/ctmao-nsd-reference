"""Tests for the strict provider-neutral normalized event protocol."""

from __future__ import annotations

import json
import unittest
from dataclasses import FrozenInstanceError
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

from ctmao_nsd.normalized_events import (
    EvidenceKind,
    EventImportance,
    EventOrigin,
    MAX_EVENT_BYTES,
    NormalizedEvent,
    redact_text,
    utc_now_rfc3339,
    validate_rfc3339_utc,
)


class NormalizedEventTests(unittest.TestCase):
    def event(self, **changes: object) -> NormalizedEvent:
        values = {
            "event_type": "task.started",
            "session_id": uuid4(),
            "run_id": uuid4(),
            "sequence": 1,
            "summary": "Worker A started the task.",
            "origin": EventOrigin.WORKER,
            "evidence": EvidenceKind.OBSERVED,
            "importance": EventImportance.ROUTINE,
            "worker_id": "A",
            "task_id": uuid4(),
            "agent_path": ("supervisor:A", "child:one:1"),
            "attributes": (("depth", 1), ("visible", True)),
            "provenance": (("source", "worker-envelope"),),
            "redactions": (("redacted", False),),
            "occurred_at": "2026-08-21T12:34:56.123Z",
        }
        values.update(changes)
        return NormalizedEvent(**values)  # type: ignore[arg-type]

    def test_event_is_immutable_and_metadata_is_canonical(self) -> None:
        event = self.event(attributes=(("z", 1), ("a", "first")))
        self.assertEqual(event.attributes, (("a", "first"), ("z", 1)))
        with self.assertRaises(FrozenInstanceError):
            event.summary = "changed"  # type: ignore[misc]

    def test_json_round_trip_is_deterministic_and_compact(self) -> None:
        event = self.event()
        encoded = event.to_json()
        self.assertEqual(encoded, event.to_json())
        self.assertNotIn(": ", encoded)
        self.assertEqual(NormalizedEvent.from_json(encoded), event)
        self.assertEqual(NormalizedEvent.from_dict(event.to_dict()), event)
        self.assertEqual(json.loads(encoded)["schema_version"], "1.0")

    def test_rejects_extra_missing_and_duplicate_wire_fields(self) -> None:
        payload = self.event().to_dict()
        payload["unexpected"] = True
        with self.assertRaises(ValueError):
            NormalizedEvent.from_dict(payload)
        payload = self.event().to_dict()
        del payload["summary"]
        with self.assertRaises(ValueError):
            NormalizedEvent.from_dict(payload)
        duplicate = self.event().to_json().replace(
            '"sequence":1', '"sequence":1,"sequence":2'
        )
        with self.assertRaises(ValueError):
            NormalizedEvent.from_json(duplicate)

    def test_rejects_non_finite_numbers_and_non_scalar_metadata(self) -> None:
        with self.assertRaises(ValueError):
            self.event(attributes=(("bad", float("nan")),))
        payload = self.event().to_dict()
        payload["attributes"] = {"bad": [1, 2]}
        with self.assertRaises(ValueError):
            NormalizedEvent.from_dict(payload)
        non_finite = self.event().to_json().replace('"depth":1', '"depth":NaN')
        with self.assertRaises(ValueError):
            NormalizedEvent.from_json(non_finite)

    def test_rejects_non_namespaced_type_invalid_sequence_and_uuid(self) -> None:
        with self.assertRaises(ValueError):
            self.event(event_type="started")
        with self.assertRaises(ValueError):
            self.event(sequence=True)
        with self.assertRaises(ValueError):
            self.event(sequence=0)
        with self.assertRaises(ValueError):
            self.event(session_id=UUID(int=0))

    def test_rejects_unsupported_version_and_oversized_json(self) -> None:
        payload = self.event().to_dict()
        payload["schema_version"] = "2.0"
        with self.assertRaises(ValueError):
            NormalizedEvent.from_dict(payload)
        with self.assertRaises(ValueError):
            NormalizedEvent.from_json(b" " * (MAX_EVENT_BYTES + 1))

    def test_rfc3339_helper_uses_canonical_utc(self) -> None:
        current = utc_now_rfc3339()
        self.assertEqual(validate_rfc3339_utc(current), current)
        self.assertTrue(current.endswith("Z"))
        for invalid in (
            "2026-08-21 12:00:00Z",
            "2026-08-21T12:00:00+00:00",
            "2026-02-30T12:00:00Z",
            "2026-08-21T12:00:60Z",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_rfc3339_utc(invalid)

    def test_adapter_uuid5_identity_round_trips(self) -> None:
        identifier = uuid5(NAMESPACE_URL, "external-source")
        event = self.event(session_id=identifier, event_id=identifier)
        self.assertEqual(NormalizedEvent.from_json(event.to_json()), event)

    def test_redacts_common_secret_forms_without_echoing_values(self) -> None:
        secrets = (
            "api_key=very-secret-value",
            "Authorization: Bearer abc.def-ghi_123",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.signature_value",
            "ghp_abcdefghijklmnopqrstuvwxyz123456",
            "github_pat_abcdefghijklmnopqrstuvwxyz_123456",
            "sk-proj-abcdefghijklmnopqrstuvwxyz123456",
        )
        text, metadata = redact_text(" ".join(secrets))
        for secret in secrets:
            sensitive = secret.split("=", 1)[-1]
            self.assertNotIn(sensitive, text)
        self.assertGreaterEqual(text.count("[REDACTED]"), len(secrets))
        details = dict(metadata)
        self.assertTrue(details["redacted"])
        self.assertGreaterEqual(details["redaction_count"], len(secrets))

    def test_redaction_is_fail_closed_and_length_bounded(self) -> None:
        text, metadata = redact_text(None)  # type: ignore[arg-type]
        self.assertEqual(text, "[REDACTED]")
        self.assertEqual(dict(metadata)["redaction_kinds"], "fail_closed")
        text, metadata = redact_text("x" * 100, max_length=20)
        self.assertEqual(len(text), 20)
        self.assertTrue(dict(metadata)["truncated"])


if __name__ == "__main__":
    unittest.main()
