"""Interactive multi-round conference tests."""

from __future__ import annotations

import threading
import unittest

from ctmao_nsd import (
    DemoConferenceProvider,
    EventBus,
    InteractiveConference,
    ModelResponse,
)


class InteractiveConferenceTests(unittest.TestCase):
    def test_objective_is_redacted_in_public_snapshot(self) -> None:
        bus = EventBus()
        conference = InteractiveConference(
            bus, DemoConferenceProvider(), provider_name="demo", task_timeout=2.0
        )
        try:
            state = conference.start("Review password=hunter2")
            self.assertNotIn("hunter2", state["objective"])
            self.assertIn("[REDACTED]", state["objective"])
        finally:
            conference.close()
            bus.close()

    def test_each_participant_requires_exactly_one_provider_call(self) -> None:
        calls: list[str] = []

        class CountingProvider:
            async def complete(self, request: object) -> ModelResponse:
                calls.append(request.worker_id)
                return ModelResponse(
                    f"public turn from {request.worker_id}", "test", "counting"
                )

        bus = EventBus()
        conference = InteractiveConference(
            bus, CountingProvider(), provider_name="counting", task_timeout=2.0
        )
        try:
            conference.start("Count provider calls")
            self.assertTrue(conference.wait(5.0))
            self.assertEqual(len(calls), 4)
            self.assertEqual(set(calls[:2]), {"Researcher", "Builder"})
            self.assertEqual(calls[2:], ["Reviewer", "Moderator"])
        finally:
            conference.close()
            bus.close()

    def test_demo_conference_exchanges_four_genuine_public_messages(self) -> None:
        bus = EventBus()
        received = []
        delivered = threading.Event()

        def accept(event: object) -> None:
            if event.event_type == "message.sent":
                received.append(event)
                if len(received) == 4:
                    delivered.set()

        token = bus.subscribe(accept)
        conference = InteractiveConference(
            bus, DemoConferenceProvider(), provider_name="demo", task_timeout=2.0
        )
        try:
            state = conference.start("Plan a safe release")
            self.assertEqual(state["status"], "queued")
            self.assertTrue(conference.wait(5.0))
            self.assertTrue(delivered.wait(2.0))
            self.assertEqual(conference.snapshot()["status"], "completed")
            speakers = [dict(event.attributes)["speaker_id"] for event in received]
            self.assertEqual(set(speakers[:2]), {"Researcher", "Builder"})
            self.assertEqual(speakers[2:], ["Reviewer", "Moderator"])
            self.assertTrue(all(event.summary for event in received))
            self.assertTrue(all(event.run_id is not None for event in received))
        finally:
            conference.close()
            bus.unsubscribe(token)
            bus.close()

    def test_end_to_end_causality_is_research_and_build_then_review_then_moderate(self) -> None:
        bus = EventBus()
        received = []
        all_events = []
        delivered = threading.Event()

        def accept(event: object) -> None:
            all_events.append(event)
            if event.event_type == "message.sent":
                received.append(event)
                if len(received) == 4:
                    delivered.set()

        token = bus.subscribe(accept)
        conference = InteractiveConference(
            bus, DemoConferenceProvider(), provider_name="demo", task_timeout=2.0
        )
        try:
            conference.start("Audit the causal conference graph")
            self.assertTrue(conference.wait(5.0))
            self.assertTrue(delivered.wait(2.0))
            messages = {
                dict(event.attributes)["speaker_id"]: event for event in received
            }
            research = messages["Researcher"]
            builder = messages["Builder"]
            reviewer = messages["Reviewer"]
            moderator = messages["Moderator"]

            self.assertIsNotNone(research.correlation_id)
            self.assertIsNotNone(builder.correlation_id)
            self.assertIsNotNone(reviewer.correlation_id)
            self.assertIsNotNone(moderator.correlation_id)
            self.assertNotEqual(research.correlation_id, builder.correlation_id)
            events_by_id = {event.event_id: event for event in all_events}
            for message in (research, builder, reviewer, moderator):
                self.assertIsNotNone(message.causation_event_id)
                cause = events_by_id[message.causation_event_id]
                self.assertEqual(cause.event_type, "task.started")
                self.assertEqual(cause.task_id, message.task_id)
                self.assertEqual(cause.correlation_id, message.correlation_id)

            review_sources = set(
                dict(reviewer.provenance)["source_event_ids"].split(",")
            )
            self.assertEqual(
                review_sources, {str(research.event_id), str(builder.event_id)}
            )

            moderation_sources = set(
                dict(moderator.provenance)["source_event_ids"].split(",")
            )
            self.assertEqual(
                moderation_sources,
                {str(research.event_id), str(builder.event_id), str(reviewer.event_id)},
            )
        finally:
            conference.close()
            bus.unsubscribe(token)
            bus.close()

    def test_rejects_empty_and_oversized_objectives(self) -> None:
        bus = EventBus()
        conference = InteractiveConference(
            bus, DemoConferenceProvider(), provider_name="demo"
        )
        try:
            with self.assertRaises(ValueError):
                conference.start(" ")
            with self.assertRaises(ValueError):
                conference.start("x" * 4001)
        finally:
            conference.close()
            bus.close()

    def test_reviewer_failure_degrades_to_a_completed_conference_with_warning(self) -> None:
        calls: list[str] = []

        class PartialProvider:
            async def complete(self, request: object) -> ModelResponse:
                calls.append(request.worker_id)
                if request.worker_id == "Reviewer":
                    raise RuntimeError("review provider unavailable")
                return ModelResponse(
                    f"public turn from {request.worker_id}", "test", "partial"
                )

        bus = EventBus()
        conference = InteractiveConference(
            bus, PartialProvider(), provider_name="partial", task_timeout=2.0
        )
        try:
            conference.start("Continue safely after one participant fails")
            self.assertTrue(conference.wait(5.0))
            state = conference.snapshot()
            self.assertEqual(state["status"], "completed")
            self.assertIn("Reviewer could not contribute", state["warning"])
            self.assertEqual(calls[-1], "Moderator")
            reviewer = next(
                item
                for item in state["participants"]
                if item["participant_id"] == "Reviewer"
            )
            self.assertEqual(reviewer["state"], "failed")
        finally:
            conference.close()
            bus.close()

    def test_queued_or_running_conference_can_be_cancelled(self) -> None:
        release = threading.Event()
        entered = threading.Event()

        class SlowProvider:
            async def complete(self, request: object) -> ModelResponse:
                import asyncio

                entered.set()
                await asyncio.to_thread(release.wait, 2.0)
                return ModelResponse("late", "test", "slow")

        bus = EventBus()
        conference = InteractiveConference(
            bus, SlowProvider(), provider_name="slow", task_timeout=3.0
        )
        try:
            conference.start("cancel this run")
            self.assertTrue(entered.wait(1.0))
            state = conference.cancel()
            self.assertIn(state["status"], {"cancelling", "cancelled"})
            self.assertTrue(conference.wait(2.0))
            self.assertEqual(conference.snapshot()["status"], "cancelled")
        finally:
            release.set()
            conference.close()
            bus.close()


if __name__ == "__main__":
    unittest.main()
