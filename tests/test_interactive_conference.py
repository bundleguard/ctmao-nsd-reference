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
