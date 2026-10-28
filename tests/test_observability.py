"""Normalized runtime instrumentation and failure-isolation tests."""

from __future__ import annotations

import asyncio
import threading
import time
import unittest
from uuid import uuid4

from ctmao_nsd import EventBus, EventProjection, Orchestrator, RuntimeConfig, TaskSpec
from ctmao_nsd.normalized_events import (
    EvidenceKind,
    EventImportance,
    EventOrigin,
    NormalizedEvent,
)


_BUS_SESSION = uuid4()


def _bus_event(sequence: int, importance: EventImportance) -> NormalizedEvent:
    return NormalizedEvent(
        "task.started",
        _BUS_SESSION,
        sequence,
        f"event {sequence}",
        EventOrigin.ORCHESTRATOR,
        EvidenceKind.OBSERVED,
        importance=importance,
    )


class ObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_nested_events_are_run_scoped_ordered_and_source_backed(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            first = await orchestrator.run(
                {
                    "A": TaskSpec(
                        "root",
                        children=(TaskSpec("child", children=(TaskSpec("nested"),)),),
                    )
                }
            )
            second = await orchestrator.run({"A": TaskSpec("second")})

        first_types = [event.event_type for event in first.normalized_events]
        self.assertIn("orchestration.run.started", first_types)
        self.assertIn("delegation.created", first_types)
        self.assertIn("task.completed", first_types)
        self.assertIn("orchestration.run.completed", first_types)
        self.assertEqual(first.run_id, first.normalized_events[0].run_id)
        self.assertTrue(all(event.run_id == first.run_id for event in first.normalized_events))
        self.assertNotEqual(first.run_id, second.run_id)
        self.assertTrue(all(event.run_id == second.run_id for event in second.normalized_events))
        sequences = [event.sequence for event in first.normalized_events]
        self.assertEqual(sequences, sorted(sequences))
        self.assertEqual(len(sequences), len(set(sequences)))

    async def test_failing_subscriber_cannot_fail_or_delay_a_run(self) -> None:
        bus = EventBus()
        observed = threading.Event()

        def broken(event: object) -> None:
            observed.set()
            raise RuntimeError("observer failed")

        bus.subscribe(broken, capacity=2)
        try:
            async with Orchestrator(("A",), event_bus=bus) as orchestrator:
                report = await asyncio.wait_for(
                    orchestrator.run({"A": TaskSpec("root")}), timeout=1.0
                )
            self.assertTrue(report.results["A"].succeeded)
            self.assertTrue(await asyncio.to_thread(observed.wait, 0.5))
            for _ in range(20):
                if bus.diagnostics()["failures"]:
                    break
                await asyncio.sleep(0.01)
            self.assertGreater(bus.diagnostics()["failures"], 0)
        finally:
            bus.close()

    async def test_fanout_enqueue_latency_cannot_change_run_result(self) -> None:
        bus = EventBus()
        token = bus.subscribe(lambda event: None, capacity=2)
        subscriber = bus._subscribers[token]
        original_enqueue = subscriber.enqueue

        def delayed_enqueue(event: NormalizedEvent) -> None:
            time.sleep(0.1)
            original_enqueue(event)

        subscriber.enqueue = delayed_enqueue  # type: ignore[method-assign]
        try:
            config = RuntimeConfig(task_timeout=0.05)
            async with Orchestrator(("A",), config, event_bus=bus) as orchestrator:
                report = await asyncio.wait_for(
                    orchestrator.run(
                        {"A": TaskSpec("root", children=(TaskSpec("child"),))}
                    ),
                    timeout=0.5,
                )
            self.assertTrue(report.results["A"].succeeded)
        finally:
            bus.close()

    async def test_projection_contains_detached_task_and_timeline_state(self) -> None:
        projection = EventProjection()
        bus = EventBus()
        bus.subscribe(projection.apply)
        try:
            async with Orchestrator(("A",), event_bus=bus) as orchestrator:
                report = await orchestrator.run(
                    {"A": TaskSpec("root", children=(TaskSpec("child"),))}
                )
            for _ in range(20):
                if projection.snapshot().latest_sequence >= report.normalized_events[-1].sequence:
                    break
                await asyncio.sleep(0.01)
            snapshot = projection.snapshot()
            self.assertEqual(snapshot.run_status, "completed")
            self.assertGreaterEqual(len(snapshot.tasks), 2)
            self.assertTrue(snapshot.timeline)
        finally:
            bus.close()

    async def test_secret_in_task_name_is_redacted_before_normalized_output(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            report = await orchestrator.run(
                {"A": TaskSpec("root", children=(TaskSpec("api_key=very-secret"),))}
            )
        serialized = "\n".join(event.to_json() for event in report.normalized_events)
        self.assertNotIn("very-secret", serialized)
        self.assertIn("[REDACTED]", serialized)

    async def test_long_task_label_cannot_break_result_dispatch(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            report = await asyncio.wait_for(
                orchestrator.run(
                    {"A": TaskSpec("root", children=(TaskSpec("x" * 500),))}
                ),
                timeout=1.0,
            )
        self.assertTrue(report.results["A"].succeeded)

    async def test_projection_resets_between_runs(self) -> None:
        projection = EventProjection()
        async with Orchestrator(("A",)) as orchestrator:
            first = await orchestrator.run({"A": TaskSpec("first")})
            second = await orchestrator.run({"A": TaskSpec("second")})
        for event in (*first.normalized_events, *second.normalized_events):
            projection.apply(event)
        snapshot = projection.snapshot()
        self.assertEqual(snapshot.run_id, second.run_id)
        self.assertEqual(set(snapshot.tasks), {str(second.results["A"].task_id)})
        self.assertTrue(
            all(item["event_id"] in {str(e.event_id) for e in second.normalized_events}
                for item in snapshot.timeline)
        )

    async def test_public_result_findings_are_explicit_opt_in(self) -> None:
        task = TaskSpec("root", children=(TaskSpec("inspect", value="safe finding"),))
        async with Orchestrator(("A",)) as orchestrator:
            private = await orchestrator.run({"A": task})
        self.assertNotIn(
            "safe finding", "\n".join(e.to_json() for e in private.normalized_events)
        )

        config = RuntimeConfig(publish_result_summaries=True)
        async with Orchestrator(("A",), config) as orchestrator:
            public = await orchestrator.run({"A": task})
        event_types = [event.event_type for event in public.normalized_events]
        self.assertIn("finding.reported", event_types)
        self.assertIn("delegation.completed", event_types)
        self.assertIn(
            "safe finding", "\n".join(e.to_json() for e in public.normalized_events)
        )

    async def test_agent_message_requires_config_and_per_task_authorization(self) -> None:
        task = TaskSpec(
            "root",
            children=(
                TaskSpec(
                    "public contribution",
                    value="public message",
                    publish_output=True,
                    message_kind="analysis",
                ),
            ),
        )
        async with Orchestrator(("Researcher",)) as orchestrator:
            private = await orchestrator.run({"Researcher": task})
        self.assertNotIn(
            "message.sent", [event.event_type for event in private.normalized_events]
        )

        config = RuntimeConfig(publish_agent_messages=True)
        async with Orchestrator(("Researcher",), config) as orchestrator:
            public = await orchestrator.run({"Researcher": task})
        message = next(
            event for event in public.normalized_events if event.event_type == "message.sent"
        )
        self.assertIn("public message", message.summary)
        self.assertEqual(dict(message.attributes)["speaker_id"], "Researcher")
        self.assertEqual(dict(message.attributes)["message_kind"], "analysis")

    async def test_upstream_evidence_must_be_a_prior_session_public_message(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            first = await orchestrator.run({"A": TaskSpec("first")})
            lifecycle = next(
                event
                for event in first.normalized_events
                if event.event_type == "task.started"
            )
            with self.assertRaisesRegex(ValueError, "must cite public messages"):
                await orchestrator.run(
                    {
                        "A": TaskSpec(
                            "invalid upstream",
                            upstream_event_ids=(lifecycle.event_id,),
                        )
                    }
                )

        foreign_bus = EventBus()
        try:
            async with Orchestrator(("A",), event_bus=foreign_bus) as first_orchestrator:
                foreign = await first_orchestrator.run({"A": TaskSpec("foreign")})
            foreign_id = next(
                event.event_id
                for event in foreign.normalized_events
                if event.event_type == "task.started"
            )
            async with Orchestrator(("A",)) as second_orchestrator:
                with self.assertRaisesRegex(ValueError, "unknown upstream"):
                    await second_orchestrator.run(
                        {
                            "A": TaskSpec(
                                "cross session",
                                upstream_event_ids=(foreign_id,),
                            )
                        }
                    )
        finally:
            foreign_bus.close()

    async def test_duplicate_task_ids_are_rejected_before_ambiguous_lifecycle(self) -> None:
        duplicate_id = uuid4()
        task = TaskSpec(
            "root",
            children=(
                TaskSpec("first", task_id=duplicate_id),
                TaskSpec("second", task_id=duplicate_id),
            ),
        )
        async with Orchestrator(("A",)) as orchestrator:
            with self.assertRaisesRegex(ValueError, "task IDs must be unique"):
                await orchestrator.run({"A": task})

    async def test_duplicate_task_ids_across_workers_are_rejected_before_scheduling(self) -> None:
        duplicate_id = uuid4()
        async with Orchestrator(("A", "B")) as orchestrator:
            with self.assertRaisesRegex(ValueError, "unique across run assignments"):
                await orchestrator.run(
                    {
                        "A": TaskSpec("worker A", task_id=duplicate_id),
                        "B": TaskSpec("worker B", task_id=duplicate_id),
                    }
                )

    async def test_all_upstream_evidence_is_preflighted_before_any_worker_submission(self) -> None:
        unknown_event_id = uuid4()
        orchestrator = Orchestrator(("A", "B"))
        try:
            with self.assertRaisesRegex(ValueError, "unknown upstream"):
                await orchestrator.run(
                    {
                        "A": TaskSpec("valid first assignment", value="must not run"),
                        "B": TaskSpec(
                            "invalid second assignment",
                            upstream_event_ids=(unknown_event_id,),
                        ),
                    }
                )
            recorded = tuple(
                event
                for events in orchestrator._normalized_by_run.values()
                for event in events
            )
            self.assertNotIn("task.assigned", {event.event_type for event in recorded})
            self.assertEqual(orchestrator._pending, {})
            self.assertEqual(orchestrator.synchronized_memory("A"), {})
            self.assertEqual(orchestrator.synchronized_memory("B"), {})
        finally:
            await orchestrator.close()


class EventBusLifecycleTests(unittest.TestCase):
    def test_lower_priority_event_cannot_displace_critical_backlog(self) -> None:
        bus = EventBus()
        entered = threading.Event()
        release = threading.Event()
        delivered: list[int] = []

        def callback(event: NormalizedEvent) -> None:
            delivered.append(event.sequence)
            if event.sequence == 1:
                entered.set()
                release.wait(1.0)

        bus.subscribe(callback, capacity=2)
        try:
            bus.publish(_bus_event(1, EventImportance.ROUTINE))
            self.assertTrue(entered.wait(0.5))
            bus.publish(_bus_event(2, EventImportance.CRITICAL))
            bus.publish(_bus_event(3, EventImportance.CRITICAL))
            bus.publish(_bus_event(4, EventImportance.IMPORTANT))
            release.set()
            for _ in range(50):
                if len(delivered) >= 3:
                    break
                time.sleep(0.01)
            self.assertEqual(delivered, [1, 2, 3])
            self.assertEqual(bus.diagnostics()["dropped"], 1)
        finally:
            release.set()
            bus.close()

    def test_higher_priority_event_evicts_only_lower_priority_work(self) -> None:
        bus = EventBus()
        entered = threading.Event()
        release = threading.Event()
        delivered: list[int] = []

        def callback(event: NormalizedEvent) -> None:
            delivered.append(event.sequence)
            if event.sequence == 1:
                entered.set()
                release.wait(1.0)

        bus.subscribe(callback, capacity=2)
        try:
            bus.publish(_bus_event(1, EventImportance.ROUTINE))
            self.assertTrue(entered.wait(0.5))
            bus.publish(_bus_event(2, EventImportance.ROUTINE))
            bus.publish(_bus_event(3, EventImportance.DEBUG))
            bus.publish(_bus_event(4, EventImportance.IMPORTANT))
            release.set()
            for _ in range(50):
                if len(delivered) >= 3:
                    break
                time.sleep(0.01)
            self.assertEqual(delivered, [1, 2, 4])
        finally:
            release.set()
            bus.close()

    def test_unsubscribe_discards_queued_callbacks(self) -> None:
        bus = EventBus()
        entered = threading.Event()
        release = threading.Event()
        delivered: list[int] = []

        def callback(event: NormalizedEvent) -> None:
            delivered.append(event.sequence)
            entered.set()
            release.wait(1.0)

        token = bus.subscribe(callback, capacity=2)
        bus.publish(_bus_event(1, EventImportance.ROUTINE))
        self.assertTrue(entered.wait(0.5))
        bus.publish(_bus_event(2, EventImportance.ROUTINE))
        closing = threading.Thread(target=bus.unsubscribe, args=(token,))
        closing.start()
        time.sleep(0.02)
        release.set()
        closing.join(0.5)
        try:
            self.assertFalse(closing.is_alive())
            self.assertEqual(delivered, [1])
        finally:
            release.set()
            bus.close()


if __name__ == "__main__":
    unittest.main()
