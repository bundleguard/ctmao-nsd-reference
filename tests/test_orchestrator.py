"""Integration tests for two-thread orchestration and cleanup."""

from __future__ import annotations

import asyncio
import threading
import unittest
from unittest import mock
from uuid import uuid4

from ctmao_nsd import (
    OrchestrationCancelledError,
    Orchestrator,
    OrchestratorBusyError,
    OrchestratorClosedError,
    ResultStatus,
    RuntimeConfig,
    TaskSpec,
)
from ctmao_nsd.thread_manager import ThreadManager
from ctmao_nsd.types import EnvelopeKind, WorkerEnvelope


class OrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_workers_are_isolated_and_memory_is_synchronized(self) -> None:
        orchestrator = Orchestrator()
        try:
            report = await orchestrator.run(
                {
                    "A": TaskSpec("root-a", children=(TaskSpec("child-a"),)),
                    "B": TaskSpec("root-b", children=(TaskSpec("child-b"),)),
                }
            )
            self.assertEqual(set(report.results), {"A", "B"})
            self.assertTrue(all(item.succeeded for item in report.results.values()))
            identities = orchestrator.worker_identities()
            self.assertNotEqual(identities["A"], identities["B"])
            self.assertNotIn(threading.get_ident(), identities.values())
            self.assertEqual(orchestrator.synchronized_memory("A")["root_task"], "root-a")
            self.assertEqual(orchestrator.synchronized_memory("B")["root_task"], "root-b")
            self.assertGreater(report.memory_revisions["A"], 0)
        finally:
            await orchestrator.close()

    async def test_failure_in_one_worker_does_not_cancel_the_other(self) -> None:
        async with Orchestrator() as orchestrator:
            report = await orchestrator.run(
                {
                    "A": TaskSpec("root-a", children=(TaskSpec("bad", should_fail=True),)),
                    "B": TaskSpec("root-b", children=(TaskSpec("good", duration=0.02),)),
                }
            )
            self.assertEqual(report.results["A"].status, ResultStatus.FAILED)
            self.assertEqual(report.results["B"].status, ResultStatus.SUCCEEDED)

    async def test_timeout_is_contained_and_worker_remains_usable(self) -> None:
        config = RuntimeConfig(task_timeout=0.05)
        async with Orchestrator(("A",), config) as orchestrator:
            first = await orchestrator.run(
                {"A": TaskSpec("slow-root", children=(TaskSpec("slow", duration=0.2),))}
            )
            self.assertEqual(first.results["A"].status, ResultStatus.FAILED)
            self.assertEqual(first.results["A"].children[0].status, ResultStatus.TIMED_OUT)
            second = await orchestrator.run(
                {"A": TaskSpec("healthy-root", children=(TaskSpec("healthy"),))}
            )
            self.assertEqual(second.results["A"].status, ResultStatus.SUCCEEDED)

    async def test_width_violation_is_rejected_without_child_results(self) -> None:
        config = RuntimeConfig(max_children_per_agent=1)
        async with Orchestrator(("A",), config) as orchestrator:
            report = await orchestrator.run(
                {"A": TaskSpec("root", children=(TaskSpec("one"), TaskSpec("two")))}
            )
            result = report.results["A"]
            self.assertEqual(result.status, ResultStatus.REJECTED)
            self.assertEqual(result.children, ())

    async def test_nested_delegation_beyond_maximum_is_rejected(self) -> None:
        config = RuntimeConfig(max_delegation_depth=2)
        too_deep = TaskSpec(
            "depth-1",
            children=(
                TaskSpec(
                    "depth-2",
                    children=(TaskSpec("depth-3-rejected"),),
                ),
            ),
        )
        async with Orchestrator(("A",), config) as orchestrator:
            report = await orchestrator.run(
                {"A": TaskSpec("root-depth-0", children=(too_deep,))}
            )
            depth_two = report.results["A"].children[0].children[0]
            self.assertEqual(depth_two.status, ResultStatus.REJECTED)
            self.assertEqual(depth_two.children, ())

    async def test_report_mappings_are_read_only(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            report = await orchestrator.run({"A": TaskSpec("root")})
            with self.assertRaises(TypeError):
                report.results["A"] = report.results["A"]  # type: ignore[index]

    async def test_close_is_idempotent_and_rejects_new_work(self) -> None:
        orchestrator = Orchestrator(("A",))
        await orchestrator.start()
        await orchestrator.close()
        await orchestrator.close()
        self.assertFalse(any(t.name == "ctmao-worker-A" and t.is_alive() for t in threading.enumerate()))
        with self.assertRaises(OrchestratorClosedError):
            await orchestrator.run({"A": TaskSpec("late")})

    async def test_overlapping_runs_are_rejected_without_losing_first_result(self) -> None:
        config = RuntimeConfig(task_timeout=0.5)
        orchestrator = Orchestrator(("A",), config)
        try:
            active = asyncio.create_task(
                orchestrator.run(
                    {
                        "A": TaskSpec(
                            "active-root",
                            children=(TaskSpec("active-child", duration=0.1),),
                        )
                    }
                )
            )
            await asyncio.sleep(0.02)
            with self.assertRaises(OrchestratorBusyError):
                await orchestrator.run({"A": TaskSpec("overlap")})
            report = await asyncio.wait_for(active, timeout=1.0)
            self.assertEqual(report.results["A"].status, ResultStatus.SUCCEEDED)
        finally:
            await orchestrator.close()

    async def test_close_cancels_active_run_without_blocking_event_loop(self) -> None:
        config = RuntimeConfig(task_timeout=2.0)
        orchestrator = Orchestrator(("A",), config)
        active = asyncio.create_task(
            orchestrator.run(
                {
                    "A": TaskSpec(
                        "active-root",
                        children=(TaskSpec("active-child", duration=1.0),),
                    )
                }
            )
        )
        await asyncio.sleep(0.02)
        closing = asyncio.create_task(orchestrator.close())

        # This timer must run while close is waiting; a synchronous join in the
        # event-loop thread would prevent it from completing on time.
        await asyncio.wait_for(asyncio.sleep(0.02), timeout=0.08)
        await asyncio.wait_for(closing, timeout=1.0)
        with self.assertRaises(OrchestrationCancelledError):
            await asyncio.wait_for(active, timeout=1.0)
        self.assertFalse(
            any(
                thread.name == "ctmao-worker-A" and thread.is_alive()
                for thread in threading.enumerate()
            )
        )

    async def test_cancelled_run_blocks_reuse_until_late_result_drains(self) -> None:
        config = RuntimeConfig(task_timeout=0.5)
        orchestrator = Orchestrator(("A",), config)
        try:
            active = asyncio.create_task(
                orchestrator.run(
                    {
                        "A": TaskSpec(
                            "cancelled-root",
                            children=(TaskSpec("late-child", duration=0.1),),
                        )
                    }
                )
            )
            await asyncio.sleep(0.02)
            active.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await active
            with self.assertRaises(OrchestratorBusyError):
                await orchestrator.run({"A": TaskSpec("too-early")})

            await asyncio.sleep(0.15)
            report = await orchestrator.run({"A": TaskSpec("after-drain")})
            self.assertTrue(report.results["A"].succeeded)
        finally:
            await orchestrator.close()

    async def test_all_worker_commands_share_one_absolute_deadline(self) -> None:
        captured_deadlines: list[float | None] = []
        original_submit = ThreadManager.submit

        def recording_submit(
            manager: ThreadManager, worker_id: str, command: object
        ) -> None:
            captured_deadlines.append(command.deadline)  # type: ignore[attr-defined]
            original_submit(manager, worker_id, command)  # type: ignore[arg-type]

        async with Orchestrator(("A", "B")) as orchestrator:
            with mock.patch.object(ThreadManager, "submit", new=recording_submit):
                report = await orchestrator.run(
                    {"A": TaskSpec("root-a"), "B": TaskSpec("root-b")}
                )
        self.assertTrue(all(result.succeeded for result in report.results.values()))
        self.assertEqual(len(captured_deadlines), 2)
        self.assertIsNotNone(captured_deadlines[0])
        self.assertEqual(captured_deadlines[0], captured_deadlines[1])

    async def test_dispatcher_ignores_unknown_correlation(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            assert orchestrator._outbound is not None
            await orchestrator._outbound.put(
                WorkerEnvelope(
                    kind=EnvelopeKind.RESULT,
                    worker_id="A",
                    correlation_id=uuid4(),
                )
            )
            report = await orchestrator.run({"A": TaskSpec("real-root")})
            self.assertTrue(report.results["A"].succeeded)

    async def test_unknown_worker_is_rejected(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            with self.assertRaises(KeyError):
                await orchestrator.run({"B": TaskSpec("unknown")})
