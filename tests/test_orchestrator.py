"""Integration tests for two-thread orchestration and cleanup."""

from __future__ import annotations

import asyncio
import threading
import unittest

from ctmao_nsd import Orchestrator, OrchestratorClosedError, ResultStatus, RuntimeConfig, TaskSpec


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

    async def test_unknown_worker_is_rejected(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            with self.assertRaises(KeyError):
                await orchestrator.run({"B": TaskSpec("unknown")})
