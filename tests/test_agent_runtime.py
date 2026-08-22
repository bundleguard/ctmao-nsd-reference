"""Provider-neutral runtime contract tests."""

from __future__ import annotations

import unittest

from ctmao_nsd import ModelBackedRuntime, ModelResponse, Orchestrator, TaskSpec


class _FakeProvider:
    async def complete(self, request: object) -> ModelResponse:
        return ModelResponse("model result", "fake", "contract-test")


class AgentRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_model_runtime_is_optional_and_preserves_orchestration(self) -> None:
        async with Orchestrator(
            ("A",), runtime_factory=lambda: ModelBackedRuntime(_FakeProvider())
        ) as orchestrator:
            report = await orchestrator.run(
                {"A": TaskSpec("root", children=(TaskSpec("model task"),))}
            )
        self.assertTrue(report.results["A"].succeeded)
        self.assertEqual(report.results["A"].children[0].output, "model result")

    async def test_default_runtime_retains_v011_output(self) -> None:
        async with Orchestrator(("A",)) as orchestrator:
            report = await orchestrator.run(
                {"A": TaskSpec("root", children=(TaskSpec("child", value=7),))}
            )
        self.assertEqual(report.results["A"].children[0].output, "processed child: 7")


if __name__ == "__main__":
    unittest.main()
