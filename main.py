"""Run the deterministic two-thread CTMAO-NSD demonstration."""

from __future__ import annotations

import asyncio

from ctmao_nsd import Orchestrator, TaskResult, TaskSpec
from ctmao_nsd.logging_config import configure_logging


def _print_tree(result: TaskResult, indent: str = "") -> None:
    """Render a compact, deterministic view of an aggregated result tree."""
    print(f"{indent}- {result.name}: {result.status.value} [{'/'.join(result.agent_path)}]")
    for child in result.children:
        _print_tree(child, indent + "  ")


async def main() -> None:
    """Run two isolated supervisors, each with a nested child branch."""
    configure_logging()
    assignments = {
        "A": TaskSpec(
            "Thread A root",
            children=(
                TaskSpec(
                    "Child A1",
                    value="analyze alpha",
                    children=(TaskSpec("Child A2", value="verify alpha"),),
                ),
            ),
        ),
        "B": TaskSpec(
            "Thread B root",
            children=(
                TaskSpec(
                    "Child B1",
                    value="analyze beta",
                    children=(TaskSpec("Child B2", value="verify beta"),),
                ),
            ),
        ),
    }

    async with Orchestrator(("A", "B")) as orchestrator:
        report = await orchestrator.run(assignments)
        for worker_id, result in sorted(report.results.items()):
            print(f"\nWorker {worker_id}")
            _print_tree(result)
            print("Synchronized memory:", orchestrator.synchronized_memory(worker_id))


if __name__ == "__main__":
    asyncio.run(main())
