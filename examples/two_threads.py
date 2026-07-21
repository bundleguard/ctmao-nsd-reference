"""Minimal library usage example."""

import asyncio

from ctmao_nsd import Orchestrator, TaskSpec


async def run() -> None:
    """Submit one nested task tree to each isolated worker."""
    jobs = {
        "A": TaskSpec("A", children=(TaskSpec("A1", children=(TaskSpec("A2"),)),)),
        "B": TaskSpec("B", children=(TaskSpec("B1", children=(TaskSpec("B2"),)),)),
    }
    async with Orchestrator() as orchestrator:
        report = await orchestrator.run(jobs)
        print({worker: result.status.value for worker, result in report.results.items()})


if __name__ == "__main__":
    asyncio.run(run())
