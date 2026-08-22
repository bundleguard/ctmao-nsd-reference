"""Run the truthful local live conference and browser-speech demonstration."""

from __future__ import annotations

import argparse
import asyncio

from ctmao_nsd import EventBus, Orchestrator, RuntimeConfig, RuntimeRequest, TaskSpec
from ctmao_nsd.live_ui import LiveDashboard


class ScriptedFindingRuntime:
    """Produce controlled demo findings without claiming external inspection."""

    async def execute(self, request: RuntimeRequest) -> str:
        if request.duration:
            await asyncio.sleep(request.duration)
        if request.should_fail:
            raise RuntimeError(f"task failed: {request.name}")
        return str(request.value) if request.value is not None else f"{request.name} complete"


async def run(hold_seconds: float = 30.0, *, wait_for_listener: bool = True) -> None:
    """Execute a controlled nested run and keep its dashboard briefly available."""
    bus = EventBus()
    dashboard = LiveDashboard(bus)
    print("Open the live conference:", dashboard.start())
    try:
        if wait_for_listener and hold_seconds > 0:
            print("Waiting up to 60 seconds for the live browser listener.")
            if not await asyncio.to_thread(dashboard.wait_for_listener, 60.0):
                print("No listener connected; continuing with the complete text transcript.")
        assignments = {
            "Backend": TaskSpec(
                "Demonstrate backend handoff",
                children=(
                    TaskSpec(
                        "Publish supplied schema finding",
                        value="The users table lacks a rollback-safe index.",
                        duration=0.2,
                        children=(
                            TaskSpec(
                                "Publish supplied safety check",
                                value=(
                                    "The index can be added concurrently without "
                                    "rewriting existing rows."
                                ),
                                duration=0.2,
                            ),
                        ),
                    ),
                ),
            ),
            "Test": TaskSpec(
                "Demonstrate test handoff",
                children=(
                    TaskSpec(
                        "Publish supplied regression finding",
                        value=(
                            "Upgrade and rollback fixtures preserve all existing records."
                        ),
                        duration=0.3,
                    ),
                ),
            ),
            "Audit": TaskSpec(
                "Exercise failure containment",
                children=(
                    TaskSpec(
                        "Simulate rejected migration",
                        value="The unsafe migration is rejected without stopping peer work.",
                        should_fail=True,
                    ),
                ),
            ),
        }
        config = RuntimeConfig(publish_result_summaries=True)
        async with Orchestrator(
            ("Backend", "Test", "Audit"),
            config,
            event_bus=bus,
            runtime_factory=ScriptedFindingRuntime,
        ) as orchestrator:
            report = await orchestrator.run(assignments)
            print(
                "Run",
                report.run_id,
                "completed with",
                len(report.normalized_events),
                "source-backed events.",
            )
            for worker_id, result in report.results.items():
                print(worker_id, "succeeded" if result.succeeded else "failed")
        if hold_seconds > 0:
            print(f"Dashboard remains available for {hold_seconds:g} seconds.")
            await asyncio.sleep(hold_seconds)
    finally:
        dashboard.close()
        bus.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hold", type=float, default=30.0)
    args = parser.parse_args()
    hold = max(0.0, args.hold)
    asyncio.run(run(hold, wait_for_listener=hold > 0))


if __name__ == "__main__":
    main()
