"""Launch the v0.3 interactive local agent conference."""

from __future__ import annotations

import argparse
import os
import time

from ctmao_nsd import (
    DemoConferenceProvider,
    EventBus,
    InteractiveConference,
    OpenAIResponsesProvider,
)
from ctmao_nsd.live_ui import LiveDashboard


ROLE_INSTRUCTIONS = {
    "Researcher": "Identify relevant facts, assumptions, evidence needs, and uncertainty.",
    "Builder": "Propose a concrete, bounded, reversible implementation approach.",
    "Reviewer": "Review the other public messages, challenge gaps, and specify tests.",
    "Moderator": "Synthesize the public discussion into a concise decision and next steps.",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=("demo", "openai"), default="demo")
    parser.add_argument("--model", default="gpt-5.4")
    parser.add_argument(
        "--hold",
        type=float,
        default=0.0,
        help="seconds to keep the server open; zero means until Ctrl+C",
    )
    args = parser.parse_args()

    if args.provider == "openai":
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise SystemExit("OPENAI_API_KEY is required for --provider openai")
        provider = OpenAIResponsesProvider(
            api_key,
            model=args.model,
            role_instructions=ROLE_INSTRUCTIONS,
        )
        provider_name = f"OpenAI Responses API · {args.model}"
    else:
        provider = DemoConferenceProvider()
        provider_name = "Deterministic demo (no model call)"

    bus = EventBus()
    conference = InteractiveConference(bus, provider, provider_name=provider_name)
    dashboard = LiveDashboard(bus, control=conference)
    print("Open the interactive conference:", dashboard.start(), flush=True)
    print("Press Ctrl+C to stop the local server.", flush=True)
    try:
        if args.hold > 0:
            time.sleep(args.hold)
        else:
            while True:
                time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        dashboard.close()
        conference.close()
        bus.close()


if __name__ == "__main__":
    main()

