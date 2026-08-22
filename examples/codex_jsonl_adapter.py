"""Normalize a ``codex exec --json`` stream from stdin without launching Codex."""

from __future__ import annotations

import argparse
import sys

from ctmao_nsd import CodexExecJsonlAdapter


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--include-message-text",
        action="store_true",
        help="include redacted user-visible agent messages (off by default)",
    )
    args = parser.parse_args()
    adapter = CodexExecJsonlAdapter(include_message_text=args.include_message_text)
    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            result = adapter.ingest_line(line)
            for event in result.events:
                print(event.to_json(), flush=True)
    except ValueError as exc:
        print(f"adapter rejected a record: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
