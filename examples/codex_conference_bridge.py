"""Run or feed the v0.4 Codex-native conference bridge.

The ``serve`` command is the loopback dashboard process.  The remaining
commands are intentionally small host adapters used by the Codex plugin after
it observes genuine public subagent results.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import tempfile
import time
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ctmao_nsd import (
    CODEX_BRIDGE_PROTOCOL_VERSION,
    CodexConferenceBridge,
    EventBus,
)
from ctmao_nsd.live_ui import LiveDashboard


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve")
    serve.add_argument("--objective", required=True)
    serve.add_argument(
        "--terminal-hold",
        type=float,
        default=0.0,
        help="seconds to remain available after completion; zero waits for Ctrl+C",
    )
    serve.add_argument("--plugin-protocol-version")

    participant = commands.add_parser("participant")
    participant.add_argument("--session-file", required=True)
    participant.add_argument("--participant", required=True)
    participant.add_argument("--state", required=True)
    participant.add_argument("--source-ref", required=True)
    participant.add_argument("--detail")
    participant.add_argument("--plugin-protocol-version")

    message = commands.add_parser("message")
    message.add_argument("--session-file", required=True)
    message.add_argument("--participant", required=True)
    message.add_argument("--kind", required=True)
    message.add_argument("--source-ref", required=True)
    message.add_argument("--text", required=True)
    message.add_argument("--recipient", default="conference")
    message.add_argument("--causation-event-id")
    message.add_argument("--source-event-id", action="append", default=[])
    message.add_argument("--plugin-protocol-version")

    finish = commands.add_parser("finish")
    finish.add_argument("--session-file", required=True)
    finish.add_argument("--status", default="completed")
    finish.add_argument("--source-ref", required=True)
    finish.add_argument("--detail")
    finish.add_argument("--plugin-protocol-version")

    args = parser.parse_args()
    if args.command == "serve":
        _serve(args.objective, args.terminal_hold, args.plugin_protocol_version)
    elif args.command == "participant":
        _post(
            args.session_file,
            "/api/bridge/participants",
            {
                "participant_id": args.participant,
                "state": args.state,
                "source_ref": args.source_ref,
                "detail": args.detail,
            },
            plugin_protocol_version=args.plugin_protocol_version,
        )
    elif args.command == "message":
        _post(
            args.session_file,
            "/api/bridge/messages",
            {
                "participant_id": args.participant,
                "message_kind": args.kind,
                "source_ref": args.source_ref,
                "recipient_id": args.recipient,
                "causation_event_id": args.causation_event_id,
                "source_event_ids": args.source_event_id,
                "text": args.text,
            },
            plugin_protocol_version=args.plugin_protocol_version,
        )
    else:
        _post(
            args.session_file,
            "/api/bridge/finish",
            {
                "status": args.status,
                "source_ref": args.source_ref,
                "detail": args.detail,
            },
            plugin_protocol_version=args.plugin_protocol_version,
        )


def _serve(
    objective: str,
    terminal_hold: float,
    plugin_protocol_version: str | None,
) -> None:
    if terminal_hold < 0:
        raise SystemExit("--terminal-hold cannot be negative")
    _require_protocol_version(plugin_protocol_version)
    bus = EventBus()
    bridge = CodexConferenceBridge(bus)
    dashboard = LiveDashboard(bus, control=bridge)
    session_path: Path | None = None
    terminal_since: float | None = None
    try:
        dashboard.start()
        bridge.start(objective)
        session_path = _write_session_file(dashboard.url)
        output = {
            "conference_url": dashboard.url,
            "session_file": str(session_path),
            "bridge_protocol_version": CODEX_BRIDGE_PROTOCOL_VERSION,
        }
        print(json.dumps(output, ensure_ascii=False), flush=True)
        while True:
            status = bridge.snapshot()["status"]
            if status in {"completed", "failed", "cancelled"} and terminal_hold > 0:
                terminal_since = terminal_since or time.monotonic()
                if time.monotonic() - terminal_since >= terminal_hold:
                    return
            time.sleep(0.25)
    except KeyboardInterrupt:
        pass
    finally:
        dashboard.close()
        bus.close()
        if session_path is not None:
            try:
                session_path.unlink()
            except FileNotFoundError:
                pass


def _write_session_file(conference_url: str) -> Path:
    handle, raw_path = tempfile.mkstemp(prefix="ctmao-conference-", suffix=".json")
    path = Path(raw_path)
    parsed = urlparse(conference_url)
    query = parse_qs(parsed.query, strict_parsing=True)
    tokens = query.get("token", ())
    if len(tokens) != 1 or not tokens[0]:
        os.close(handle)
        path.unlink(missing_ok=True)
        raise ValueError("conference URL does not contain one valid token")
    value = {
        "origin": f"{parsed.scheme}://{parsed.netloc}",
        "token": tokens[0],
        "conference_url": conference_url,
        "bridge_protocol_version": CODEX_BRIDGE_PROTOCOL_VERSION,
    }
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, separators=(",", ":"))
        os.chmod(path, 0o600)
    except Exception:
        try:
            os.close(handle)
        except OSError:
            pass
        path.unlink(missing_ok=True)
        raise
    return path


def _post(
    session_file: str,
    endpoint: str,
    payload: dict[str, object],
    *,
    plugin_protocol_version: str | None = None,
) -> None:
    _require_protocol_version(plugin_protocol_version)
    session_path = Path(session_file).resolve()
    if not session_path.is_file():
        raise SystemExit("conference session file does not exist")
    if session_path.stat().st_size > 8192:
        raise SystemExit("conference session file is too large")
    value = json.loads(session_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SystemExit("conference session file is invalid")
    origin = value.get("origin")
    token = value.get("token")
    session_protocol_version = value.get("bridge_protocol_version")
    if session_protocol_version != CODEX_BRIDGE_PROTOCOL_VERSION:
        raise SystemExit(
            "conference session protocol is incompatible with this bridge; "
            "restart the conference with the matching Agent Conference plugin"
        )
    origin = _validated_loopback_origin(origin)
    if not isinstance(token, str) or not token or len(token) > 512:
        raise SystemExit("conference session token is invalid")
    body = json.dumps(
        {key: item for key, item in payload.items() if item is not None},
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    request = Request(
        f"{origin}{endpoint}?{urlencode({'token': token})}",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-CTMAO-Bridge-Protocol": CODEX_BRIDGE_PROTOCOL_VERSION,
        },
    )
    try:
        with urlopen(request, timeout=5.0) as response:
            result = json.loads(response.read(65536))
    except HTTPError as exc:
        if exc.code == 409:
            raise SystemExit(
                "Agent Conference bridge rejected the plugin protocol; "
                "reinstall the matching plugin and restart the conference"
            ) from None
        raise
    if result.get("bridge_protocol_version") != CODEX_BRIDGE_PROTOCOL_VERSION:
        raise SystemExit("Agent Conference bridge response protocol is incompatible")
    print(json.dumps(result, ensure_ascii=False), flush=True)


def _require_protocol_version(value: object) -> None:
    if value == CODEX_BRIDGE_PROTOCOL_VERSION:
        return
    supplied = value if isinstance(value, str) and value else "missing"
    raise SystemExit(
        "Agent Conference plugin protocol "
        f"{supplied} is incompatible with bridge protocol "
        f"{CODEX_BRIDGE_PROTOCOL_VERSION}; reinstall the plugin from this repository"
    )


def _validated_loopback_origin(value: object) -> str:
    if not isinstance(value, str):
        raise SystemExit("conference session origin is not loopback")
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError:
        raise SystemExit("conference session origin is not loopback") from None
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.username is not None
        or parsed.password is not None
        or port is None
        or not 1 <= port <= 65535
        or parsed.netloc != f"127.0.0.1:{port}"
        or parsed.path
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise SystemExit("conference session origin is not loopback")
    return f"http://127.0.0.1:{port}"


if __name__ == "__main__":
    main()
