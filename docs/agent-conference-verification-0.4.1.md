# Agent Conference 0.4.1 verification

Verified on Windows on 2026-10-25 using the bundled Python and the Codex
in-app browser. The working tree already contained v0.4 work; that work was
preserved. Runtime orchestration, worker ownership, result queues, and
normalized-event semantics were not changed by this presentation/compatibility
update. The prior presentation remains available through `view=v4`.

## Compatibility

- Installed `agent-conference@personal` version: `0.4.1`.
- Bridge protocol: `1.1`, checked at launch, session-file read, HTTP request,
  and response. Missing/wrong HTTP versions return a clear HTTP 409 error.
- All five installed plugin files matched the repository by SHA-256 after
  reloading through `codex plugin add agent-conference@personal --json`.
- A smoke session launched from the installed cache completed Researcher,
  Builder, Reviewer, Moderator, and `finish`. Each message used its role's
  accepted working receipt. Reviewer cited exactly Researcher and Builder;
  Moderator cited all three upstream public message receipts.
- Smoke messages were explicitly synthetic test inputs, not claims about real
  subagent execution. Host-fed reference digests remain host-relative evidence.

## Automated and browser checks

All required commands passed with `PYTHONPATH=src`:

```text
python -m unittest discover -s tests -v  # 131 tests passed
python main.py                         # both workers succeeded
python examples/two_threads.py         # A and B succeeded
git diff --check                       # no whitespace errors
```

The browser harness (`python tests/browser_dashboard_smoke.py`) passed 48/48
checks: 16 checks each at 1440px, 768px, and 360px. It uses the production HTML
with synthetic records and controlled speech callbacks, with no new dependency.
Coverage includes exact evidence links after out-of-order message delivery,
visible task/phase/failure state, source redaction badges, collapsed JSON,
responsive content priority, audio opt-in, queue versus actual start, pause,
mute, provider failure, voice exhaustion, unsupported audio, cancellation,
terminal polling, new-run reset, stale callbacks, duplicate/reconnected SSE,
late events from a retired run, and decision focus.
The HTTP suite also covers Windows aborted SSE sockets alongside broken pipes
and peer resets, so browser reload/disconnect does not print a handler traceback.

The installed-plugin live session was also visually inspected at desktop,
tablet, and 360px widths without horizontal overflow. Reload reconstructed four
messages and three accepted evidence connections with audio still off. Enter
on the Final decision button moved focus to the decision article.

Speech state was tested through provider callbacks. Audible output quality and
every operating-system speech provider were not tested. This is functional
accessibility coverage, not a formal accessibility certification.
