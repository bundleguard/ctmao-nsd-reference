# Agent Conference 0.4.2 verification

Verified on Windows on 2026-10-28 using Codex's bundled Python, the installed
personal Agent Conference plugin, and the Codex in-app browser. The pre-existing
uncommitted v0.4 work was preserved. Runtime orchestration, worker ownership,
result-envelope consumption, and normalized-event semantics were not changed.

## Conference and compatibility

- A genuine bounded Codex conference accepted Researcher, Builder, Reviewer,
  and Moderator public messages. Each message cited its accepted working event;
  Reviewer cited the exact Researcher and Builder message receipts; Moderator
  cited the exact Researcher, Builder, and Reviewer receipts. The loopback view
  later expired while the task spanned sessions, before a terminal event could
  be posted; the separate wrapper smoke below proves terminal completion.
- Repository plugin, configured personal-marketplace source, and installed
  `agent-conference@personal` 0.4.2 cache matched across all five files by
  SHA-256.
- Plugin/bridge protocol remains `1.1` and the installed plugin was reloaded
  through the supported Codex plugin installation flow.
- A Windows integration test executed the actual PowerShell wrappers through
  all four role messages and terminal completion, including explicit repository
  root handling.

## Presentation and accessibility

- The evidence map exposes five exact cited-input relationships: Researcher and
  Builder into Reviewer, then Researcher, Builder, and Reviewer into Moderator.
  Its legend states that the links represent accepted inputs, not direct agent
  communication or execution order.
- Participant states use human-readable labels while raw protocol values remain
  available in technical data. Provenance is labeled complete but host-reported.
- A partial browser voice pool is reused deterministically. Transcript-only
  fallback remains truthful for unsupported speech and a zero-voice pool.
- The production dashboard browser harness passed 48/48 checks: 16 checks at
  1440px, 768px, and 360px. Audio stayed opt-in and responsive, accessibility,
  reconnect, cancellation, reset, partial-failure, evidence, and speech-state
  checks passed.

## Required verification

```text
python -m unittest discover -s tests -v  # 133 tests passed
python main.py                           # both workers succeeded
python examples/two_threads.py           # A and B succeeded
git diff --check                         # no whitespace errors
```

The deterministic examples required execution outside the restricted command
sandbox so their worker threads could complete; both exited successfully. No
Docker container, external model call, or audible speech was required.
