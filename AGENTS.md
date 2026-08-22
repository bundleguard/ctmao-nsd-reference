# Codex Operating Guide

This repository is the reference implementation of **Cross-Thread Multi-Agent
Orchestration with Nested Sub-Agent Delegation (CTMAO-NSD)**. Read `README.md`,
`docs/architecture.md`, and `docs/hardening.md` before changing runtime code.
The architecture documents under `docs/` are authoritative for new event and
conference work.

## Terminology and scope

- A **worker** is a thread-owned execution runtime.
- A **supervisor** and **child agent** are deterministic Python executors in
  v0.1.x. Do not call them model-backed or autonomous agents.
- A **logical agent** is the stable conference identity presented to a human;
  it is not an OS thread identity.
- A **normalized event** is an immutable execution fact. A **conference
  utterance** is a presentation derived from one or more normalized events.
- A **Codex sub-agent** used while developing this repository is not a
  CTMAO-NSD runtime child agent.

## Inspect before refactoring

Before a material change, inspect Git status, relevant source, tests, examples,
and architecture documentation. Preserve unrelated user changes. Prefer small,
additive changes over repository replacement or broad rewrites. Do not silently
change public APIs or execution semantics.

## Architectural invariants

Preserve all of the following:

1. Each worker owns its event loop, supervisor, child agents, and mutable local
   memory.
2. Workers do not hold references to or mutate other workers.
3. Cross-thread values are immutable envelopes.
4. Selected memory crosses a worker boundary only as an allowlisted,
   revisioned snapshot authorized by a single-use capability.
5. The orchestrator alone owns synchronized memory and consumes worker result
   envelopes through one correlation dispatcher.
6. Delegation depth, direct fan-out, lineage, and one absolute tree deadline are
   enforced before unsafe work is scheduled.
7. Expected task failures are typed results. Worker failure, cancellation, and
   shutdown retain correlation and identity.
8. Observability is downstream of execution. UI, transcript, speech,
   persistence, or adapter failure must not normally fail orchestration.

Do not add another consumer of the worker result-envelope queue. Publish
normalized events from explicit instrumentation points through a separate,
best-effort boundary.

## Dependencies and compatibility

The runtime core is standard-library-only and supports Python 3.11 and newer.
Before adding a dependency, document the unmet need, security and licensing
impact, maintenance cost, and why the standard library is insufficient. Keep
optional UI, model, speech, and exporter integrations outside the core.

Preserve the deterministic runtime, public imports, examples, and existing test
contract. Breaking changes require a migration note, changelog entry, updated
tests, and explicit rationale.

## Sub-agent work

Use development sub-agents only for genuinely independent, bounded work. Give
each an objective, relevant files, constraints, expected output, and validation
requirement. Avoid concurrent edits to the same critical file. The supervising
agent reviews and tests all delegated work; a completion claim is not evidence.

## Event, provenance, and truthfulness rules

Follow `docs/EVENT_MODEL.md` and ADR 0001.

- Events describe observable facts, not hidden cognition.
- Never expose or reconstruct private chain-of-thought.
- Do not invent agent dialogue, intent, blockers, decisions, or relationships.
- Every utterance must reference genuine source event IDs. Derived summaries
  must preserve meaning and record their transformation.
- Preserve partial ordering through causation, correlation, and parent
  relationships. The native source records one central acceptance sequence for
  deterministic replay; it is not evidence of execution order between
  concurrent workers.
- Use versioned RFC 8259 JSON, RFC 3339 UTC timestamps, and RFC 9562 UUIDs at
  interchange boundaries.
- Validate event type, version, field sizes, and payload shape at adapter
  boundaries. Unknown compatible fields may be retained; incompatible major
  versions are rejected or quarantined.

## Security and privacy

Assume task data, tool output, events, logs, transcripts, and speech may contain
secrets or personal information. Redaction is a mandatory, fail-closed boundary
before projection, persistence, presentation, or TTS. If redaction fails,
suppress the content and emit only a sanitized diagnostic. Never vocalize API
keys, tokens, passwords, private environment values, or sensitive personal
data.

Default to local-only operation, minimal retention, loopback binding, no-store
HTTP responses, strict origin checks, and per-session authorization. Do not
claim ISO, OWASP, W3C, or other certification/compliance; these sources provide
design guidance only.

## Conference and voice rules

Follow `docs/CONFERENCE_PROTOCOL.md`.

- Identity and voice follow the logical agent, not its thread.
- Voice assignment is deterministic, stable, overrideable, and free of role or
  gender stereotypes.
- Transcript speaker identity must match spoken identity.
- Mute, audio-off, transcript-only fallback, voice-pool exhaustion, stale
  speech cancellation, and provider failure must degrade safely.
- SSML is an optional provider capability, never part of core event semantics.

## Concurrency and failure isolation

Observer delivery must be non-blocking and bounded. Define overflow behavior:
coalesce or drop low-priority progress with a sanitized diagnostic; never block
the orchestration path. Isolate subscriber, adapter, persistence, UI, and TTS
exceptions. Tag queued presentation work by run and lifecycle state so terminal
or cancellation events can invalidate stale output.

Use deterministic synchronization primitives in tests instead of arbitrary
sleeps where practical. Test duplicates, out-of-order delivery, saturation,
slow and failing subscribers, cancellation, shutdown, and late events.

## Required verification

For runtime changes, run:

```text
python -m unittest discover -s tests -v
python main.py
python examples/two_threads.py
```

Also run focused schema, serialization, causality, provenance, redaction,
subscriber-failure, transport, conference, and adapter tests for affected
features. Inspect the final diff and update documentation when contracts change.
