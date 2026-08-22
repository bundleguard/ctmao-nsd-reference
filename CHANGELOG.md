# Changelog

All notable changes to this project are documented in this file.

## [0.3.0] - 2026-08-22

### Added

- Authenticated interactive start, cancel, state, and live SSE paths.
- Researcher and Builder contributions, Reviewer critique, and Moderator
  synthesis with genuine public outputs passed into later rounds.
- Double opt-in public agent messages through runtime policy and per-task
  authorization.
- Conference-first UI with task entry, recognizable participants, voices,
  decisions, provenance, and a separate technical log.
- Optional bounded OpenAI Responses API provider plus an offline deterministic
  conference provider.

### Changed

- Lifecycle narration moved behind the technical log; only explicit
  `message.sent` evidence appears in the primary conversation.
- Version metadata and server identity advanced to v0.3.

### Compatibility

- The deterministic runtime, original examples, legacy event fields, and v0.2
  dashboard remain available. Add `view=legacy` to the URL for the old view.

## [0.2.0] - 2026-08-22

### Added

- Versioned RFC 8259 normalized events with RFC 3339 timestamps, RFC 9562 UUID
  identity, strict codecs, provenance, and fail-closed redaction.
- Live nested task and delegation instrumentation routed through the existing
  central dispatcher, with run-scoped reports and non-blocking bounded fan-out.
- Deterministic conference and briefing projection, stable voice assignment,
  transcript-only speech contracts, and a stale-aware bounded speaker queue.
- Loopback-only token-protected SSE dashboard with browser-local voices, mute,
  pause, briefing, timeline, agent state, and transcript views.
- Additive deterministic/model runtime boundary with a provider-neutral model
  adapter contract and fake-provider tests; no credentials or vendor SDK are
  required.
- Persistent operating guide, event/observability/conference/adapter docs, and
  architecture decision records.
- Host-fed Codex prototype adapters for currently observed `codex exec --json`
  records and authorized task snapshots, with in-instance replay detection,
  privacy, provenance, and no-hidden-reasoning boundaries.
- Complete source-distribution contents for the operating documentation,
  architecture decisions, examples, and test suite, with SPDX license metadata.

### Compatibility

- The v0.1.1 deterministic runtime remains the default, its public task/result
  behavior is preserved, and the original tests and examples remain valid.

## [0.1.1] - 2026-07-22

### Changed

- Reframed the project as an experimental Python concurrency reference rather
  than an externally canonical AI framework.
- Defined runtime agents as deterministic Python executors and stated that the
  release contains no LLM calls, autonomous planning, or model-provider layer.
- Clarified that `TaskSpec` trees are caller-declared rather than dynamically
  planned by supervisors.
- Distinguished the memory capability `SyncToken` from LLM context or token
  budgets.
- Scoped canonical terminology to this repository and documented model-backed
  adapters as a future extension.

### Runtime compatibility

- No runtime behavior or public Python API changed from v0.1.0.

## [0.1.0] - 2026-07-22

### Added

- Repository-local CTMAO-NSD architecture definition and standard-library
  reference runtime.
- Two isolated worker threads with thread-owned event loops, supervisors, child
  agents, and local memory.
- Bounded delegation depth and fan-out, lineage checks, shared absolute
  deadlines, and deterministic result aggregation.
- Orchestrator-mediated, allowlisted, revisioned memory synchronization.
- Single central envelope dispatcher and explicit single-flight admission.
- Cooperative active-task cancellation, concurrent non-blocking worker joins,
  and terminal failed/stopped worker behavior.
- Unit, integration, failure-boundary, shutdown, and example coverage.

### Known limitations

- Public `run()` calls are single-flight per orchestrator instance.
- In-process transport and synchronized memory are not durable.
- Worker restart, replay, and arbitrary product handler registration remain
  future extensions.
