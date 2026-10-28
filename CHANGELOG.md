# Changelog

All notable changes to this project are documented in this file.

## [0.4.2] - 2026-10-25

### Changed

- The evidence map now displays all five required cited-input relationships:
  Researcher and Builder into Reviewer, then Researcher, Builder, and Reviewer
  into Moderator. The legend explicitly distinguishes evidence inputs from
  communication and execution order.
- Participant state and provenance labels use first-time-user language while
  retaining raw protocol values in technical details.
- Browser speech reuses a partial voice pool deterministically; transcript-only
  fallback is reserved for unavailable speech or a zero-voice pool.
- Agent Conference PowerShell wrappers restore the caller's `PYTHONPATH`, and
  event commands accept the same explicit repository root as bridge startup.

### Added

- Windows-only end-to-end coverage that executes the actual PowerShell wrappers
  through four causal message receipts and terminal completion.
- Installation, update, version, repository-root, and troubleshooting guidance
  for the personal Agent Conference development distribution.

### Compatibility

- Bridge protocol remains `1.1`; runtime, queue, worker, and normalized-event
  semantics are unchanged. Plugin and package versions advance to `0.4.2`.

## [0.4.1] - 2026-10-25

### Added

- Explicit Agent Conference plugin/bridge protocol `1.1` handshake in launch,
  session, state, and event-command paths. Missing or incompatible versions
  fail before accepting host-fed conference facts.
- Evidence-first dashboard with a visible Researcher/Builder fork, Reviewer
  join, Moderator decision path, human-readable evidence summaries, redaction
  status, and mode-aware Codex steering guidance.
- Plugin package, bridge compatibility, responsive layout, accessibility, and
  audio-state tests, including a dependency-free browser regression harness at
  desktop, tablet, and 360px widths and a four-role HTTP client smoke test.

### Changed

- Agent Conference plugin version advanced to `0.4.1`; causal receipts and
  exact upstream message IDs are required throughout the documented workflow.
- Python resolution now prefers a project environment or Codex's bundled
  Python and skips the noisy Windows Store alias.
- Plugin scripts preserve an existing `PYTHONPATH` when prepending the
  repository `src` directory.
- The public transcript and final decision are now the primary desktop
  content. Voice configuration is secondary and audio starts explicitly off.
- "Now speaking" is split into latest contribution, queued audio, and actual
  speaking state. Host-fed evidence is described as accepted and linked rather
  than independently verified.
- Speech state now follows provider start/end/error callbacks, ignores stale
  callbacks after cancellation or a new run, and survives repeated terminal
  status polling during an explicit replay. Redaction metadata from accepted
  source records remains visible in the evidence drawer.
- Windows aborted SSE sockets now use the normal peer-disconnect cleanup path
  rather than printing a request-handler traceback.

### Compatibility

- Runtime task, queue, worker, normalized-event, and orchestration semantics are
  unchanged. The earlier dashboard remains available with `view=v4`.

## [0.4.0] - 2026-08-22

### Added

- Host-fed `CodexConferenceBridge` with authenticated loopback participant,
  message, and terminal-state routes.
- Repository-contained Agent Conference Codex plugin, skill, and PowerShell
  host scripts for conferencing the current task with genuine Codex subagents.
- Responsive v0.4 dashboard with task attachment, per-run participant state,
  concise safe message rendering, long-message expansion, and backstage
  technical details.

### Changed

- A new conference clears prior replay, projection, transcript, task-tree, and
  speech state instead of accumulating unrelated runs.
- Presentation-bound objectives are redacted while interactive execution keeps
  the original objective inside the trusted runtime boundary.
- Bridge event sequences remain monotonic across conferences in one session,
  and delayed events from an earlier Codex conference are discarded.
- Host command session files now require a canonically parsed loopback HTTP
  origin, rejecting authority-confusion URLs before any request is sent.
- Reviewer failure now degrades to a sanitized warning and allows Moderator
  synthesis from available genuine contributions.
- Terminal conference state clears stale speaker and queued speech state.
- Public conference messages now require a matching accepted `task.started`
  cause and exact role-correct upstream message evidence.
- The dashboard exposes each utterance's complete bounded causal graph and
  suppresses presentation when any source evidence is missing or inconsistent.
- Native causal references are centrally validated as prior same-session
  events, ambiguous duplicate task IDs are rejected, and bridge/dashboard
  provenance registries have deterministic bounds.
- Participant lifecycle is forward-only with one public contribution per role;
  structured recipient metadata is redacted before events or UI payloads, and
  task identity uniqueness is enforced across the complete run submission.

### Compatibility

- The deterministic runtime, optional OpenAI interactive provider, public
  imports, and earlier dashboards remain available. Add `view=v3` or
  `view=legacy` to select an earlier dashboard.

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
