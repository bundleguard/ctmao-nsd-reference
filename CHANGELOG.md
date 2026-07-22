# Changelog

All notable changes to this project are documented in this file.

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
