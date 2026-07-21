# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-07-22

### Added

- Canonical CTMAO-NSD architecture definition and standard-library reference
  runtime.
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
