# CTMAO-NSD v0.1 Hardening Status

The v0.1 implementation passes its standard-library unit suite and both
two-thread examples. It is suitable as an alpha reference and teaching
implementation under a single-flight usage contract. It is not yet a hardened
production runtime.

## Supported Usage Contract

- Use one `Orchestrator.run()` call at a time per orchestrator instance.
- Await the active run before calling `close()`.
- Treat worker runtime failure as terminal for that worker instance.
- Install the package or place `src` on the import path before running examples.

## Verified Hardening Blockers

### Central Result Dispatch

Concurrent `run()` calls currently consume the same outbound queue. Each call
can receive and discard an envelope correlated with the other call, causing
both operations to time out. A single orchestrator-owned dispatcher must route
envelopes to per-correlation futures before concurrent callers are supported.

### Active-Task Shutdown

Worker joining is synchronous and currently allows less time than a legitimate
default task deadline. Calling `close()` during active work can raise
`TimeoutError`, block the caller's event loop during joins, and leave a
non-daemon worker alive until the task finishes. A drain/cancel/stop lifecycle
with asynchronous bounded joining is required.

### Worker Failure Attribution

The outer worker crash boundary can emit a fabricated correlation ID rather
than the active command's ID. Failure attribution is therefore only reliable
under the single-in-flight contract. Dead workers also require an explicit
terminal lifecycle state so later submissions fail immediately.

### Orchestration Deadline

The orchestrator currently applies a fresh collection timeout to every envelope
read. A future implementation should calculate one absolute orchestration
deadline so unrelated or stale envelopes cannot extend the run indefinitely.

## Highest-Value Next Engineering Task

Implement a central result dispatcher with an explicit single-flight guard as
the safe intermediate step. The guard should reject overlapping runs clearly;
the dispatcher can then evolve toward supported concurrent calls without
changing the worker message protocol.

After that change, implement the drain/cancel/stop shutdown state machine and
add deterministic regression tests for both verified failure cases.
