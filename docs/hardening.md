# CTMAO-NSD v0.1 Hardening Status

The v0.1 implementation is an experimental concurrency reference and teaching
runtime. Its core concurrency safety controls are implemented and covered by
the standard-library test suite. It is not yet a model-backed AI orchestration
system.

## Resolved Controls

### Single Correlation Dispatcher

One orchestrator-owned dispatcher is the sole consumer of worker envelopes.
Commands are registered before submission, results are routed by correlation
ID, stale envelopes are ignored, and worker failures resolve all pending work
owned by that worker.

### Explicit Single-Flight Contract

One `Orchestrator.run()` call is allowed at a time. An overlapping call raises
`OrchestratorBusyError` immediately without disturbing the active run. If a
caller cancels a run, the orchestrator rejects reuse until the late worker result
has drained through the dispatcher.

### Absolute Execution Deadline

Each run creates one monotonic deadline before command submission. The same
deadline is carried into every worker delegation tree and result collection uses
that fixed boundary plus a small transport grace period. Unrelated envelopes
cannot extend execution indefinitely.

### Active-Task Shutdown

`Orchestrator.close()` rejects new work, requests cancellation of every worker
root coroutine, joins workers concurrently outside the caller event loop, and
stops the dispatcher after lifecycle envelopes have been processed. An
interrupted run raises `OrchestrationCancelledError`.

### Worker Failure Attribution

Workers retain the active command correlation at the fatal boundary. Failed and
stopped workers reject later commands with `WorkerUnavailableError`; lifecycle
events come from actual worker envelopes rather than optimistic shutdown claims.

## Supported Usage Contract

- Use one public `run()` call at a time per orchestrator instance.
- Treat an infrastructure-level worker failure as terminal for that worker.
- Expect `close()` to cancel active asynchronous task trees.
- Keep task implementations cancellation-cooperative and avoid blocking the
  worker event loop with unbounded synchronous work.
- Install the package or place `src` on the import path before running examples.

## Remaining Alpha Limitations

- Concurrent public runs are rejected rather than queued or multiplexed.
- Worker restart and assignment replay are not implemented.
- Transport and synchronized memory are in-process and non-durable.
- Cancellation cannot forcibly interrupt a blocking native or synchronous call.
- The reference task executor is declarative; product adapters and arbitrary
  handler registration are future extension points.
- Agents are deterministic Python executors; LLM calls, autonomous planning,
  and model-provider adapters are not implemented.
- Complete task trees are caller-declared rather than dynamically planned by a
  supervisor.
- `SyncToken` protects memory synchronization and does not enforce LLM token
  budgets.

These are explicit scope limits, not untracked correctness failures.
