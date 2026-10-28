# CTMAO-NSD v0.4 Hardening Status

The v0.4 implementation is an experimental concurrency and conference
reference. Its core concurrency safety controls and interactive conference path
are covered by the standard-library test suite. The deterministic runtime is
the default; an application may supply a model provider through the optional
runtime contract, but no provider or credentials are bundled.

## Resolved Controls

### Plugin/Bridge Compatibility Handshake

The repository-contained Agent Conference plugin and host-fed bridge declare
protocol `1.1` at launch and on every event command. The private session file
retains the accepted bridge version, and mismatches fail before participant or
message facts are submitted. This prevents a stale plugin from silently
omitting the causal receipts required by Reviewer and Moderator.

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

### Fail-Closed Conference Provenance

Public conference messages require a same-participant accepted task-lifecycle
cause and role-correct prior message inputs. Native causal references are
validated by the central event authority. The dashboard resolves a bounded full
evidence graph and suppresses any utterance whose graph is incomplete or
inconsistent; bridge and presentation registries are explicitly bounded.

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
- The default task executor is deterministic. `ModelBackedRuntime` provides a
  bounded provider-neutral node-execution hook, not autonomous planning or
  provider-controlled delegation.
- The optional OpenAI provider is a bounded text-completion adapter, not a tool
  runtime. It requires explicit host configuration and transmits selected
  conference content to the provider without making the local server public.
- Codex adapters are host-fed prototypes for currently observed records. They
  do not launch Codex, connect to desktop task storage, or guarantee a
  version-stable external schema.
- The Codex conference bridge is also host-fed. It presents only explicit
  public results supplied by the invoking Codex skill; it cannot transparently
  attach to an arbitrary pre-existing desktop task. Its source-reference digest
  proves stable association inside the host workflow, not independent
  attestation of a source record's contents.
- The event bus, dashboard replay, transcripts, and adapter replay state are
  in-memory and non-durable.
- Complete task trees are caller-declared rather than dynamically planned by a
  supervisor.
- `SyncToken` protects memory synchronization and does not enforce LLM token
  budgets.

These are explicit scope limits, not untracked correctness failures.
