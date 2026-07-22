# Architecture notes

This document supplements the repository-level definition of **Cross-Thread
Multi-Agent Orchestration with Nested Sub-Agent Delegation (CTMAO-NSD)**.

## Runtime terminology

In v0.1.1, `SupervisorAgent` and `ChildAgent` are deterministic Python
executors. They do not call an LLM, select a model, plan autonomously, or invent
new tasks. The caller supplies the complete immutable `TaskSpec` tree before
execution, and the runtime enforces how that tree crosses ownership and
delegation boundaries.

`SyncToken` is a single-use capability authorizing one memory snapshot. It is
not an LLM context-window or input/output token budget.

## Ownership map

| Component | Owner | Mutable? | Crosses threads? |
| --- | --- | --- | --- |
| `Orchestrator` | Main event-loop thread | Yes | No |
| `SharedMemoryHub` | Orchestrator | Yes | No |
| `WorkerCommand` | Immutable value | No | Yes |
| `WorkerEnvelope` | Immutable value | No | Yes |
| `SupervisorAgent` | Worker runtime | Yes | No |
| `ChildAgent` | Worker runtime | Yes | No |
| `ThreadLocalMemory` | Constructing worker | Yes | Snapshot only |
| `TaskSpec` / `TaskResult` | Immutable values | No | Yes |

The model deliberately avoids a globally locked dictionary shared by worker
threads. A lock can make mutation race-safe, but it does not express ownership,
selection, revision policy, or the orchestrator-only synchronization invariant.

## Boundary protocol

1. The orchestrator creates an immutable `WorkerCommand` and a single-use
   `SyncToken` tied to the target worker.
2. `call_soon_threadsafe` schedules the command on the worker-owned event loop.
3. The supervisor receives a caller-declared task tree and builds a root
   `DelegationContext` at depth `0` with one absolute monotonic deadline.
4. Each child derivation appends its task ID and agent name while retaining the
   original deadline.
5. The worker returns an immutable result tree and an allowlisted memory
   snapshot.
6. The orchestrator correlates the envelope, validates its capability and
   revision, and publishes the snapshot.

## Depth semantics

The supervisor is depth `0`. Its direct child is depth `1`. A node whose depth
equals `max_delegation_depth` may execute, but cannot create another child.
Fan-out is a per-parent limit and is validated before any direct child coroutine
is created.

## Failure semantics

Expected task, delegation, and timeout failures are data: they become
`TaskResult` instances and propagate upward. Unexpected runtime failure at the
worker boundary becomes a `WORKER_FAILED` envelope. The current run fails if a
worker runtime itself crashes, but ordinary failed tasks remain isolated and
other worker results are collected.

## Shutdown semantics

`close()` stops accepting work, sends cooperative `STOP` commands, waits for
worker coroutines to return, and joins every non-daemon thread. v0.1.x executes
one root assignment at a time per worker, so a stop command naturally follows
the active assignment in its inbox. Explicit drain and cancellation commands
are planned extensions for multi-assignment worker concurrency.
