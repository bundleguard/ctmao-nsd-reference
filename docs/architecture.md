# Architecture notes

This document supplements the repository-level definition of **Cross-Thread
Multi-Agent Orchestration with Nested Sub-Agent Delegation (CTMAO-NSD)**.

## Runtime terminology

In v0.4.0, `SupervisorAgent` and `ChildAgent` use `DeterministicRuntime` by
default. An application may inject `ModelBackedRuntime`, but the model provider
executes only the current node: it cannot plan, alter the immutable `TaskSpec`
tree, or invent delegation. The runtime enforces how the caller-supplied tree
crosses ownership and delegation boundaries.

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

`close()` stops accepting work, requests cancellation on each worker-owned
event loop, waits for cooperative coroutine cleanup, and joins every non-daemon
thread outside the caller event loop. Explicit drain and per-task cancellation
commands remain future extensions.

## Additive observability and runtime boundaries

The normalized event layer is additive to the v0.1 diagnostic `Event` record.
Worker-local facts travel as immutable `EVENT` envelopes through the existing
single outbound dispatcher. The orchestrator validates correlation ownership,
assigns one central observed sequence, redacts presentation text, and then
publishes to a bounded failure-isolated `EventBus`. No UI, speech provider, or
external adapter consumes worker result envelopes directly.

`AgentRuntime` separates node intelligence from orchestration. The default
`DeterministicRuntime` preserves v0.1.1 behavior. An optional
`ModelBackedRuntime` delegates one node to a provider adapter but cannot create
children, alter deadlines, or bypass delegation policy.

## Interactive conference application

`InteractiveConference` is an application layer above the unchanged
orchestrator. It runs three bounded rounds: Researcher and Builder in parallel,
Reviewer with both genuine public outputs, then Moderator with all public
messages. It never lets a model create `TaskSpec` nodes or address worker queues.

Public conversation is double-authorized:
`RuntimeConfig.publish_agent_messages` and the individual
`TaskSpec.publish_output` must both be true. The worker emits `message.sent` only
after a successful runtime result. Lifecycle facts remain separate.

The token-protected HTTP control plane schedules work through
`InteractiveConference`; it does not consume result envelopes. Token, Host, and
Origin validation protect every state-changing route, and only one conference
is admitted at a time.

## Codex host-fed conference bridge

`CodexConferenceBridge` is a separate application boundary for a conference
that originates in the current Codex task. The Codex skill owns genuine
subagent delegation; the bridge owns only validated public presentation facts.
It neither launches Codex nor reads task storage, credentials, prompts, tool
arguments, or private reasoning.

The trusted host starts one conference and posts bounded participant states,
explicit public messages, and one terminal result over token-authenticated
loopback routes. Every accepted fact becomes an `ADAPTER`-origin normalized
event. Raw source references are replaced with bounded SHA-256 digests before
projection. Redaction is fail-closed before public message publication.

`LiveDashboard` scopes replay and projection to the active conference ID. A new
conference clears prior messages, projected events, task-tree state, and stale
speech. Terminal state invalidates the current speaker. The UI remains
downstream: bridge, presentation, speech, or browser failure cannot fail the
originating Codex task.

The bridge applies the fixed Researcher/Builder fork, Reviewer join, and
Moderator synthesis evidence policy before accepting a public message. The
dashboard retains a bounded active-conference event registry across the native
interactive conference's internal runs and resolves each utterance's complete
causal graph fail-closed before presentation.

Plugin/bridge protocol `1.1` is a host-command compatibility boundary distinct
from normalized-event schema `1.0`. The evidence-first dashboard remains a
downstream projection: its accepted workflow edges are derived only from the
already validated upstream message IDs and never consume worker envelopes or
alter orchestration.
