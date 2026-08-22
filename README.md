# Cross-Thread Multi-Agent Orchestration with Nested Sub-Agent Delegation

**Cross-Thread Multi-Agent Orchestration with Nested Sub-Agent Delegation
(CTMAO-NSD)** is this repository's name for an experimental Python concurrency
pattern and reference runtime. It combines isolated, thread-owned executors
with bounded hierarchical task execution and orchestrator-mediated state
exchange.

> **Scope of v0.3.0:** the default runtime remains deterministic and task trees
> remain caller-declared. This release adds an interactive local conference,
> explicit public agent messages, and an optional OpenAI Responses API provider.
> Models cannot plan delegation, execute tools, or bypass runtime limits.

This standard-library-only implementation is intended for concurrency research,
experiments, and teaching. Its deterministic runtime demonstrates a control
plane and safety boundaries; the optional model-runtime contract can call an
application-supplied provider without granting it orchestration authority. No
provider implementation or credentials are bundled. It does not depend on LangGraph, CrewAI,
AutoGen, Swarm, Semantic Kernel, or another orchestration SDK.

## Relationship to the AI Product Framework

This repository is the standalone execution-engine reference. The
[AI Product Framework](https://github.com/bundleguard/ai-product-framework)
governs discovery, architecture selection, approval gates,
implementation, verification, deployment, and retrospectives. It may select
CTMAO-NSD when a product genuinely needs isolated concurrent agent runtimes, but
the two repositories remain independent:

- the AI Product Framework has no runtime dependency on this Python package;
- CTMAO-NSD does not embed product-specific workflows or framework policy; and
- consuming products integrate through adapters while preserving the engine's
  thread-ownership, delegation, and synchronization invariants.

> CTMAO-NSD is not a distributed consensus protocol, process sandbox, durable
> workflow engine, or claim of secure memory isolation. Python threads share a
> process; isolation here means enforced ownership and message boundaries.

## Architectural pattern

CTMAO-NSD is a concurrency architecture in which multiple isolated worker
threads each own a supervisor executor, an asynchronous runtime, and
thread-local state; supervisors route caller-declared work through bounded
parent-child execution trees; and all permitted state exchange between threads
is mediated by a global orchestrator through immutable messages and controlled
synchronization.

The pattern has two inseparable mechanisms:

- **Cross-Thread Multi-Agent Orchestration** governs worker lifecycle, routing,
  scheduling, correlation, failure boundaries, synchronization-capability
  passing, and selected state transfer between isolated runtimes.
- **Nested Sub-Agent Delegation** governs execution of caller-declared
  hierarchical task trees, supervisor-child routing, recursive execution,
  ancestry checks, depth and fan-out limits, timeout propagation, and
  deterministic result aggregation.

They are complementary responsibilities within one architecture—not competing
architectures.

### Architectural invariants

1. A worker thread owns its event loop, supervisor, child agents, and mutable
   local memory.
2. Workers never hold references to one another and never mutate another
   worker's state.
3. Cross-thread commands and results use immutable dataclass envelopes.
4. Selected memory crosses a worker boundary only as an allowlisted snapshot
   authorized by a single-use synchronization capability.
5. The orchestrator is the sole owner of synchronized memory.
6. The root supervisor is delegation depth `0`; a child at the configured
   maximum depth is allowed, but another child below it is rejected.
7. Depth and direct-child limits are validated before a child branch is
   scheduled.
8. One absolute deadline is inherited by the entire delegation tree.
9. Results and failures propagate upward and retain worker, task, and agent-path
   identity.

## Execution model

```mermaid
flowchart TB
    Caller["Caller"] --> O["Global Orchestrator"]

    subgraph WA["Worker Thread A"]
        SA["Supervisor A"] --> A1["Child A1"] --> A2["Child A2"]
        SA --- LA[("Thread-local memory A")]
    end

    subgraph WB["Worker Thread B"]
        SB["Supervisor B"] --> B1["Child B1"] --> B2["Child B2"]
        SB --- LB[("Thread-local memory B")]
    end

    O -->|"immutable command"| SA
    O -->|"immutable command"| SB
    SA -->|"result + snapshot"| O
    SB -->|"result + snapshot"| O
    O <--> M[("Orchestrator-owned synchronized memory")]
```

The arrows are logical message paths. They do not represent direct mutation of
another worker's memory.

```mermaid
sequenceDiagram
    participant C as Caller
    participant O as Orchestrator
    participant SA as Supervisor A
    participant A1 as Child A1
    participant SB as Supervisor B
    participant B1 as Child B1

    C->>O: Submit two root tasks
    par Worker A
        O->>SA: Route immutable task tree
        SA->>A1: Delegate at depth 1
        A1-->>SA: Nested result
        SA-->>O: Aggregate + selected snapshot
    and Worker B
        O->>SB: Route immutable task tree
        SB->>B1: Delegate at depth 1
        B1-->>SB: Nested result
        SB-->>O: Aggregate + selected snapshot
    end
    O->>O: Validate capability and revision
    O-->>C: Combined report
```

The orchestrator creates the main event-loop inbox. Each non-daemon worker
thread creates and closes its own `asyncio` event loop and all loop-bound
resources inside that thread. Transfer between loops uses
`call_soon_threadsafe`; no event loop consumes an `asyncio` primitive owned by a
different loop.

## Core concepts

### Global orchestrator

`Orchestrator` accepts root assignments, issues correlation IDs and
synchronization capabilities, routes commands, collects results, publishes
approved snapshots, and coordinates bounded shutdown. It does not execute child
work or expose worker-local mutable state.

### Worker runtime and supervisor agent

`WorkerThread` owns one OS thread and one event loop. The supervisor inside that
runtime is the only root-routing authority. It validates the caller-supplied
root fan-out, creates child contexts once, and aggregates results in declared
order. It does not select a model, invent tasks, or choose specialists.

### Child agents and delegation contexts

`ChildAgent` is a deterministic runtime executor, not a model-backed AI agent.
It executes a node and may recursively route its declared children. Every child
inherits immutable lineage, agent path, policy, and the root's absolute
deadline. Reusing an ancestor task ID triggers circular-delegation rejection.

### Thread-local and synchronized memory

`ThreadLocalMemory` records its constructing thread's identity and rejects
foreign-thread access at runtime. Its snapshots contain only allowlisted keys.
`SharedMemoryHub` accepts a snapshot only when:

- its capability belongs to the same worker;
- the capability has not already been consumed; and
- its revision is newer than the currently published revision.

Visibility is explicit and eventual: a private write is not synchronized until
the worker returns a snapshot and the orchestrator validates it. The reference
implementation publishes results; routing selected synchronized context into a
later assignment is an application-level policy.

## Safety and bounded execution

| Policy | Default | Effect |
| --- | ---: | --- |
| `MAX_DELEGATION_DEPTH` | `3` | Rejects a child whose depth would exceed the bound. |
| `MAX_CHILDREN_PER_AGENT` | `4` | Rejects excessive direct fan-out before scheduling. |
| `TASK_TIMEOUT` | `5.0 s` | Supplies one absolute deadline to the full execution tree. |
| `THREAD_SYNC_INTERVAL` | `0.05 s` | Reserved cadence for heartbeat/lifecycle extensions. |
| `MEMORY_SYNC_INTERVAL` | `0.25 s` | Reserved cadence for periodic snapshot extensions. |

Depth and fan-out bounds prevent unbounded recursive tree growth through the
delegation API. Lineage checks reject task-ID cycles. The shared absolute
deadline contains stalled asynchronous work, and thread ownership plus message
passing removes cross-worker lock ordering from the reference path. These rules
do not prevent every possible application loop, retry storm, CPU starvation, or
blocking call; extensions must preserve and supplement the safety model.

## Failure isolation

- A child exception becomes a typed failed result rather than crossing the
  thread boundary as a live exception object.
- A child rejection or timeout propagates upward, causing its branch and root
  aggregate to report failure while retaining partial child results.
- One worker's task failure does not cancel work assigned to another worker.
- A fatal worker-runtime error is reported through a `WORKER_FAILED` envelope.
- Shutdown is cooperative and bounded. `Orchestrator.close()` cancels active
  worker root coroutines, keeps the caller event loop responsive, and joins all
  worker threads before returning.

Automatic worker restart, durable replay, retry policy, and process-level crash
recovery are intentionally future concerns.

## Quick start

Requires Python 3.11 or newer.

```bash
python -m venv .venv
python -m pip install --editable .
python main.py
```

The demonstration starts two isolated workers. Supervisor A delegates `Child
A1` to `Child A2`, while Supervisor B independently delegates `Child B1` to
`Child B2`. Both result trees return to the orchestrator, which publishes only
their selected memory snapshots.

Run the test suite with no test-framework dependency:

```bash
python -m unittest discover -s tests -v
```

## Live Agent Conference

The additive observability layer turns genuine runtime facts into a live local
dashboard and spoken conference. It does not expose private reasoning or invent
dialogue. Every transcript turn retains its source event ID, and redaction runs
before browser or speech presentation.

```bash
python examples/live_conference.py
```

Open the printed loopback URL; the normal demo waits for its first listener
before beginning. It exchanges controlled, explicitly supplied findings through
a nested delegation, contains one intentionally failed branch, and shows the
resulting delegation tree and source-backed transcript. Conference/Briefing
mode, global and per-agent mute, per-agent voice overrides, and locally installed
browser voices are available. Use `--hold 0` for an automated smoke run. Audio
is optional: transcript-only mode and the `NullSpeechProvider` work without a
speech service.

Normalized events are available in `OrchestrationReport.normalized_events` and
through an optional failure-isolated `EventBus`. The legacy `events` report
field remains available for v0.1 compatibility.

## Interactive conference

Start the offline interactive application:

```bash
python examples/interactive_conference.py
```

Open the printed tokenized loopback URL, enter an objective, and select **Start
conference**. Researcher and Builder contribute concurrently; Reviewer receives
their actual public outputs; Moderator receives all three messages and produces
the final decision. The deterministic demo is clearly labeled and performs no
model or network call.

For a real model-backed conference, explicitly select the OpenAI provider:

```powershell
$env:OPENAI_API_KEY = "your API key"
python examples/interactive_conference.py --provider openai --model gpt-5.4
```

This sends the objective and public participant messages to the
[OpenAI Responses API](https://developers.openai.com/api/reference/resources/responses/methods/create).
The key remains in the host process and is not copied into events. Requests use
`store: false`; provider and account retention policy still applies. API use may
incur charges. Never paste an API key into the conference task box.

## Agent runtime boundary

`DeterministicRuntime` remains the default. `ModelBackedRuntime` defines an
optional provider-neutral node-execution boundary; it does not grant a model
authority to create tasks, change delegation, or bypass deadlines. No vendor
provider or credentials are bundled. Applications can supply a thread-owned
runtime factory and contract-test it with their own adapter.

## Codex external adapter prototype

The standard-library-only `CodexExecJsonlAdapter` is a prototype for record
shapes currently observed from `codex exec --json`. It observes explicit
thread, turn, message, and tool lifecycle records from a compatible host-fed
stream; it never launches Codex, reads credentials, or consumes reasoning
items. Agent-message text is disabled by default and must be explicitly enabled.

```bash
codex exec --json "summarize the repository" \
  | python examples/codex_jsonl_adapter.py
```

For an opted-in transcript, add `--include-message-text`; redaction still runs
before normalized output. Keep Codex stderr separate from stdout. The adapter
does not infer nested-agent identity or delegation when the stream does not
expose it. `CodexThreadSnapshotAdapter` additionally accepts a schema-v1 task
snapshot supplied by an authorized host integration; it does not treat
`codex://` deep links as an API.

## Repository layout

```text
src/ctmao_nsd/  Runtime, normalized events, observability, conference, speech,
                local UI, model-runtime, and adapter modules
tests/          Standard-library runtime, concurrency, privacy, UI, speech,
                conference, and adapter tests
examples/       Deterministic runtime, live conference, and Codex JSONL demos
docs/           Architecture, protocols, operating guidance, and ADRs
main.py         Original deterministic two-worker demonstration
```

## Benefits

- **Bounded delegation:** depth and fan-out checks constrain execution-tree
  growth before unsafe children are scheduled.
- **Clear ownership:** every mutable local store has one owning runtime.
- **Controlled synchronization:** state movement is explicit, revisioned,
  capability-authorized, and testable.
- **Failure containment:** outcomes preserve child, supervisor, and worker
  boundaries without turning every task failure into global cancellation.
- **Traceable decomposition:** task IDs, correlation IDs, worker IDs, agent
  paths, events, and revisions support diagnosis.
- **Framework independence:** execution has no third-party runtime dependency.

## Trade-offs and limitations

- Python threads do not provide process or security isolation.
- v0.3.0 performs no model call by default. The optional OpenAI provider requires
  explicit launch selection and a host-supplied credential. Autonomous planning,
  provider-managed tools, and model-token budget enforcement are not implemented.
- Task trees are caller-declared; supervisors do not dynamically create or
  select work.
- `SyncToken` is a single-use memory-synchronization capability. It is unrelated
  to LLM input/output token accounting.
- CPU-bound Python work remains constrained by the CPython GIL; processes or
  native extensions may be more appropriate.
- Central orchestration and snapshot validation add latency and can become a
  throughput bottleneck.
- In-memory transport is not durable after process termination.
- Cancellation across threads is cooperative, not forced.
- A supervisor processes its declared root branches deterministically; an
  application can add bounded sibling concurrency while retaining the same
  ownership and deadline rules.
- The interval constants are extension points in v0.x, not background polling
  loops required for correctness.
- `Orchestrator.run()` is intentionally single-flight. A second overlapping call
  raises `OrchestratorBusyError`; all worker envelopes still pass through one
  central correlation dispatcher.
- Calling `close()` during a run cancels that run with
  `OrchestrationCancelledError`. Cancellation is cooperative at awaited Python
  boundaries; blocking native or synchronous calls require their own policy.

See [Hardening status](docs/hardening.md) for verified alpha limitations.

## Future extensions

- A public executor protocol for product-defined task handlers
- Optional model-backed executor adapters outside the standard-library core
- Policy-driven dynamic task planning that preserves depth, fan-out, and
  deadline bounds
- Explicit LLM context and token-budget policies, separate from `SyncToken`
- Durable message transport and restartable assignments
- Pluggable task executors and supervisor routing policies
- Heartbeats, explicit health states, drain policies, and worker restart
- Compare-and-swap context import into subsequent assignments
- Process or remote-worker transports using the same immutable envelopes
- Metrics, tracing adapters, persistence, authentication, and governance hooks

## Terminology and scope

The first use of the name is **Cross-Thread Multi-Agent Orchestration with
Nested Sub-Agent Delegation (CTMAO-NSD)**; later references use **CTMAO-NSD**.
This exact name is canonical only within this repository. "Agent" names an
execution role in this version; it does not imply LLM use or autonomous
decision-making. The implementation is an experimental reference, not a claim
of legal exclusivity, novelty, external recognition, or industry
standardization.

Build-time Codex sub-agents used to help review this repository are unrelated to
the runtime agents demonstrated by CTMAO-NSD.

## License

MIT. See [LICENSE](LICENSE).
