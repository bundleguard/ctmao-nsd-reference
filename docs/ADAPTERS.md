# External Runtime Adapters

## Contract

An adapter translates facts exposed by an external runtime into the normalized
event protocol. It does not grant CTMAO-NSD access to hidden runtime state and
must not fill missing fields with plausible stories.

Every adapter declares:

- runtime and adapter name/version;
- supported protocol versions and event types;
- observable interfaces used;
- identity and causality guarantees;
- evidence classification rules;
- timestamp and sequence behavior;
- redaction responsibility and trust boundary;
- reconnect, duplication, rate-limit, and failure behavior.

Inbound records are untrusted. Validate schema, sizes, enumerations, UUIDs,
timestamps, and version before projection. Apply rate limits and bounded queues;
reject or quarantine malformed records. Adapter failure is isolated from native
execution and other adapters.

## Codex capability classification

Exact availability depends on the supported Codex interface and version. The
adapter must probe or document capabilities instead of assuming them.

## Implemented Codex prototypes

`CodexExecJsonlAdapter` is a lower-fidelity prototype for record shapes
currently observed from `codex exec --json`. A host runs a compatible Codex CLI,
keeps stderr separate, and feeds complete stdout JSONL records into the adapter.
The parser recognizes explicit thread/turn lifecycle,
completed agent messages, command execution, file changes, MCP tool calls, and
web-search items. It ignores reasoning records and never reads tool arguments,
command text, diffs, usage, credentials, or Codex storage. Message text is
private by default and opt-in.

`CodexThreadSnapshotAdapter` converts schema-v1 task snapshots supplied by an
authorized host. This is useful inside a host that already has a supported task
read capability, but the adapter owns no such connection. A `codex://` link is
only a user-facing locator and is not treated as a public API endpoint.

Codex app-server is the richer future route because its documented item model
includes `collabToolCall` records with sender/receiver/new-thread identifiers.
As of this decision, the CLI classifies app-server as experimental and intended
for local development/debugging. It is therefore not launched by the core
package. A future optional integration should use default JSONL stdio, pin and
probe a compatible CLI version/schema, tolerate unknown notifications, and
fail closed. WebSocket transport is unnecessary for the local prototype.

Neither prototype claims access to existing desktop tasks, private reasoning,
hidden instructions, or unreported sub-agent activity. The exec records
currently handled by the prototype do not guarantee the parentage needed for a
high-fidelity nested-agent
conference, so missing delegation remains unavailable rather than inferred.

## Codex trust boundary

- The host owns process launch, authentication, permissions, and stderr.
- The adapter accepts complete bounded JSON records and rejects duplicate keys,
  non-finite values, invalid UTF-8, missing thread/turn context, overlapping
  turns, post-terminal items, and oversized data. It accepts a completed item
  without a preceding item-start record because currently observed exec output
  may surface an authoritative completed item directly.
- Reasoning records are unconditionally suppressed.
- Command text, tool arguments/results, diffs, file paths, and usage are not
  copied into normalized events.
- Source IDs become deterministic UUIDv5 identities scoped to an adapter
  session; normalized acceptance sequence never comes from a source timestamp.
- Item and terminal replays with stable source identities are deduplicated and
  conflicting reuse is counted within one adapter instance. The currently
  handled exec turn-start record does not expose a turn ID, so reconnect-level
  turn-start deduplication requires a host-owned cursor and is not claimed by
  this prototype.
- No adapter failure can alter native orchestration.
- Parser instances are scoped to a bounded host feed. The host applies rate
  limiting and backpressure and calls `reset()` or discards the adapter at its
  retention boundary; these pure parsers do not own a transport queue.

### Observed

Only facts directly exposed by a supported interface qualify, potentially:

- task/thread IDs, explicit statuses, and timestamps;
- user, assistant, or agent messages actually returned by the interface;
- surfaced tool-call names, arguments, results, and process exit status;
- stdout/stderr made available to the adapter;
- explicit delegation, child IDs, parent IDs, and correlation IDs;
- workspace artifacts or diffs read through authorized interfaces.

### Derived

Safe computations from observed facts include elapsed duration from timestamps,
terminal state from explicit lifecycle records, deterministic summaries, and
file-change summaries. Causality or parentage is derived only when exposed IDs
support it. Record the derivation rule/version and every source record ID.

### Inferred

The following are merely probable unless explicitly exposed: role from a name
or prompt; working, waiting, or blocked state from silence; authorship of edits
in a shared workspace; delegation from chronology or prose; intended recipient;
decision rationale. Label them as inferred and exclude them from default speech.

### Unavailable

Never request, reconstruct, or claim access to private chain-of-thought, hidden
reasoning or scratch state, hidden instructions, model internals, unreported
tool activity, or exact sub-agent identity/parentage/token usage when the
supported interface does not expose it.

## Evidence and presentation

Adapter events carry evidence class, source interface and version, an opaque
source reference or digest, observation time, and transformation provenance.
Conference mode accepts observed and safely derived execution claims by
default. Diagnostic UI may show clearly labeled inference; unavailable data is
reported as a capability limitation, not an event-shaped guess.

## Portability

CTMAO-NSD native events and external adapter events share the normalized
protocol but retain distinct `source` values. Provider-specific fields stay in
namespaced payloads or adapter metadata. No Codex-specific type belongs in the
orchestration core.
