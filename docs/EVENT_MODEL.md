# Normalized Event Model

## Purpose

Normalized events are the provider-neutral boundary between CTMAO-NSD
execution and downstream observability. They report genuine execution facts
without exposing mutable worker state or private reasoning. The existing
`events.Event` type remains the small cumulative v0.1 diagnostic record.
`normalized_events.NormalizedEvent` is the implemented interchange protocol.

## Envelope

The initial protocol should be an immutable value with these fields:

| Field | Requirement | Meaning |
| --- | --- | --- |
| `schema_version` | required | Semantic protocol version, initially `1.0`. |
| `event_id` | required | RFC 9562 UUID identifying this event. |
| `event_type` | required | Namespaced fact such as `task.started`. |
| `occurred_at` | required | Canonical RFC 3339 UTC timestamp. |
| `session_id` | required | UUID for one orchestrator lifetime. |
| `sequence` | required | Central orchestrator-observed order. |
| `run_id` | optional | UUID for one run; absent for session-only facts. |
| `origin` | required | `worker`, `orchestrator`, `projection`, or `adapter`. |
| `evidence` | required | `observed` or deterministically `derived`. |
| `importance` | required | `debug`, `routine`, `important`, or `critical`. |
| `summary` | required | Bounded presentation-safe factual text. |
| `worker_id` | optional | Worker identity; not logical identity. |
| `task_id` | optional | Task UUID. |
| `parent_task_id` | optional | Direct task parent when observed. |
| `correlation_id` | optional | Groups request and response facts. |
| `causation_event_id` | optional | Event ID that directly caused this event. |
| `agent_path` | optional | Current logical execution path. |
| `status` | optional | Explicit lifecycle outcome. |
| `attributes` | required | Bounded immutable scalar metadata. |
| `provenance` | required | Bounded source/transformation metadata. |
| `redactions` | required | Applied redaction metadata. |

Not every identifier is meaningful for every event. Omitted means unavailable;
it must not be guessed to fill the envelope.

## Initial taxonomy

Implement only facts backed by real instrumentation. A coherent first set is:

- `orchestration.run.started`, `orchestration.run.completed`,
  `orchestration.run.failed`, `orchestration.run.cancelled`;
- `runtime.worker.started`, `runtime.worker.failed`,
  `runtime.worker.stopped`;
- `task.assigned`, `task.started`, `task.completed`, `task.failed`,
  `task.timed_out`, `task.rejected`;
- `delegation.created`, `delegation.completed`, `delegation.failed`;
- opt-in `finding.reported` for explicitly publishable task output;
- double-opt-in `message.sent` for a successful, explicitly public runtime
  output, with public speaker, recipient, and message-kind attributes;
- `memory.snapshot.accepted`.

Add other `message.*`, `tool.*`, progress, blocked, waiting, or resumed events only
when a runtime explicitly exposes those facts.

## Ordering and causality

`occurred_at` is descriptive, not a global ordering authority. `sequence` is
the order in which the sole orchestrator dispatcher accepted facts. It is an
observed delivery order, not a claim that independent workers executed in that
order. `causation_event_id`, correlation IDs, and parent identifiers express
causal relationships when known.
Monotonic clocks remain appropriate for internal deadline and duration
measurement but are not serialized as wall-clock timestamps.

Duplicate delivery is allowed. Consumers deduplicate by `event_id`. A sequence
gap is observable but does not authorize inventing missing events.

## Provenance and evidence

Provenance records:

- `evidence_class`: `observed`, `derived`, `inferred`, or `unavailable`;
- source runtime/adapter and version;
- opaque source record IDs or digests;
- source event IDs for derived records;
- transformation name and version, when applicable.

Native runtime events are normally `observed`. A deterministic conference
summary is `derived`. Inferences are excluded from default conference speech
and must be visibly labeled when diagnostic tooling elects to show them.
`unavailable` is a documented capability state, not an emitted substitute fact.

This is a pragmatic application of W3C PROV concepts: execution records are
entities, transformations are activities, and runtime/adapter/logical-agent
identities are agents. Full PROV-DM serialization is deferred.

## Redaction

Raw execution events remain inside the trusted runtime boundary. Before any
event reaches projection, persistence, HTTP, transcript, or speech, a redaction
stage creates a presentation-safe derivative and records redaction metadata.
Redaction is fail-closed: on redactor error or uncertain secret handling, omit
the sensitive payload and emit a sanitized diagnostic with no raw value.

The implemented redactor covers every presentation-bound string field and
common credential/token patterns. It is not a general personal-information
classifier; deployments with regulated or domain-specific data must add a
fail-closed policy before those records reach this boundary. Size limits apply
before and after redaction.

## Serialization and evolution

Canonical interchange is RFC 8259 JSON encoded as UTF-8. Timestamps are RFC
3339 UTC strings. UUID fields follow RFC 9562. JSON object keys are strings and
non-finite numbers are invalid.

The current strict `1.0` codec rejects missing, extra, duplicate, oversized, or
ill-typed fields. Future compatibility rules must be introduced by a new codec
version rather than silently changing this parser. CBOR (RFC 8949) remains
deferred until measurements justify it.

## OpenTelemetry mapping

The domain model remains independent of OpenTelemetry. An optional exporter may
map a run to a trace, tasks/delegations to spans or links, lifecycle facts to
span events, and IDs/roles/status to attributes. Export failure is an isolated
subscriber failure and cannot alter orchestration. OpenTelemetry is not a
canonical storage format for conference provenance.
