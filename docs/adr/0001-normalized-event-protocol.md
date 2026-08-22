# ADR 0001: Versioned Normalized Event Protocol

- Status: Accepted
- Date: 2026-08-21

## Context

The v0.1 `Event` record is sufficient for basic diagnostics but lacks run
identity, causal ordering, provenance, redaction state, and interoperable
serialization. UI, conference, speech, tracing, and external adapters need a
truthful boundary that does not expose worker memory or alter orchestration.

## Options considered

1. Extend ad hoc log strings.
2. Make OpenTelemetry the domain model.
3. Define an independent versioned event envelope with optional exporters.

## Decision

Adopt option 3. Use immutable domain events serialized as RFC 8259 JSON with
RFC 3339 UTC timestamps and RFC 9562 UUID identifiers. Model causation,
correlation, run identity, evidence class, provenance, and redaction explicitly.
Native identities use UUIDv4; adapters may use deterministic UUIDv5 mappings,
and compatible externally observed UUIDv7 identities are accepted. Other UUID
versions are rejected by protocol v1.
The native producer records one central acceptance sequence for deterministic
replay; it is not evidence of execution order between concurrent workers.

Use a practical subset of W3C PROV concepts for derivation. Keep OpenTelemetry
as an optional exporter. Defer RFC 8949 CBOR until measurements justify compact
binary encoding. RFC 9457 remains available for a future HTTP control API.

## Consequences

Events become stable and testable across native and external runtimes.
Conference utterances can answer which facts produced them. Producers and
consumers must validate versions, sizes, identifiers, and event-specific
payloads. The existing public runtime API and deterministic result types remain
unchanged while instrumentation is added incrementally.

Redaction occurs before presentation consumers and fails closed. Observer and
exporter delivery is best-effort, bounded, and isolated so it cannot normally
fail orchestration.
