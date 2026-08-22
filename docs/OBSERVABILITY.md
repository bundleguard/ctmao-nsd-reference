# Observability Architecture

## Boundary

The required direction is:

```text
execution -> normalized events -> safe projections -> UI / transcript / speech
```

Observability does not consume worker result envelopes directly, mutate worker
memory, schedule delegation, or decide task success. Native instrumentation
publishes immutable facts to a separate event boundary after preserving the
existing execution semantics.

## Best-effort event delivery

The initial event bus is in process and non-durable. Publishing from execution
is non-blocking. Each subscriber receives a bounded queue and an isolated daemon
delivery thread. A slow or failed subscriber cannot raise into orchestration.

On saturation, queued debug/routine events are preferred eviction candidates.
If the queue contains only important/critical events, the oldest queued event
may still be dropped to preserve the hard capacity bound. Drops and subscriber
exceptions are exposed as sanitized numeric diagnostics; the bus does not emit
additional diagnostic events. Shutdown stops admission and attempts a bounded
join. A callback already running cannot be forcibly cancelled and may finish on
its daemon thread after `close()` returns. These best-effort semantics do not
extend the orchestration deadline.

## Projections

Presentation-safe events may feed independent projections:

- logical-agent registry and state;
- delegation tree;
- current activity and blockers, only when explicit;
- timeline and run summary;
- transcript and utterance provenance;
- bounded replay buffer for local streaming.

Projectors deduplicate by event ID, tolerate compatible unknown events, and
handle out-of-order facts without inventing state. Each projection is scoped by
`run_id`; one run's state must not leak into another report.

## Failure model

The following degrade observability but normally leave execution intact:

- event subscriber exception or queue saturation;
- malformed external-adapter input;
- transcript or persistence failure;
- UI disconnect or replay gap;
- TTS error, timeout, or missing voice;
- optional exporter failure;
- conference transformation error.

Malformed or oversized adapter records are rejected or quarantined. A redaction
failure suppresses content rather than falling back to raw presentation.
Operators receive sanitized diagnostics through a separate health projection.

## Security and retention

Only redacted events enter presentation projections. Default persistence is
off; transcript and history retention must be explicit, bounded, local-first,
and deletable. Logs must not duplicate raw task values or exception text.
Provider credentials remain outside events and transcripts.

The local server binds to loopback by default, uses a per-session secret,
validates browser origins, disables permissive CORS, and marks event/transcript
responses `Cache-Control: no-store`. These are risk controls, not claims of
certification under ISO/IEC 27001, OWASP, or another program.

## Standards mapping

- RFC 3339, RFC 8259, and RFC 9562 define timestamp, JSON, and identifier
  interchange.
- W3C PROV informs event-to-utterance derivation links.
- OpenTelemetry is an optional exporter mapping, not the domain model.
- SSE is the initial browser transport; see ADR 0002.
- RFC 9457 is appropriate if a future HTTP control API needs machine-readable
  errors. No such API is required for the first read-only stream.

## Verification priorities

Test schema validation, concurrent source sequences, duplication, ordering
gaps, causation, redaction including split/chunked secrets, queue saturation,
slow and failing subscribers, cancellation, bounded shutdown, replay, UI
disconnect, persistence errors, and exporter errors. In every observer-failure
test, assert that the underlying orchestration result is unchanged.
