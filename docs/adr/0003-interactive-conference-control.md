# ADR 0003: Interactive conference control and public messages

## Status

Accepted for v0.3.

## Decision

Add a loopback-only authenticated control plane above the existing orchestrator.
The control layer may start or cancel one bounded conference, but it never reads
the worker result queue or mutates worker-local state.

Represent human-facing contributions as explicit `message.sent` normalized
events. Publication requires both runtime-policy and per-task opt-in. Only
successful runtime output may supply message text. Record logical speaker,
recipient, message kind, source event, and transformation provenance.

Run the reference conference in three stages: Researcher and Builder, Reviewer,
then Moderator. Pass earlier public outputs into later stages as data. Keep
lifecycle events in a separate technical presentation.

## Consequences

- The primary UI resembles a conference instead of an event console.
- Model output remains bounded by caller-declared task topology.
- The deterministic demo proves transport and causality but is not represented
  as model intelligence.
- The optional OpenAI adapter transmits selected content off-device and requires
  explicit launch configuration and security documentation.
