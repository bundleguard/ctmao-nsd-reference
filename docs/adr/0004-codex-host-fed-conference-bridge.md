# ADR 0004: Use a host-fed bridge for Codex conferences

- Status: Accepted
- Date: 2026-08-22

## Context

The conference should follow genuine Codex subagents working on the user's
current Codex task. Launching another model conference from the browser creates
a second, disconnected task. Reading desktop task storage or inferring agents
from transcripts would also violate provenance and privacy requirements.

Codex App Server is the documented foundation for rich clients with streamed
agent events, approvals, authentication, and conversation history, but adding a
version-pinned App Server client is a larger optional integration and is not
required for the standard-library runtime.

## Decision

Ship a repository-contained Codex skill and a loopback-only host-fed bridge.
The invoking Codex task performs genuine bounded delegation and reports only
explicit public participant states and final messages. The bridge validates and
redacts those facts, digests source references, converts them to normalized
events, and drives the existing conference presentation path.

The bridge does not launch Codex, read Codex storage, access private reasoning,
or claim automatic attachment to arbitrary desktop tasks. The originating Codex
task remains authoritative for steering, approvals, implementation, and the
final result.

The plugin and bridge exchange an explicit compatibility version at launch and
on subsequent host commands. A missing or incompatible version is rejected
before the bridge accepts participant facts. This handshake covers the host-fed
command contract; it does not change normalized-event schema versioning.

## Consequences

- The observed conference matches the task the user actually assigned in
  Codex when the skill is invoked there.
- UI and speech remain downstream and failure-isolated.
- Provenance is truthful but host-relative; a digest is not independent source
  attestation.
- A future optional App Server adapter can replace the host commands without
  changing normalized event or conference semantics.
