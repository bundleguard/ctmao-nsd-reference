# Conference Protocol

## Product contract

The conference converts presentation-safe normalized events into a concise,
human-consumable account of genuine multi-agent execution. It does not create
teamwork for entertainment and does not expose or reconstruct private
chain-of-thought.

Every utterance contains:

- an `utterance_id`;
- `run_id` and logical `agent_id`/speaker identity;
- presentation text;
- source event IDs;
- transformation name and version;
- importance, lifecycle validity, and queue time;
- redaction status;
- optional voice profile and provider-neutral speech hints.

An utterance without source events is invalid except for an explicitly marked
local UI/system notice that makes no execution claim.

## Deterministic transformation first

Common lifecycle facts use deterministic, testable templates. Optional
model-assisted summarization may handle complex batches, but it must be
disableable, preserve source meaning, expose cost/usage when available, and
fall back to deterministic output. Inferred adapter state is silent by default.

Prioritize assignments, delegation, findings, explicit questions and answers,
blockers, failures, recovery, handoffs, decisions, and completion. Suppress or
batch routine low-level activity. Conference and briefing modes consume the
same normalized stream; briefing mode summarizes current observable state.

Native task outputs remain private by default. A deployment or controlled demo
may enable `RuntimeConfig.publish_result_summaries` only when its runtime output
is explicitly suitable for presentation; those source-backed outputs become
`finding.reported` and delegation-result events and still pass through central
redaction before transcript or speech.

## Interactive public messages

The primary v0.3 conversation contains only `message.sent` events. A successful
runtime output becomes such an event only when both the runtime configuration
and the individual task explicitly authorize publication. Attributes record the
stable public speaker, recipient, and message kind. Later conference rounds
receive earlier public output strings as actual inputs; the system does not
invent replies from lifecycle events.

Those inputs retain their accepted `message.sent` event IDs as bounded
`source_event_ids` provenance. Researcher and Builder are independent inputs to
Reviewer; Reviewer is therefore a fork/join rather than a fabricated linear
handoff. Each public message is directly caused by its own accepted
`task.started` fact and remains grouped by the worker-command correlation ID.
Moderator cites all supplied public inputs while its direct operational cause
remains its own task lifecycle.

The four-role protocol has a fixed evidence policy. Researcher and Builder cite
no upstream messages. Reviewer cites exactly every available successful
Researcher and Builder message, with at least one required. Moderator cites
exactly those available initial messages and the Reviewer message when one was
published. A host-fed message is rejected unless its direct cause is that same
participant's accepted `task.started` event.
Each participant has one forward-only lifecycle and may publish at most one
public message per conference; contributed, failed, and finished identities
cannot be reopened or assigned a replacement `task.started` event.

Before a public message reaches transcript or speech, the dashboard resolves
its complete direct-cause and upstream graph from the active conference event
registry. A missing, cross-session, non-prior, mismatched, cyclic, or oversized
graph suppresses the utterance. The provenance disclosure lists each resolved
event. Resolution is bounded to 64 events per utterance; active dashboard
evidence is bounded to 512 events, and the Codex bridge admits at most 256
events while reserving its final slot for a terminal fact.
Presentation-bound structured fields such as recipient identity pass through
the same fail-closed redaction boundary as utterance text.

Start, completion, delegation, memory, and worker events remain available in the
technical log. They are not rendered as conversational speech in the primary
view. The legacy v0.2 projection remains available for diagnostics.

## Codex task conference

The v0.4 Codex bridge is populated only by the authorized host workflow that
owns the current Codex task. Its four presentation identities are Researcher,
Builder, Reviewer, and Moderator; those labels describe bounded conference
roles and do not replace the genuine Codex subagent/thread identifiers retained
as digested source references.

Starting a new conference creates a new run identity and clears prior replay,
projection, transcript, speaking, and task-tree state. Participant state is
run-scoped. One failed participant produces a sanitized warning and may degrade
to the remaining evidence; it must not produce invented dialogue or stale
output. Completion, failure, and cancellation clear the current speaker and
invalidate queued speech.

In Codex mode the browser task composer is hidden. The originating Codex task is
the steering and final-answer surface; the dashboard is a live public record.
The fixed Researcher/Builder-to-Reviewer-to-Moderator diagram describes the
protocol, not worker-to-worker communication or global execution order. A
connection is presented as accepted only when the downstream public message
cites the exact accepted upstream `message.sent` event IDs.

## Speaker and voice identity

Identity follows the logical agent, not the worker thread. The current native
runtime uses the immutable full `agent_path` as its within-session logical
speaker key; a future cross-run registry may add explicit stable agent UUIDs
without confusing them with thread IDs. The orchestrator receives a reserved
stable voice. Remaining voices
are assigned deterministically, retained for the run, and user-overridable.
Do not map technical roles to gender, accent, or other stereotypes.

When the voice pool is exhausted, reuse voices predictably and preserve speaker
labels; modest provider-supported prosody differences may help, but transcript
identity remains authoritative. Individual mute, global audio-off, and
transcript-only operation are required. Speech-provider failure cannot fail the
run.

SSML is optional and generated only inside a provider adapter. Core utterances
store plain safe text and provider-neutral hints. Unsupported SSML is removed,
not spoken as markup. Voice selection is application policy, not an SSML rule.

## Queue and lifecycle

The speech queue is bounded and importance-aware. Consumers invalidate queued
progress when terminal or cancellation facts make it stale; the bundled browser
client does this automatically. Duplicate source event
sets do not produce duplicate speech. A timeout or provider exception marks the
utterance unspoken while leaving it visible in the transcript.

The text handed to TTS must be the same redacted substantive text shown in the
transcript. Provider-specific pronunciation changes may not introduce new
claims. Audio artifacts link back to the utterance and its source event IDs.

## Acceptance tests

Verify distinct voices when available, stable assignment, orchestrator voice,
parent/child identification, overrides, mute, audio-off, pool exhaustion,
provider failure, stale cancellation, deduplication, transcript-only fallback,
speaker consistency, redaction before TTS, and source-event provenance for
every execution claim.
