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
