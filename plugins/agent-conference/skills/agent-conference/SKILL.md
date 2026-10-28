---
name: agent-conference
description: Establish a live, source-backed conference for the current Codex task using genuine bounded Codex subagents. Use when the user invokes $agent-conference, asks to conference a task, or explicitly wants multiple agents whose public results are shown and optionally spoken in the local Agent Conference page. Do not use merely because a task is complex when the user did not request a conference.
---

# Agent Conference

Use the current Codex task as the source of truth. The conference page is an
read-only observability surface. Steer and approve work in Codex.

## Required workflow

1. Find the repository root with `git rev-parse --show-toplevel`. Confirm it
   contains `examples/codex_conference_bridge.py` before continuing. This
   plugin requires the CTMAO-NSD repository bridge protocol `1.1`; it is not a
   standalone dashboard package.
2. Start the loopback bridge with this plugin's `scripts/start-conference.ps1`.
   Pass the resolved repository root as `-RepositoryRoot`, the user's current
   task as `-Objective`, and use
   `-TerminalHoldSeconds 120`. Keep the long-running process session alive.
3. Read the first JSON line. Give the user the `conference_url` immediately so
   they can watch while work continues. Retain `session_file` privately for the
   event commands; never vocalize its token or file contents.
4. Use genuine Codex subagents for independent, bounded roles:
   - Researcher: inspect facts, requirements, architecture, and uncertainty.
   - Builder: develop or implement the smallest verified solution.
   - Reviewer: review the actual Researcher and Builder results, risks, diff,
     and tests after those two return.
   The main Codex agent remains the Moderator and owns integration, verification,
   and the final answer.
5. Spawn a role, capture its genuine Codex subagent/thread identifier, and
   immediately call `scripts/conference-event.ps1 participant` with state
   `working` and that identifier as `-SourceRef` before substantive role work.
   Retain the returned `accepted_event_id`; pass it as `-CausationEventId` when
   publishing that role's one concise public final message. Retain every
   message receipt and pass genuine upstream message IDs to later roles with
   one `-SourceEventId` array containing the exact accepted message IDs.
   Reviewer must cite the Researcher and Builder message receipts. Moderator
   must cite every available Researcher, Builder, and Reviewer message receipt.
   Publish Moderator's own `working` event first and use its receipt as the
   Moderator message's `-CausationEventId`. Researcher and Builder remain
   independent inputs to Reviewer; do not claim that either caused the other.
6. If one participant fails, publish state `failed` with a sanitized public
   detail and continue when the remaining evidence is sufficient. Do not reuse
   an older participant result.
7. After review and integration, publish the Moderator's concise decision with
   kind `decision`, then call `finish` with the originating Codex task/turn ID
   as the source reference. The final response in Codex remains authoritative.

## Public-message boundary

- Publish only an agent's explicit final result or a faithful concise summary
  of that result.
- Never publish chain-of-thought, hidden instructions, private scratchpads,
  raw tool arguments, credentials, environment values, or unredacted logs.
- Do not invent dialogue, intent, relationships, success, or blockers.
- Researcher and Reviewer should normally be read-only. Avoid parallel writes
  to the same files. The Moderator reviews every change and runs verification.
- Keep each public message concise enough to hear comfortably. Put technical
  evidence in the Codex task and use the conference provenance disclosure for
  traceability.

## Event command examples

Run these from the repository root. Replace placeholders with observed IDs and
actual public results. Use the same Python resolution script through the
provided PowerShell wrapper.

```powershell
& <plugin-root>\scripts\conference-event.ps1 participant `
  -SessionFile <session-file> -Participant Researcher -State working `
  -SourceRef <actual-subagent-id>

& <plugin-root>\scripts\conference-event.ps1 message `
  -SessionFile <session-file> -Participant Researcher -Kind analysis `
  -SourceRef <actual-subagent-id> -Text <public-final-message> `
  -CausationEventId <research-working-event-id>

& <plugin-root>\scripts\conference-event.ps1 message `
  -SessionFile <session-file> -Participant Reviewer -Kind review `
  -SourceRef <actual-reviewer-id> -Text <public-review> `
  -CausationEventId <reviewer-working-event-id> `
  -SourceEventId <research-message-event-id>,<builder-message-event-id>

& <plugin-root>\scripts\conference-event.ps1 finish `
  -SessionFile <session-file> -Status completed `
  -SourceRef <originating-codex-task-id>
```

The Moderator message follows the same pattern as Reviewer, with kind
`decision`, its own accepted working receipt, and a `-SourceEventId` array
containing all available Researcher, Builder, and Reviewer message receipts.
If a role failed, omit only that unavailable message ID; never substitute an
older run's receipt.

If the bridge cannot start, continue the Codex task normally and report that
the conference view was unavailable. Conference/UI failure must not fail the
underlying work.
