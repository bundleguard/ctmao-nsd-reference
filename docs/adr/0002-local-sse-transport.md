# ADR 0002: Server-Sent Events for the Local V1 UI

- Status: Accepted
- Date: 2026-08-21

## Context

The first local UI needs a live browser view of agents, delegation, timeline,
and transcript. Its primary transport is one-way server-to-browser event
delivery. The runtime core is standard-library-only and V1 should remain small.

## Options considered

1. Poll JSON snapshots.
2. Use RFC 6455 WebSockets.
3. Use Server-Sent Events (SSE) for events and ordinary HTTP only if later
   controls require server mutation.

## Decision

Adopt option 3 for V1. SSE matches one-way delivery, uses browser-native
reconnection, preserves text/event IDs, and can be implemented with the Python
standard library alongside static HTML/CSS/JavaScript. Mute, mode, and voice
overrides are client-local initially. Small authenticated JSON POST endpoints
may be added when server-side controls are genuinely needed.

The server binds to `127.0.0.1` by default on a random available port, requires
an unguessable per-session token, validates `Origin`, sends no permissive CORS
headers, and marks transcript/event responses `Cache-Control: no-store`. A
bounded redacted replay buffer supports `Last-Event-ID`; heartbeat comments
detect dead clients. Each connection is isolated and backpressure cannot block
orchestration.

## Consequences

V1 avoids a WebSocket dependency and bidirectional protocol state. SSE is UTF-8
text and not suited to binary audio; audio remains a separate provider/browser
concern. WebSockets remain a future option if measured requirements demand
low-latency bidirectional controls or binary streaming. This decision makes no
claim that SSE itself supplies authentication, confidentiality, or compliance.

