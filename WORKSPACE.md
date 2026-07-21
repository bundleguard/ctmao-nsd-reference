# Codex Workspace Organization

## Repository Purpose

This workspace contains the canonical reference implementation of
**Cross-Thread Multi-Agent Orchestration with Nested Sub-Agent Delegation
(CTMAO-NSD)**.

It is intentionally separate from the AI Product Framework repository. The AI
Product Framework decides whether and how a product should be built. This
repository defines one optional runtime architecture that a qualifying product
may consume.

## Recommended Codex Task Structure

Keep tasks focused and avoid duplicating implementation across conversations:

1. **CTMAO-NSD Reference Implementation** — canonical architecture and v0.1
   runtime; this task owns integration decisions.
2. **CTMAO-NSD Verification and Hardening** — concurrency tests, failure modes,
   performance measurements, and release readiness.
3. **CTMAO-NSD Roadmap** — message bus, persistence, adapters, and future
   releases after the reference implementation is stable.

Product-specific integrations belong in the consuming product's Codex project,
not in this repository. The AI Product Framework should retain a single
integration task that owns the cross-repository contract.

## Repository Rules

- Do not nest another Git repository inside this one.
- Do not copy the engine into the AI Product Framework documentation repository.
- Keep runtime dependencies standard-library-only unless a future approved
  release changes that constraint.
- Preserve the exact canonical name and introduce the abbreviation CTMAO-NSD
  after its first use.
- Run the complete unit suite and two-thread example before each handoff.
