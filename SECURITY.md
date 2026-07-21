# Security Policy

## Supported version

Security and correctness fixes currently target the latest `0.1.x` release.

## Reporting a vulnerability

Do not publish sensitive vulnerability details in a public issue. Use GitHub's
private vulnerability reporting feature for this repository when available, or
contact the repository owner through the owning GitHub organization.

Include the affected version, a minimal reproduction, expected impact, and any
suggested mitigation. Avoid including credentials, personal data, or production
records.

## Scope note

CTMAO-NSD thread isolation is an ownership and concurrency boundary, not a
process sandbox or security boundary. Applications requiring hostile-code or
tenant isolation must use processes, containers, or stronger isolation.
