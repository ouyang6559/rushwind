# Security Policy

## Reporting a Vulnerability

Use GitHub's private vulnerability reporting on this repository
(*Security → Report a vulnerability*), or contact the maintainer directly.

Please include:

- The affected crate and version
- Reproduction steps or proof of concept
- Impact assessment

Expect acknowledgement within 72 hours. Coordinated disclosure timelines
are flexible for a volunteer-maintained project; we will keep you posted.

## Scope

In scope: every crate in this workspace.

Out of scope:

- Vulnerabilities in upstream protocol stacks (axum, tokio, quinn, …) —
  report those upstream
- Example programs run in untrusted environments
- Anything requiring an already-compromised host

## Security invariants

- The entire workspace compiles with `#![forbid(unsafe_code)]`.
- Every shutdown phase is deadline-bounded; a hanging or malicious
  `Server::stop` cannot wedge a process indefinitely (see
  `docs/threat-model.md`).
- A panicking server is isolated and recorded; it never skips sibling
  teardown.
- Note: `catch_unwind`-based panic isolation requires an unwind panic
  strategy; processes built with `panic = abort` trade that isolation for
  immediate termination.
