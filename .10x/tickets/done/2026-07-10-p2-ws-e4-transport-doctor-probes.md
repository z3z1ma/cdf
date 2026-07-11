Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-ws-e-remote-transports.md
Depends-On: .10x/tickets/done/2026-07-10-p2-ws-e3-cloud-object-stores-and-http-templates.md

# P2 WS-E4 — Transport doctor probes

## Scope

Make `cdf doctor` preflight every configured remote file resource through the production transport facade, including credential resolution, egress enforcement, reachability, listing/template enumeration, metadata, and bounded format confirmation.

## Acceptance criteria

- Each remote file resource produces a named doctor check with resource id, transport kind, and matched-file count but no credential or signed-URL material.
- A transport failure is reported as a failed doctor check and makes the command nonzero without preventing unrelated checks.
- Doctor uses the same transport and partition resolution as plan/run; it does not create state, package, or destination artifacts.
- Deterministic adapter tests and CLI report tests cover success, failure, and redaction.

## Explicit exclusions

Extracting records or writing packages. Live cloud provider availability remains the WS-I nightly tier.

## Evidence expectations

CLI JSON/human tests, transport fixture tests, clippy, and adversarial review.

## Blockers

None.

## Progress and notes

- 2026-07-10: Opened from the remaining WS-E parent scope under the user's autonomous ratification authority.
- 2026-07-10: `cdf doctor` now probes every configured remote file resource through production partition resolution and reports isolated, redacted checks without extraction or writes.
- 2026-07-10: Full CLI verification exposed and repaired an exhaustive-evolve regression in the preceding discovery refactor while retaining sampled-pin quarantine semantics. Evidence: `.10x/evidence/2026-07-10-p2-ws-e4-transport-doctor.md`. Review: `.10x/reviews/2026-07-10-p2-ws-e4-transport-doctor-review.md`.
