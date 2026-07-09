Status: done
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md
Depends-On: .10x/specs/data-onramp-conformance.md, .10x/knowledge/runtime-conformance-throughput-rule.md

# P2 WS-I1 friction regression registry

## Scope

Create the durable conformance-side registry that maps the eighteen P2 field-test frictions to owning tests or explicitly open test gaps, so P2 closure cannot claim "history" without a named regression guard.

Owned write scope:

- `.10x/evidence/2026-07-08-p2-friction-regression-registry.md`
- `crates/cdf-conformance/**` only if a lightweight registry/test helper is useful without depending on unfinished WS-A/B/C/D implementation

## Acceptance criteria

- The evidence record lists all eighteen frictions from the P2 directive.
- Each row names one of:
  - an existing test/conformance scenario that already catches the recurrence;
  - an open P2 executable ticket that must add the guard;
  - an explicit recorded exclusion with rationale.
- No friction is marked covered by a test unless the test's assertions actually cover that behavior.
- The registry is linked from the P2 parent and WS-I ticket progress notes.
- If a code helper is added, it must not assert behavior that is not yet implemented; it may only make missing coverage visible.

## Evidence expectations

Record focused evidence for:

- source/record inspection used to classify existing versus missing coverage;
- `cargo test -p cdf-conformance --locked` if any conformance code changes;
- `jscpd` scoped to any new records/helpers;
- `git diff --check`.

## Explicit exclusions

This ticket does not implement S1-S8 scenarios or mark any P2 workstream complete. It is the closure map that future implementation tickets must fill in.

## Progress and notes

- 2026-07-08: Opened as the first WS-I executable slice so harness ownership starts with the implementation lanes rather than after them.
- 2026-07-09: Completed the friction registry in `.10x/evidence/2026-07-08-p2-friction-regression-registry.md`. All eighteen rows are classified; none has complete P2 coverage yet, and partial primitive/negative coverage is explicitly called out. Linked the registry from the P2 parent and WS-I progress notes. No conformance code helper was added.
- 2026-07-09: Verification passed: `git diff --check`; `gitleaks dir --no-banner --redact` over the new/changed `.10x` records; `jscpd` scoped to `.10x/evidence/2026-07-08-p2-friction-regression-registry.md` with 0 clones. `cargo test -p cdf-conformance --locked` was not run because no `crates/cdf-conformance/**` code changed.
- 2026-07-09: Parent review recorded in `.10x/reviews/2026-07-09-p2-ws-i1-friction-regression-registry-review.md`; ticket moved to `.10x/tickets/done/`.

## Blockers

None.
