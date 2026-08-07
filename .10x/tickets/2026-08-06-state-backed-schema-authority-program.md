Status: open
Created: 2026-08-06
Updated: 2026-08-06

# State-backed schema authority program

This is a parent planning/orchestration ticket. It is not executable implementation scope.

## Objective

Delete `cdf.lock` and make the configured state backend the sole active logical schema authority,
with immutable versions, per-resource/environment CAS, exact portable-plan preconditions, total
drift dispositions, fenced promotion settlement, and no compatibility or dual-authority period.

## Scope and sequence

1. **S1 — state schema-authority foundation.** Add kernel authority types/traits, SQLite current
   tables/transactions, batch first-use establishment, history, domain fencing, and reusable store
   conformance. Owner:
   `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`.
2. **S2 — preparation and portable-plan cutover.** Add stable project id, move selected
   plan/compile/run and derived artifacts to state bindings, implement all-selected first-use CAS,
   and replace portable lock preconditions with per-resource state preconditions. Owner:
   `.10x/tickets/2026-08-06-s2-state-backed-preparation-portable-plan.md`.
3. **S3 — total drift dispositions.** Replace evolution/quarantine switches with typed field/row/
   record/partition actions, exact trust presets, field-role safety, variant/quarantine telemetry,
   and plan-visible disposition facts. Owner:
   `.10x/tickets/2026-08-06-s3-schema-drift-dispositions.md`.
4. **S4 — promotion settlement migration.** Implement generation-bound run settlement permits,
   fenced promotion head state, complete committed-frontier cutoff, historical residual correction,
   and state-atomic publication. Owner:
   `.10x/tickets/2026-08-06-s4-state-backed-promotion-settlement.md`.
5. **S5 — lockfile surface deletion.** Delete lock models/parsers/CAS/hydration, lock-bound contract
   and inspect commands, artifact/report/system-SQL fields, fixtures/docs/generated artifacts, and
   lock-only publication machinery. Owner:
   `.10x/tickets/2026-08-06-s5-delete-lockfile-product-surface.md`.
6. **S6 — integration and release certificate.** Run the one broad closure barrier, release-binary
   sandbox journeys, current-only sweep, and final review. Owner:
   `.10x/tickets/2026-08-06-s6-state-schema-integration-certificate.md`.

S1 is first. S2 and S3 may proceed independently after S1. S4 depends on S1, S2, and S3. S5
depends on S2–S4 so deletion cannot strand a live authority path. S6 depends on every prior child.
No child adds Postgres, migration readers, aliases, dual authorities, future-only promotion, nested
promotion, or schema export/import.

## Integration boundaries

- `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md` retains orthogonal CDC,
  semantic, relational IR, and project-authoring authority. This program supersedes only its
  lockfile publication/manifest-binding assumptions.
- `.10x/tickets/done/2026-08-04-resource-first-cli-experience-program.md` remains authoritative for
  selectors, command intents, static validate, discovery/generation, plan terminal UX, one run
  verb, and live telemetry except where the new state specifications replace lock bindings.
- Existing exact variant codec, immutable packages, destination receipts, checkpoint commit gate,
  source-position safety, and destination correction strategies are reused rather than rebuilt.
- Concrete state implementation is SQLite only. Kernel/store contracts must remain suitable for a
  future Postgres implementation without adding it speculatively.

## Governing references

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/schema-drift-dispositions.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/specs/data-onramp-schema-intelligence.md`
- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/residual-variant-capture.md`
- `.10x/knowledge/fenced-lease-lock-publication.md`

## Acceptance criteria

1. State is the only active logical schema authority; versions are immutable and heads are isolated
   by authority domain/project id/environment/resource.
2. Plan is no-write; compile/run first use is exact, atomic, idempotent, and all-selected before
   effects; established schemas never evolve ordinarily.
3. Portable plans bind only relevant state generations/hashes and survive unrelated changes.
4. Every drift class receives a total compiled disposition with exact variant/quarantine/fail
   evidence and unchanged schema head.
5. Promotion prevents late old-generation settlement, reconciles complete retained top-level
   residual history, settles all targets, and advances head/publication atomically.
6. `cdf.lock` and every command/artifact/report/doc/code path whose only purpose is that authority
   are absent, with no compatibility machinery.
7. Static validate remains offline, terminal plan output remains polished, and release sandbox
   journeys prove first use, drift, quarantine, fail, promotion, portable execution, and recovery.

## Journal

- 2026-08-06: Opened after the user ratified stable `[project].id`, the four exact trust presets,
  first-use/`--locked` semantics, state-atomic promotion, and full current-only lockfile deletion.
  The governing decision and focused specifications were authored in this shaping turn. Per the
  explicit handoff and 10x separation, no implementation begins in the same turn.
- 2026-08-06: S1 closed with backend-neutral authority contracts, the current-only SQLite store,
  reusable conformance, 76/76 SQLite tests, and strict affected-package Clippy. S2 and S3 are now
  dependency-unblocked; S4 still depends on both.

## Blockers

None. Children remain dependency-gated by the graph above.

## Evidence

Pending child execution.

## Review

Pending child and final integration review under the then-current coordination policy.

## Retrospective

Pending program closure.
