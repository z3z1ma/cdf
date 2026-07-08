Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/destination-receipts-guarantees.md, .10x/specs/run-orchestration-ledger.md

# Implement plan/explain DDL and guarantee output

## Scope

Implement the lower-layer planning facade needed for `cdf plan` and `cdf explain` to show scan/resource schema, pushdown fidelity, destination DDL preview, delivery guarantee, and state-advancement preview before bytes move.

Owns:

- `crates/cdf-cli/src/scan_command.rs` plan/explain report assembly and tests.
- Lower `cdf-project`, `cdf-engine`, and destination planning APIs needed to dry-run destination DDL and guarantee derivation without creating packages, destination data, or checkpoints.
- JSON/human output stability for automation-relevant fields.

## Acceptance criteria

- `cdf plan` and `cdf explain` expose what will be fetched, pushdown fidelity, destination DDL/migration preview, delivery guarantee, and state-advancement rule for supported resource/destination/disposition combinations.
- Guarantee text is mechanically derived from destination sheet/idempotency/disposition facts and matches `.10x/specs/destination-receipts-guarantees.md`.
- Plan/explain remain no-write commands: no package directory, destination mutation, receipt, checkpoint, or run-ledger event is created.
- Unsupported destination/resource/disposition combinations fail closed or report explicit unsupported details without pretending a guarantee.
- JSON fields remain stable for automation, and human output remains scheduler-friendly.

## Evidence expectations

Run focused CLI plan/explain tests, no-write assertions, guarantee-table representative tests, relevant lower-layer planner tests, `cargo fmt --all -- --check`, `cargo clippy -p cdf-cli -p cdf-project -p cdf-engine --all-targets --locked -- -D warnings`, `cargo check --workspace --all-targets --locked`, `git diff --check`, Jscpd/rust-code-analysis over touched CLI/planner modules, and applicable security scans.

## Explicit exclusions

No source execution, no package creation, no destination writes, no checkpoint commits, no scheduler behavior, and no unqualified "exactly-once" product claim.

## Blockers

None. The governing guarantee table is ratified; if implementation needs a new guarantee class, add or supersede a decision/spec before source edits.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Current `scan_command` reports pushdown and state-advancement text but marks DDL preview unsupported because scan-to-destination planning is not exposed yet.
- 2026-07-08: Worker A implemented a no-write project destination planning facade and wired `cdf plan`/`cdf explain --target` JSON/human reports to resource schema, destination sheet, DDL/migration preview, derived delivery guarantee, and state-advancement preview. Ticket remains open for parent review, evidence, and closure.
- 2026-07-08: Parent review closed the slice with evidence `.10x/evidence/2026-07-08-cli-plan-explain-ddl-guarantee.md` and review `.10x/reviews/2026-07-08-cli-plan-explain-ddl-guarantee-review.md`. Focused CLI/project tests, workspace tests, fmt/check/clippy, Jscpd, rust-code-analysis, cargo machete, cargo deny/audit/vet, Semgrep, OSV, CodeQL through the reusable DB path, and source-only Gitleaks passed or matched ratified limits.
