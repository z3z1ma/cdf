Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-product-ws4-error-experience-catalog.md
Depends-On: .10x/specs/cli-error-experience-catalog.md, .10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md

# P1 product WS4B: Error construction-site migration

## Scope

Migrate all `cdf-cli` error construction sites to stable catalog codes and remediation where useful.

Primary write scope is `crates/cdf-cli/src/**`, focused tests, generated catalog source data if introduced, and this ticket's records. Keep changes surgical and avoid command behavior changes beyond structured error metadata.

## Acceptance criteria

- Every direct `CliError::usage`, `CliError::not_supported`, and `CliError::from(CdfError::...)` construction site either supplies a specific stable code/remediation or is covered by an explicit documented generic mapping.
- Exit codes and `ErrorKind` behavior remain unchanged.
- Not-supported paths name the required lower layer or owning ticket/layer where known.
- Representative errors for every command family have snapshot or JSON assertions covering code/remediation.
- Redaction checks cover destination URIs, secret references, Python interpreter stderr/stdout, SQL text, and project paths where relevant.

## Evidence expectations

Record construction-site inventory before/after, focused command tests, JSON compatibility tests, redaction adversarial tests, and required `QUALITY.md` checks, including jscpd, complexity reports, Semgrep, Gitleaks, cargo audit/deny/vet, and reusable CodeQL if Rust source changed materially.

## Explicit exclusions

Do not implement edit-distance suggestions except where needed to keep a migrated construction site coherent. Do not generate docs; WS4D owns docs generation.

## Progress and notes

- 2026-07-08: Split from WS4. Source inspection found many construction sites across `args.rs`, command modules, and `system_sql.rs`; this child owns the inventory and migration.
- 2026-07-08: Implemented command-family error mappings, migrated production construction sites where specific product codes were useful, documented generic parser/lower-layer mappings, added representative JSON code/remediation and redaction tests, and recorded evidence in `.10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`.
- 2026-07-08: Closure review passed in `.10x/reviews/2026-07-08-p1-product-ws4b-error-construction-site-migration-review.md`; ticket moved to done.
- 2026-07-08: Parent integration review repaired moved-ticket references and reran the WS4B quality gates; results appended to `.10x/evidence/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`.

## Blockers

None. WS4A is complete.
