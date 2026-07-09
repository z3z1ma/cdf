Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md, .10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/specs/cli-error-experience-catalog.md

# P2 WS-G2 type mismatch and command-context diagnostics

## Scope

Make current Parquet and JSON-family schema-reconciliation failures actionable at `cdf validate --deep` and plan time. Normalize the operator-facing error path so it names the command, resource, source location where known, field, observed physical type, constraint type, and the two applicable fixes without falling back to generic remediation.

This is a bounded diagnostics slice over already supported local/HTTPS Parquet and local JSON/NDJSON sources. It may add typed error context or catalog entries where needed, but must not duplicate reconciliation logic in the CLI.

## Acceptance criteria

- Unsupported/lossy/parse-gated mismatches surface the exact field, observed type, constraint type, and specific remediation alternatives from the shared reconciliation result.
- `cdf validate --deep`, `cdf plan`, and `cdf preview` identify their own command; no error written for `cdf run` leaks into them.
- File path/URL and compiled resource id are included where safely known, with signed URLs and secrets redacted.
- The error catalog uses source-specific stable codes/remediation rather than the generic project/schema retry text.
- Deep validation catches these mismatches before extraction, package creation, destination writes, checkpoint changes, or ledger mutation.
- Existing JSON row-local quarantine remains a successful governed outcome rather than being incorrectly promoted to a command failure.

## Evidence expectations

Human/JSON snapshots for Parquet widening/lossy mismatch and JSON parse/lossy gates, command-name construction-site tests, no-write tree snapshots, signed-URL/secret redaction adversarial cases, catalog coverage, and applicable `QUALITY.md` security/input/test profiles.

## Explicit exclusions

Cloud-specific errors, future compression/format-detection diagnostics, Python/WASM sources, the full P1 catalog, and implementation of new coercion behavior are outside this ticket.

## Progress and notes

- 2026-07-09: Opened after G1 established the deep-validation doorway and B6 made reconciliation decisions exact; the remaining gap is preserving that specificity through CLI command/error rendering.

## Blockers

None for the supported source set.
