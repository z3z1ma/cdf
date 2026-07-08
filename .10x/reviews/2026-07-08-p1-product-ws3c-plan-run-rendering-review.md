Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md
Verdict: pass

# P1 product WS3C plan, run, and replay rendering review

## Target

Implementation and closure evidence for `.10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md`.

Evidence: `.10x/evidence/2026-07-08-p1-product-ws3c-plan-run-rendering.md`.

## Assumptions tested

- WS3C must migrate only plan, explain, run, and replay package human outputs.
- JSON output must remain unchanged unless explicitly needed and tested.
- Plan/explain panels must show pushdown, destination, guarantee, contract, migration, and next-command sections where data exists.
- Plan/explain next commands must not teach users to mint package/checkpoint identifiers that the system can mint, and must not drop an explicit `--to` destination from the operator's plan/explain invocation.
- Run output must make the checkpoint gate visible without implementing live progress.
- Replay package output must show duplicate/no-op, receipt, and checkpoint facts without migrating state/recovery/backfill/inspect-run families.
- Redaction must prevent secret-like destination URI/userinfo values from leaking into rendered output.
- Non-WS3C runtime/event-spine behavior must not be folded into WS3C.

## Findings

No blocking findings.

Pass: `scan_command.rs` now uses the renderer for plan/explain human output while leaving the existing JSON report structure unchanged. The panels cover fetch, pushdown, destination, guarantee, contract, migration, optional migration table, and next command.

Pass: the plan/explain next command was corrected during review. It now emits `cdf run <resource>` for default targets with no explicit destination, adds only `--target <target>` when the target differs, preserves explicit `--to <destination>` values, and redacts URI userinfo in displayed destinations. It does not include `--package-id`, checkpoint IDs, or any other system-minted identifier.

Pass: `RunCliReport::render_document` renders the required checkpoint-gated run facts: run/resource/pipeline/target, package status/hash/dir, rows/segments, verdicts, receipt source, and the condition that destination receipt verification precedes checkpoint commit.

Pass: `ReplayPackageCliReport::render_document` renders replay/package facts, destination, duplicate/no-op state, receipt facts, and checkpoint state through the same renderer primitives.

Pass: command call sites for `run` and `replay package` only change human rendering handoff to `CommandOutput::rendered`. Runtime execution and replay logic are not changed.

Pass: redaction was strengthened narrowly with URI userinfo redaction and tests. The helper is conservative and does not claim to be a general secret scanner.

Pass: focused tests include headless/static CLI assertions and forced-rich renderer assertions for migrated command families, plus JSON compatibility and redaction tests.

Pass: initial verification was run in a clean detached worktree with only the WS3C diff applied while WS1C was still dirty. After WS1C was committed, post-correction main-workspace verification passed `cargo check -p cdf-cli --all-targets --locked`, focused next-command/plan/explain/redaction/JSON tests, clippy, fmt, unsafe scan, Semgrep, Gitleaks, scoped diff checks, and full `cargo test -p cdf-cli --locked` after stale WS1C event-count/schema expectations were aligned.

Residual risk: the renderer tables/panels still use simple character counts inherited from WS3B. WS3B already recorded Unicode display-width risk; WS3C does not introduce arbitrary wide Unicode cell content beyond current command facts.

Residual risk: human output now has richer panel text but remains static. That is intentional; WS5 owns live progress.

## Verdict

Pass. The WS3C acceptance criteria are supported by implementation, tests, evidence, and review after correcting the plan/explain next-command grammar and explicit destination preservation.

## Residual risk

WS3D still needs to migrate recovery/state/backfill/inspect-run command families. WS3E still needs the migration gate preventing new raw human output from bypassing the renderer.
