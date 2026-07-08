Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md
Verdict: pass

# P1 product WS3D recovery, state, and backfill rendering review

## Target

Implementation and closure evidence for `.10x/tickets/done/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md`.

Evidence: `.10x/evidence/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md`.

## Assumptions tested

- WS3D must migrate only `resume`, `state show/history/rewind/migrate/recover`, `backfill`, and `inspect run`.
- JSON output must remain stable and must not be coupled to the new human renderer.
- Recovery guidance must expose failed phase, durable artifacts, mutation performed, and next command.
- State outputs must be panels/tables rather than raw sentences.
- Backfill should reuse renderer plan/panel/table primitives without changing backfill planning or execution behavior.
- `inspect run` must make missing artifacts visible and must not leak secret-like display values through the renderer.
- No parser grammar, runtime behavior, unrelated command families, or unrelated dirty worktree changes should be folded into WS3D.

## Findings

No blocking findings.

Pass: the command call sites now hand off `RenderDocument` values for the WS3D families while preserving existing report structs and JSON serialization.

Pass: resume reports preserve fail-closed nonzero exit codes while rendering structured recovery panels. The rendered report includes failed phase, action/result, source contact, mutation required/performed, durable package/checkpoint/receipt facts, destination display with URI-userinfo redaction, ledger event counts, guidance, and next command.

Pass: inspect-run output now shows recovery state/action, artifacts, missing package and receipt counts, package artifact availability with full first issue, duplicate status, and an event table. Renderer display redaction is tested with a crafted package path containing URI userinfo.

Pass: state show/history/rewind/migrate/recover now use panels and tables. History includes a full-ID summary panel before the width-aware table so truncated table cells do not hide the material checkpoint IDs. Recover explicitly states that destination rows were not written and lists evidence limits.

Pass: parent review found that follow-up commands could preserve the old raw `--scope-json` grammar when the original command used it. The renderer now teaches `--scope key=value` for lossless object scopes, with a focused regression test.

Pass: backfill dry plans and executed backfills now render backfill facts, write/mutation state, slice rows, and next commands without changing backfill execution semantics or JSON fields.

Pass: focused tests cover headless and rich rendering for WS3D command families, existing JSON sentinels continue to pass, and full `cdf-cli` tests/clippy/fmt/security scans/jscpd/complexity metrics/CodeQL passed.

Residual risk: some next-command affordances remain conservative placeholders where the command cannot safely infer exact arguments, such as `cdf run <resource>` after a no-finalized-package resume failure and `cdf state history <resource>` after executed backfill. This is preferable to minting unratified identifiers or guessing source/runtime arguments.

Residual risk: renderer tables still inherit WS3B's simple character-width truncation. WS3D mitigates material full values in state history and inspect package artifacts with summary panels where needed.

Residual risk: unrelated dirty workspace changes existed during verification and could affect workspace-wide CodeQL/cargo-check inputs. WS3D changed only the scoped CLI files and WS3D records.

## Verdict

Pass. WS3D acceptance criteria are supported by implementation, tests, evidence, and adversarial review.

## Residual risk

Remaining command-family renderer migration and the future raw-output migration gate are outside WS3D and remain owned by later work.
