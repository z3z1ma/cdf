Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md
Verdict: pass

# CLI preview resource breadth review

## Target

Review of the `cdf preview` resource breadth slice governed by `.10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md`, `.10x/decisions/preview-one-batch-sampling-semantics.md`, and `.10x/specs/project-cli-observability-security.md`.

## Findings

No blocking findings.

The main semantic risk was that preview could appear to honor filters, projection, or limits while bypassing the engine residual path. The implementation addresses this with `validate_preview_direct_stream_plan`: residual predicates, unpushed projection, and unpushed limit fail closed before the resource stream opens. REST and SQL preview success paths are therefore limited to operations carried by their runtime partition metadata.

The no-write contract is directly tested for successful file/REST/SQL preview and fail-closed unsupported cases. The helper checks package, checkpoint, DuckDB, Parquet, and SQLite state artifacts, which covers the write surfaces owned by this ticket. Run-ledger writes are not expected from preview and no project runtime run path is invoked.

REST and SQL secrets flow through project secret providers rather than ad hoc CLI handling, and tests assert the secret values are absent from command output. Source-level Gitleaks, Semgrep, CodeQL, and direct unsafe scans found no new security issue. The full-history Gitleaks findings are pre-existing and already owned by `.10x/tickets/2026-07-08-historical-gitleaks-findings-triage.md`.

The code shape is acceptable for this slice. Preview orchestration remains in `scan_command.rs`, but the new lower behavior is pushed into `cdf-declarative` and `cdf-formats` rather than expanding generic `commands.rs`. Complexity and duplication metrics do not show a new hotspot from the preview additions.

## Verdict

Pass. Acceptance criteria are supported by `.10x/evidence/2026-07-08-cli-preview-resource-breadth.md`. Residual risk is limited to the deliberate product boundary that preview is a one-batch direct-stream sample, not a package-free engine execution mode.
