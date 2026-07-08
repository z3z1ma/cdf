Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-init-scaffold.md
Verdict: pass

# cdf init scaffold review

## Target

Review of the `cdf init` scaffold implementation:

- `crates/cdf-project/src/scaffold.rs`
- `crates/cdf-project/src/lib.rs`
- `crates/cdf-project/src/tests.rs`
- `crates/cdf-cli/src/project_command.rs`
- `crates/cdf-cli/src/tests.rs`
- `.10x/decisions/cdf-init-local-scaffold-defaults.md`
- `.10x/evidence/2026-07-08-cli-init-scaffold.md`

## Assumptions tested

- The CLI must not own or duplicate project-format semantics that belong in `cdf-project`.
- `init` must not create runtime artifacts such as `.cdf`, checkpoint stores, destination databases, packages, lockfiles, resolved secrets, or data rows.
- Existing files must fail closed without `--force`.
- `--force` must be scoped to scaffold-owned paths and must not destroy unrelated runtime or user data.
- The scaffold shape must be record-ratified before source implementation lands.

## Findings

No blocking findings.

The worker implementation originally placed scaffold semantics in `cdf-cli`; parent review corrected this by moving template/write ownership into `cdf-project::write_local_project_scaffold` and keeping `project_command::init` as argument normalization plus output formatting. This preserves the CLI/project boundary required by the parent CLI ticket.

The overwrite model is appropriately narrow. Unforced `cdf.toml`, `resources/files.toml`, and `data` conflicts fail as contract errors. `--force` replaces scaffold-owned files and non-directory scaffold path obstructions, while existing `resources/`, existing `data/`, `.cdf/state.db`, `.cdf/packages`, `cdf.lock`, and unrelated files remain in place. Existing files under `data/` are preserved by current implementation and tests.

The scaffold contents are intentionally minimal and are ratified in `.10x/decisions/cdf-init-local-scaffold-defaults.md`: one local NDJSON file resource, dev SQLite state URI, local package path, DuckDB destination URI, and no secrets or runtime artifacts. This is sufficient for the first local developer loop without inventing remote template, credential, or execution defaults.

Security and safety checks did not show new source risk. Semgrep and gitleaks were clean for touched paths; direct unsafe search found no unsafe surface in the changed source. `cargo geiger` itself failed due tool/parser behavior outside this diff, so it is not used as pass evidence.

## Verdict

Pass. The implementation satisfies the ticket acceptance criteria and is safe to close with the evidence in `.10x/evidence/2026-07-08-cli-init-scaffold.md`.

## Residual risk

Full CLI surface completion remains active in the parent ticket. This closure only covers `cdf init`; it does not close preview breadth, contract registry/freeze/test, state migrate/recover, backfill, package GC retention, or status freshness.

The default scaffold is deliberately local-first. Additional scaffold templates or environment-specific variants need separate ratification before implementation.
