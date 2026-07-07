Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-mechanical-cdf-identity-rename.md

# Mechanical CDF identity rename evidence

## What was observed

The repository has been mechanically renamed to the CDF identity described by `VISION.md` D-24.

Observed outcomes:

- Root authority is now `VISION.md`; the old root book file is removed.
- First-party Rust package paths are `crates/cdf-*`; Cargo package names use `cdf-*`; Rust crate targets use `cdf_*`.
- Cargo exposes the CLI binary target as `cdf` while the package remains `cdf-cli`.
- Python SDK path/import is `python/cdf_sdk` / `cdf_sdk`.
- Direct project-name strings in source and `.10x/` records were mechanically rewritten to CDF forms.
- `.gitignore` remains dirty from pre-existing user work and was excluded from this ticket's stage plan.

## Procedure

Mechanical actions:

- Renamed first-party crate directories from the legacy prefix to `crates/cdf-*`.
- Renamed the Python SDK directory to `python/cdf_sdk`.
- Renamed `.10x/` record paths containing the legacy project name to CDF paths.
- Rewrote direct source and record text references from the legacy project identity to CDF forms.
- Added an explicit `[[bin]]` target named `cdf` in `crates/cdf-cli/Cargo.toml`.
- Updated the CLI integration-test binary hook to `CARGO_BIN_EXE_cdf`.
- Regenerated only the deterministic live-run golden hashes that changed because the CDF identity changes package bytes.

Verification commands and results:

- `rg` legacy identity scan over `Cargo.toml`, `Cargo.lock`, `crates`, `python`, `.10x`, `VISION.md`, `QUALITY.md`, `deny.toml`, `supply-chain`, and `tools`, excluding generated outputs: no matches.
- Root book-reference scan for old book paths over `.10x`, `VISION.md`, Cargo, source, and Python: no matches.
- `find crates -maxdepth 1 -type d`: reports only first-party `crates/cdf-*` directories.
- `find python -maxdepth 1 -type d`: reports `python/cdf_sdk` and no legacy SDK directory.
- `cargo metadata --format-version=1 --locked --no-deps`: passed; 17 workspace members; `cdf-cli` targets include `cdf`.
- `cargo fmt --all -- --check`: passed after formatting.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `python3 -m compileall -q python/cdf_sdk python/examples`: passed.
- `PYTHONPATH=python python3 -c 'import cdf_sdk; print(cdf_sdk.__name__)'`: printed `cdf_sdk`.
- `python3 -m json.tool crates/cdf-conformance/golden/live-local-file-v1/expected.json`: passed.
- `cargo test -p cdf-conformance live_run::tests::live_local_file_v1_matches_committed_golden_across_100_runs --locked`: passed.
- `cargo test --workspace --locked --no-fail-fast`: passed, including doc tests.
- `cargo deny check`: passed; emitted existing duplicate-dependency warnings, then reported advisories, bans, licenses, and sources OK.
- `cargo audit`: passed after scanning `Cargo.lock`.
- `osv-scanner scan source -r .`: passed, no issues found.
- `gitleaks detect --no-git --redact --source <tracked-candidate-copy> --verbose --no-banner`: passed, no leaks found.
- `git diff --check`: passed.

Skipped by explicit current goal:

- CodeQL.
- Mutation testing.

## What this supports

This supports closing the mechanical CDF identity rename ticket: in-scope source and `.10x/` references no longer carry the legacy project identity, the renamed Cargo/Python surfaces build and test, and the deterministic package evidence was updated to the new byte identity.

## Limits

This evidence does not claim semantic terminology cleanup. Mechanically transformed line-metaphor wording was intentionally out of this ticket's scope and was later handled by `.10x/tickets/done/2026-07-07-semantic-commit-gate-terminology-cleanup.md`.

The first full-tree gitleaks attempt was interrupted because it scanned generated `target/**` vendor output and produced only generated-file noise; the recorded secret-scanning evidence is the later tracked-candidate scan.
