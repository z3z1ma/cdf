Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: None

# Mechanical CDF identity rename

## Scope

Mechanically rename the repository from its legacy identity to CDF according to `VISION.md` D-24 while preserving behavior.

Owns:

- root book files: commit `VISION.md` and remove the old root book file;
- workspace members and crate directories to `crates/cdf-*`;
- Cargo package/dependency names to `cdf-*` and Rust crate paths to `cdf_*`;
- first-party Rust identifiers and strings that directly encode the project name, including `CdfError`, `CdfLock`, `_cdf_*`, `cdf.toml`, `cdf.lock`, and the CLI binary name where Cargo exposes it;
- Python SDK package paths and imports to `python/cdf_sdk` / `cdf_sdk`;
- `.10x/` records, specs, tickets, evidence, reviews, decisions, and knowledge references to the CDF identity layer.

## Acceptance Criteria

- Residual legacy-name scan over `Cargo.toml`, `Cargo.lock`, `crates`, `python`, `.10x`, `VISION.md`, `QUALITY.md`, `deny.toml`, and `supply-chain` returns no in-scope matches.
- `find crates -maxdepth 1 -type d` shows CDF-prefixed first-party crate directories and no legacy-prefixed first-party crate directories.
- `find python -maxdepth 1 -type d` shows `python/cdf_sdk` and no legacy SDK directory.
- Cargo workspace metadata loads after the rename.
- Focused tests for renamed Rust crates and Python package imports pass.
- The change is mechanical: no semantic replacement of the former line metaphor with commit-gate terminology unless it is a direct project-name string rename.
- `.gitignore` is not edited or staged.

## Evidence Expectations

Record mechanical rename commands, residual match scans, `cargo fmt --all -- --check`, `cargo metadata --format-version=1 --locked --no-deps`, focused `cargo check`/`cargo test` for affected crates, Python compile/import checks, `git diff --check`, and at least the relevant fast `QUALITY.md` gates. Use `tools/codeql-rust-quality.sh` only if Rust source changes remain after the compile/test repair loop.

## Explicit Exclusions

No semantic redesign, no native Parquet implementation, no commit-gate terminology rewrite beyond mechanical project-name replacement, no dependency upgrades except lockfile metadata required by package renames, no `.gitignore` edits, no CI workflow changes unless build commands prove they are required for the mechanical rename.

## References

- `VISION.md` D-24 and Preface.
- `.10x/decisions/cdf-system-authority.md`
- `.10x/decisions/cdf-book-decision-register.md`
- `.10x/knowledge/cdf-product-objective.md`
- `QUALITY.md`

## Progress and Notes

- 2026-07-07: Opened after user directed the project rename to CDF and confirmed `VISION.md` is the new comprehensive book. The first pass should be mechanical and behavior-preserving; semantic metaphor cleanup can follow after the renamed workspace builds.
- 2026-07-07: Mechanically renamed first-party crate/package paths, Rust crate imports, CLI binary target, Python SDK path/imports, root book authority, generated golden identity hashes, tool-script fingerprint naming, and `.10x/` CDF identity references. Closure evidence is `.10x/evidence/2026-07-07-mechanical-cdf-identity-rename.md`; closure review is `.10x/reviews/2026-07-07-mechanical-cdf-identity-rename-review.md`. Semantic terminology cleanup is deliberately split to `.10x/tickets/2026-07-07-semantic-commit-gate-terminology-cleanup.md`.

## Blockers

None.
