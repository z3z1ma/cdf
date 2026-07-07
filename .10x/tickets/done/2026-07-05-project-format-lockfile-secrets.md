Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-declarative-resources.md

# Implement project format, lockfile, and secrets

## Scope

Implement `cdf-project`: `cdf.toml` parsing, environment overlays, resource source resolution, retention policy model, Python interpreter configuration, secret URI model, secret providers for env/file/OS keychain where feasible, semantic lockfile generation/diffing, and project validation APIs. Owns `crates/cdf-project/**`.

## Acceptance criteria

- Book `cdf.toml` shape parses into typed configuration.
- Environment overlays inherit unspecified settings.
- Secret values are rejected in serialized artifacts where references are required.
- `cdf.lock` captures dependency tuple, resource capability hashes, destination sheets, type mappings, contract snapshots, schema hashes, and normalizer version.
- Validation can check secret resolvability without printing values.

## Evidence expectations

Record config parser tests, overlay tests, lockfile snapshot/diff tests, secret redaction tests, and validation tests.

## Explicit exclusions

No CLI rendering; no destination-driver implementation.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to project/secrets worker. Worker owns `crates/cdf-project/**` plus its own evidence/review records and this ticket. Do not touch `.gitignore`, parent ticket, CLI crate, destination crates, or unrelated records.
- 2026-07-06: Implemented `cdf-project` typed project parsing, environment overlays, declarative source resolution hooks, retention/Python/defaults models, env/file secret providers, redaction-preserving validation, and semantic lockfile generation/diffing. Evidence recorded in `.10x/evidence/2026-07-06-project-format-lockfile-secrets.md`; blocked review recorded in `.10x/reviews/2026-07-06-project-format-lockfile-secrets-review.md`.
- 2026-07-06: Parent integration revalidated the ticket after concurrent DuckDB and Python work restored a loadable workspace and cleared PyO3 advisory failures. `cargo fmt --all -- --check`, integrated package tests, integrated clippy, `cargo audit`, `cargo deny check advisories`, OSV, pyright/compileall, and `git diff --check` passed. Review updated to pass and this ticket is closed.
- 2026-07-06: Split the large `crates/cdf-project/src/lib.rs` into focused files under `crates/cdf-project/src/` while preserving the crate-root API. Organization evidence recorded in `.10x/evidence/2026-07-06-rust-crate-organization-refactor.md`.
- 2026-07-06: Replaced the intermediate `include!` split with ordinary Rust modules under `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`. Final parent quality gates recorded in `.10x/evidence/2026-07-06-project-python-destinations-quality-gates.md`.

## Blockers

None.
