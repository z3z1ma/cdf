Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md

# Project format, lockfile, and secrets implementation evidence

## What was observed

`crates/cdf-project` now exposes typed Layer 4 models and APIs for:

- `cdf.toml` parsing with project metadata, default environment, normalizer, environments, Python configuration, defaults, resource source mappings, freshness, and retention rules.
- Default-environment overlay inheritance for unresolved environment fields and retention entries.
- Declarative resource source resolution hooks that parse TOML/YAML through `cdf-declarative` and return compiled declarative resources.
- Secret references through `SecretRef`, env/file secret providers implementing `cdf_http::SecretProvider`, explicit unavailable handling for OS keychain providers, redacted resolved secret formatting through `cdf_http::SecretValue`, and validation that resolves secret references without storing or printing values.
- `cdf.lock` typed models, TOML round-tripping, semantic hash generation for resource capabilities and destination sheets, resource contract/schema snapshots, dependency tuple capture, normalizer capture, destination sheet/type mapping capture, and deterministic semantic diffing.

`Cargo.lock` was refreshed after adding `cdf-project` dependencies. Because the workspace is shared and concurrent package manifests are dirty, the lockfile now also reflects those current manifests.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/cdf` while the workspace was still loadable:

```text
cargo fmt -p cdf-project
cargo test -p cdf-project --no-fail-fast
cargo test -p cdf-project --locked --no-fail-fast
cargo clippy -p cdf-project --all-targets --locked -- -D warnings
```

The first locked test run passed 13 unit tests and 0 doc tests. The tests cover book-shaped project parsing, environment overlay inheritance, retention/Python/defaults parsing, declarative resource compilation through `cdf-declarative`, env/file secret providers, secret redaction, missing-secret validation errors without values, plaintext secret rejection where a secret URI is required, lockfile generation/TOML round-trip, and lockfile semantic diffing. The clippy run completed with `-D warnings`.

After concurrent workspace edits, Cargo could no longer load the workspace because `crates/cdf-dest-duckdb/Cargo.toml` declares a package and dependencies but the crate directory contains no target file. These commands now fail before selecting `cdf-project`:

```text
cargo test -p cdf-project --locked --no-fail-fast
cargo clippy -p cdf-project --all-targets --locked -- -D warnings
cargo fmt --manifest-path crates/cdf-project/Cargo.toml -- --check
cargo tree -p cdf-project --depth 2 --locked
cargo deny check advisories
```

The direct non-Cargo checks that do not require workspace metadata passed:

```text
rustfmt --check crates/cdf-project/src/lib.rs
git diff --check -- crates/cdf-project/Cargo.toml crates/cdf-project/src/lib.rs Cargo.lock
git diff --check
```

Dependency advisory scans on the current shared `Cargo.lock` were run because `cdf-project` added dependencies:

```text
cargo audit
osv-scanner scan --lockfile Cargo.lock
```

Both scans reported PyO3 0.28.3 vulnerabilities: `RUSTSEC-2026-0176` / `GHSA-36hh-v3qg-5jq4` and `RUSTSEC-2026-0177` / `GHSA-chgr-c6px-7xpp`, fixed in PyO3 0.29.0. The current dirty `crates/cdf-python/Cargo.toml` contains `pyo3 = { version = "0.28.3", features = ["auto-initialize"] }`; `crates/cdf-project/Cargo.toml` does not depend on PyO3.

## What this supports or challenges

This supports the project ticket acceptance criteria at the implementation/test level for typed project parsing, environment inheritance, secret-reference validation/redaction, semantic lockfile capture, lockfile diffing, and secret resolvability checks.

Parent integration revalidation later challenged the temporary blockers: after DuckDB restored a crate target and Python moved to PyO3 0.29 through `pyo3-arrow`, integrated package tests, clippy, formatting, advisory scans, OSV, pyright/compileall, and `git diff --check` all passed.

## Limits

OS keychain resolution is modeled as explicitly unavailable in this build rather than implemented, because adding a platform keychain dependency would expand supply-chain surface outside the feasible MVP env/file provider path. Env and file secret providers are implemented and tested.
