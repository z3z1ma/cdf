Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: None

# Ratify supply-chain policy gates

## Scope

Define and implement the repository policy needed for supply-chain quality gates that the security specification expects: `cargo-deny` policy, cargo-vet adoption decision, and how license/source/advisory checks are run locally and in CI. Owns only policy/configuration files required for these gates, such as `deny.toml`, `supply-chain/**`, and follow-on documentation or CI wiring when explicitly in scope.

## Acceptance criteria

- A ratified license/source/advisory policy exists for `cargo deny check`, including explicit handling of Apache-2.0, MIT, Unicode, BSD, LLVM-exception, and other licenses present in the current dependency graph.
- The project either initializes cargo-vet metadata with a concrete audit/exemption policy or records an active decision deferring cargo-vet adoption.
- The selected gates run without policy-configuration failures on the current workspace.
- The policy does not weaken vulnerability, source, or license checks merely to make tools pass.

## Evidence expectations

Record `cargo deny check`, `cargo audit`, and cargo-vet or cargo-vet-decision evidence after the policy is implemented.

## Explicit exclusions

No opportunistic dependency upgrades, no broad `cargo update`, no CI workflow changes unless the ticket is explicitly expanded or a child ticket is opened.

## Progress and notes

- 2026-07-06: Opened from kernel QUALITY verification. `cargo deny check advisories` passed with default config, but full `cargo deny check` failed because no `deny.toml` exists and the default config has no allowed-license list, rejecting even Apache-2.0/MIT project and dependency licenses. `cargo vet` reported that `supply-chain/` is not initialized.
- 2026-07-06: Checkpoint-store QUALITY verification added `rusqlite`, `serde_json`, and dev-only `tempfile` to the dependency graph. `cargo audit`, `cargo deny check advisories`, `osv-scanner`, Semgrep, CodeQL, and gitleaks remained clean. Full `cargo deny check` still fails only at the unratified license allowlist, and `cargo vet` still fails because `supply-chain/` is absent.
- 2026-07-06: Package/contract/HTTP QUALITY verification added `arrow-ipc`, `sha2`, `hex`, `unicode-normalization`, package dev-only `tempfile`, and transitive Arrow IPC/LZ4/hash dependencies. `cargo audit`, `cargo deny check advisories`, OSV, Semgrep, CodeQL, gitleaks, and source unsafe search remained clean. Full `cargo deny check` still fails at unratified license policy, and `cargo vet` still fails because `supply-chain/` is absent.
- 2026-07-06: CLI/dlt/crate-split QUALITY verification again passed `cargo audit`, `cargo deny check advisories`, OSV, Semgrep, CodeQL analysis, source-only gitleaks, and git-history gitleaks. Full `cargo deny check` still fails only at unratified license policy, and `cargo vet` still fails because `supply-chain/` is absent.
- 2026-07-06: Local system SQL QUALITY verification made `rusqlite` a direct `cdf-cli` dependency and added dev-only Arrow test dependencies that were already present elsewhere in the lockfile. `cargo audit`, `cargo deny check advisories`, OSV, Semgrep, reused CodeQL analysis, source-only gitleaks, and git-history gitleaks remained clean. Full `cargo deny check` still fails only at unratified license policy, and `cargo vet` still fails because `supply-chain/` is absent.
- 2026-07-06: Singer/Airbyte protocol adapter QUALITY verification made `serde_json`, `sha2`, and `hex` direct `cdf-subprocess` dependencies; all were already present in the lockfile through other crates. `cargo audit`, `cargo deny check advisories`, OSV, Semgrep, CodeQL SARIF, source snapshot gitleaks, git-history gitleaks, `cargo machete`, and `cargo +nightly udeps` remained clean. Full `cargo deny check` still fails only at unratified license policy, and `cargo vet` still fails because `supply-chain/` is absent.
- 2026-07-06: Parquet/object-store destination QUALITY verification added direct `duckdb`, `object_store`, `arrow-array`, `arrow-schema`, `tokio`, `serde`, `serde_json`, `sha2`, `hex`, and `tempfile` use in `cdf-dest-parquet`. The final implementation deliberately avoids arrow-rs `parquet` because that path pulls in `paste` and triggers `RUSTSEC-2024-0436`; `Cargo.lock` no longer contains `parquet` or `paste`. `cargo audit`, `cargo deny check advisories`, targeted Semgrep, reused CodeQL analysis, and source-only gitleaks remained clean. Full `cargo deny check` still fails only at unratified license policy, and `cargo vet` still fails because `supply-chain/` is absent.
- 2026-07-06: Parent activated this ticket after the user made `QUALITY.md` tool execution mandatory and active records confirmed Apache-2.0 governance, strict advisory/source checks, and a permitted cargo-vet adopt-or-defer outcome. Implementation must preserve vulnerability/source checks and keep policy edits tightly scoped.
- 2026-07-06: Current failure-mode inspection before policy edits: `cargo deny check` exited 4 with `advisories ok, bans ok, licenses FAILED, sources ok`; failure was the missing explicit license allowlist. `cargo audit` exited 0 after scanning 402 locked crate dependencies. `cargo vet` exited 255 because `supply-chain/` is absent.
- 2026-07-06: Added `deny.toml` with no advisory ignores, crates.io as the only allowed registry, no allowed Git sources, unknown registries/Git denied, workspace license checking retained, and a permissive SPDX allowlist matching the current locked graph: Apache-2.0, MIT, Unicode-3.0, BSD-2-Clause, BSD-3-Clause, Apache-2.0 WITH LLVM-exception, ISC, 0BSD, Zlib, CC0-1.0, and CDLA-Permissive-2.0. Did not allow LGPL-2.1-or-later, BSL-1.0, or Unlicense globally; the current expressions that mention them are satisfied through Apache-2.0 or MIT alternatives.
- 2026-07-06: Parent review rejected the initial cargo-vet deferral because the user made `QUALITY.md` tooling mandatory and a `/tmp` probe proved `cargo vet init --locked` produces an explicit current-version exemption backlog and a passing `cargo vet --locked` check. Deleted the temporary deferral decision before commit.
- 2026-07-06: Ran `cargo vet init --locked`, creating `supply-chain/config.toml`, `supply-chain/audits.toml`, and `supply-chain/imports.lock`. The initial policy uses cargo-vet's default `safe-to-deploy` criteria for current third-party version exemptions. This is not an audit claim; it is a review backlog that makes future dependency graph changes visible to `cargo vet`.
- 2026-07-06: Post-policy focused checks: `cargo deny check` exited 0 with `advisories ok, bans ok, licenses ok, sources ok`; it still reports non-failing duplicate-version warnings for the current Arrow/DataFusion and platform dependency graph. `cargo audit` exited 0 after scanning 402 locked crate dependencies. `cargo vet --locked` exited 0 with current dependencies vetted through exemptions.
- 2026-07-06: Closure evidence recorded in `.10x/evidence/2026-07-06-supply-chain-policy-gates.md`; review recorded in `.10x/reviews/2026-07-06-supply-chain-policy-gates-review.md`.

## Blockers

None.
