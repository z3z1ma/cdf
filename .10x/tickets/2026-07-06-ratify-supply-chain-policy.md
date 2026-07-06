Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
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

## Blockers

- License/source policy and cargo-vet adoption are unratified.
