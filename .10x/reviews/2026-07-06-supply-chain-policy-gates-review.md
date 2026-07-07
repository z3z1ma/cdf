Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md
Verdict: pass

# Supply-chain policy gate review

## Target

Review of the initial CDF supply-chain policy implementation: `deny.toml`, `supply-chain/audits.toml`, `supply-chain/config.toml`, `supply-chain/imports.lock`, and the active ticket closure state.

## Assumptions tested

- `QUALITY.md` tooling should run as real local gates where feasible, not remain indefinitely documented as missing configuration.
- The policy must not ignore advisories merely to make checks pass.
- The policy must not allow unknown registries or Git sources.
- License policy should allow the current locked graph's permissive expressions without globally allowing licenses that only appear as unused alternatives.
- Cargo-vet initialization must be represented honestly as exemptions/backlog rather than as completed dependency audits.

## Findings

No unresolved findings.

Parent review changed the worker's first cargo-vet approach. The worker initially recorded a deferral decision leaving `cargo vet` uninitialized. A `/tmp` probe showed `cargo vet init --locked` produced explicit metadata and a passing `cargo vet --locked` check, so the deferral was removed before commit and replaced with real `supply-chain/` metadata.

## Verdict

Pass. The policy makes `cargo deny check`, `cargo audit`, and `cargo vet --locked` pass without advisory ignores, dependency updates, CI churn, product source edits, or unknown-source allowances.

## Residual risk

Cargo-vet currently passes through 385 current-version exemptions, not through first-party audits or peer imports. That is an explicit cargo-vet backlog in `supply-chain/config.toml`; it does not block this initial policy ticket.
