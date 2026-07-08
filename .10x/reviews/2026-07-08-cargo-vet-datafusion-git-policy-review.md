Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-cargo-vet-datafusion-git-policy-bare-command.md
Verdict: pass

# Cargo Vet DataFusion Git Policy Review

## Target

Review of the supply-chain policy change in `supply-chain/config.toml` for the ratified Apache DataFusion git pin.

## Findings

No blocking findings.

The policy entries are appropriately scoped. The patch adds `audit-as-crates-io = true` only for exact DataFusion git vet-version keys at rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`, and parent verification proved the 28 policy entries exactly match the 28 DataFusion git packages in locked Cargo metadata.

The matching exemptions are also exact to the git vet versions. The obsolete plain `54.0.0` DataFusion exemptions were removed, so the file no longer carries both crates.io-version and git-version exemptions for the same current DataFusion packages.

The active DataFusion tuple policy is preserved. No manifest, lockfile, dependency source, dependency version, `deny.toml` source allowlist, or publication policy changed. Unknown git remains denied, Apache DataFusion remains the only allowed git source, and the crates.io Arrow 59 tripwire remains the migration mechanism.

The required supply-chain commands passed: bare `cargo vet`, locked `cargo vet --locked`, `cargo deny check`, and `cargo audit --json`.

## Residual risk

`cargo vet` still warns that unrelated unnecessary exemptions could be pruned. The temporary-copy prune check showed broader stale Arrow exemptions outside this ticket. That should not block this closure because both vet gates pass and pruning unrelated historical exemptions would widen the supply-chain cleanup beyond the DataFusion git-policy mismatch.

## Verdict

Pass. The repair closes the bare Cargo Vet failure without weakening the ratified DataFusion git-pin policy or broadening git-source trust.
