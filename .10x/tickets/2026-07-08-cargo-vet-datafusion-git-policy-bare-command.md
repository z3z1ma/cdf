Status: open
Created: 2026-07-08
Updated: 2026-07-08
Depends-On: .10x/decisions/datafusion-git-pin-arrow59-tuple.md, .10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md

# Repair cargo vet bare DataFusion git policy failure

## Scope

Diagnose and repair the mismatch where `cargo vet --locked` succeeds, but bare `cargo vet` exits nonzero for the ratified DataFusion git pin because cargo-vet reports that non-crates.io-fetched DataFusion packages match published crates.io versions.

The repair must preserve the active DataFusion policy:

- keep DataFusion mandatory in the design;
- keep the temporary Apache DataFusion git rev pinned until a crates.io Arrow 59 tuple is available;
- keep the git pin publication blocker;
- keep unknown git sources denied;
- avoid dependency unlocks or DataFusion version changes unless a later active decision supersedes the pin.

## Acceptance Criteria

- `cargo vet` and `cargo vet --locked` both pass, or an active decision/knowledge record explicitly narrows the project gate to `cargo vet --locked` with rationale.
- Any `policy.*.audit-as-crates-io` or equivalent cargo-vet posture change is scoped to the ratified Apache DataFusion git pin.
- The repair records why it does not weaken `.10x/decisions/datafusion-git-pin-arrow59-tuple.md` or `.10x/knowledge/datafusion-cratesio-arrow59-tripwire.md`.
- Supply-chain evidence includes `cargo vet`, `cargo vet --locked`, `cargo deny check`, and `cargo audit`.

## Exclusions

No B2 runtime replay/recovery changes. No DataFusion migration off the ratified git rev. No crates.io publication unblock while the git pin remains.

## Progress and Notes

- 2026-07-08: Opened during B2 worker verification. Observed `cargo vet --locked` pass with `Vetting Succeeded (393 exempted)`, while bare `cargo vet` failed with `policy.audit-as-crates-io` guidance for DataFusion `54.0.0` packages fetched from git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`. Existing Workstream D records own the git pin and prior locked vet pass, but do not own this bare-command failure mode.

## Blockers

None known. Investigate cargo-vet's intended policy shape before editing `supply-chain/config.toml`.

## References

- `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`
- `.10x/knowledge/datafusion-cratesio-arrow59-tripwire.md`
- `.10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md`
