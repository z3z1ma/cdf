Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-cargo-vet-datafusion-git-policy-bare-command.md

# Cargo Vet DataFusion git policy repair

## What was observed

Before this repair, `cargo vet --locked` passed but bare `cargo vet` failed. The bare command reported that the DataFusion packages fetched from Apache DataFusion git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf` matched published crates.io `54.0.0` versions and required `policy.*.audit-as-crates-io` entries.

The current locked graph has 28 DataFusion packages from:

```text
git+https://github.com/apache/datafusion.git?rev=7ff7278edc1bf7446303bff51e5883a38414bbdf#7ff7278edc1bf7446303bff51e5883a38414bbdf
```

## Procedure

- Inspected cargo-vet 0.10.2 local source and help. `audit-as-crates-io` is represented as `policy.<crate>.audit-as-crates-io`.
- Added 28 exact-version Cargo Vet policy entries in `supply-chain/config.toml` using the git vet-version key shape:

```text
[policy."<datafusion-crate>:54.0.0@git:7ff7278edc1bf7446303bff51e5883a38414bbdf"]
audit-as-crates-io = true
```

- Added 28 matching exact git-version exemptions for the same DataFusion workspace crates:

```text
[[exemptions.<datafusion-crate>]]
version = "54.0.0@git:7ff7278edc1bf7446303bff51e5883a38414bbdf"
criteria = "safe-to-deploy"
```

- Removed the now-obsolete plain `54.0.0` DataFusion exemptions. A temporary-copy `cargo vet prune` showed those plain DataFusion exemptions were unnecessary after the git-version exemptions existed; unrelated Arrow prune suggestions were left untouched.
- Verified policy coverage against the current locked Cargo metadata: 28 DataFusion git packages, 28 DataFusion policy entries, 0 missing, 0 extra.
- Verified the only DataFusion exemptions left are the 28 exact git-version exemptions.
- Confirmed manifests, `Cargo.lock`, dependency versions, `deny.toml`, and publication policy were unchanged.

## Command evidence

- `cargo vet > target/quality/reports/cargo-vet-datafusion-git-policy-bare.txt 2>&1`: passed with `Vetting Succeeded (452 exempted)`. It still warns that unrelated unnecessary exemptions could be pruned.
- `cargo vet --locked > target/quality/reports/cargo-vet-datafusion-git-policy-locked.txt 2>&1`: passed with `Vetting Succeeded (452 exempted)`.
- `cargo deny check > target/quality/reports/cargo-deny-datafusion-git-policy.txt 2>&1`: passed with existing duplicate-version warnings and final `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json > target/quality/reports/cargo-audit-datafusion-git-policy.json`: passed with 0 vulnerabilities and 1 warning for the already-ratified `paste 1.0.15` unmaintained advisory.
- `git diff --check`: passed during worker verification and parent verification.

## What this supports

- Bare `cargo vet` and locked `cargo vet --locked` now agree for the current DataFusion git pin.
- The Cargo Vet posture is scoped to the exact ratified Apache DataFusion rev and the 28 DataFusion workspace crates currently present in the locked graph.
- The repair does not weaken `.10x/decisions/datafusion-git-pin-arrow59-tuple.md` or `.10x/knowledge/datafusion-cratesio-arrow59-tripwire.md`: the git pin remains temporary, the crates.io migration tripwire remains active, and crates.io publication remains blocked while the git dependency remains.
- The broader source policy remains intact: `deny.toml` still denies unknown git sources and allows only `https://github.com/apache/datafusion.git`.

## Limits

This evidence does not audit DataFusion source itself beyond the current Cargo Vet exemption posture, does not migrate off the git pin, and does not remove unrelated stale exemptions that `cargo vet prune` may still identify. OSV/cargo-audit still surface only the already-ratified `paste` maintenance advisory.
