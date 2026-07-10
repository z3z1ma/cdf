Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a10a-discovery-manifest-artifact-budget.md, .10x/tickets/done/2026-07-09-p2-ws-a10b-aggregate-schema-join-core.md, .10x/tickets/done/2026-07-10-p2-rp1-residual-envelope-codec.md

# P2 discovery-manifest, aggregate-join, and residual-codec integration evidence

## What was observed

The first multi-file discovery and residual-promotion foundation tranche is integrated without changing the existing runtime authority model. CDF now has a validated, canonical, content-addressed discovery-manifest artifact; a pure format-neutral aggregate Arrow schema join that uses source-name identity and the existing reconciliation widening lattice; and an exact `residual-json-v1` Arrow value codec used by the existing variant-capture boundary.

Legacy schema snapshot v1 bytes and public Rust struct shapes remain compatible. Manifest-linked snapshots use version 2 identity and fail hydration on missing, mismatched, cross-resource, or tampered sidecars. Sidecar publication is atomic and no-clobber: concurrent identical writers converge, conflicting writers preserve one complete winner, and unsupported publication semantics fail closed.

## Procedure

- `cargo nextest run -p cdf-kernel -p cdf-contract -p cdf-engine -p cdf-project --locked`: passed, 224/224.
- `cargo nextest run --workspace --locked`: passed, 831/831, including four slow determinism/live-run cases.
- `cargo check -p cdf-kernel -p cdf-contract -p cdf-engine -p cdf-project --all-targets --locked`: passed.
- `cargo clippy -p cdf-kernel -p cdf-contract -p cdf-engine -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo semver-checks check-release --baseline-rev HEAD` for `cdf-kernel`, `cdf-project`, and `cdf-contract`: passed, 196/196 each.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
- `cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo deny check`: passed; existing dual-Arrow duplicate warnings remain allowed by repository policy.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed with the existing documented exception.
- `cargo vet --locked`: passed, 455 exemptions.
- `cargo machete --with-metadata --skip-target-dir crates/cdf-contract crates/cdf-kernel crates/cdf-engine crates/cdf-project`: no unused dependencies.
- `rust-code-analysis-cli` over the three new implementation modules: completed successfully.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

Focused regressions prove budget validation; canonical manifest round trips; sampled/exhaustive and probed/unprobed honesty; unsafe-path, tamper, hash, and concurrent-publication rejection; aggregate widening, nested joins, missing-field null origin, metadata variance, source-name collision handling, and input permutation; plus exact residual round trips across scalar, decimal256, binary, temporal, interval, list/view, struct, map, null, non-finite float, canonical path, version, and unsupported-type cases.

## What this supports

This supports every scoped acceptance criterion for A10a, A10b, and RP1. It establishes executor-neutral artifacts and pure semantics needed by multi-file exhaustive and sampled discovery without claiming that enumeration, runtime effective-schema evidence, file quarantine, or promotion execution already exists.

## Limits

Candidate selection and actual multi-file probing remain owned by A10c/A10g. Per-file coercion execution, effective-schema package evidence, and file quarantine remain owned by A10d/A10e. Live residual verdict routing remains owned by RP2, and schema promotion remains owned by RP3-RP10. These are dependency boundaries, not evidence gaps in the three closed children.
