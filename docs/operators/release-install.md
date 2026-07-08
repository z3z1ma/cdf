# Release and Install

CDF does not currently have a published installer or crates.io release channel.
Build from the repository checkout for local operation.

## Local Build

```bash
cargo build -p cdf-cli --locked
target/debug/cdf version
```

Expected:

```text
cdf 0.1.0
```

For a release-shaped local binary:

```bash
cargo build -p cdf-cli --release --locked
target/release/cdf version
```

## Current Publication Boundary

Crate publication is blocked while Apache DataFusion is sourced from a pinned
git dependency. Binary pre-release artifacts may be added by release workflow
work, but crates.io publication remains disabled until the active release policy
or a later decision supersedes that constraint.

Open release/install work:

- [WS8 release engineering](../../.10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md)
- [WS8B release artifact workflow](../../.10x/tickets/2026-07-08-p1-product-ws8b-release-artifact-workflow.md)
- [WS8C changelog and installer channel](../../.10x/tickets/2026-07-08-p1-product-ws8c-changelog-installer-channel.md)

The governing release policy is
[`versioning-lts-release-policy.md`](../../.10x/specs/versioning-lts-release-policy.md).

## Generated Shell Artifacts

Generated completions and man pages are not present yet. WS2D owns the generator,
and WS6B/WS8 own docs/release freshness once that generator exists.
