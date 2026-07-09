Status: active
Created: 2026-07-08
Updated: 2026-07-09

# Versioning, LTS, and release policy

## Purpose and scope

This specification governs CDF versioning, serialized artifact compatibility, dependency tuple cadence, changelog convention, CI/release phases, binary artifacts, generated CLI artifacts, and initial install-channel expectations.

It derives from `VISION.md` Chapter 22, `.10x/specs/conformance-governance-roadmap.md`, `.10x/knowledge/datafusion-cratesio-arrow59-tripwire.md`, `.10x/knowledge/quality-gate-execution.md`, and `.10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md`.

## Versioning policy

CDF crates and binaries use semver.

Before `1.0.0`, Rust API compatibility MAY break in minor releases. Serialized artifact compatibility MUST NOT break without an explicit artifact-version bump, migration path, and committed migration fixtures.

Starting at `1.0.0`, public Rust APIs, CLI machine-output contracts, package manifest schema, checkpoint schema, capability-sheet format, WIT world, and declarative JSON Schema MUST follow semver-compatible compatibility expectations.

Patch releases MUST NOT change load-bearing dependency tuple versions, serialized artifact schemas, machine-output field meaning, receipt semantics, checkpoint-gate semantics, or destination idempotency semantics.

## Serialized artifact policy

The following artifact families MUST be independently versioned:

- Package manifest schema.
- Checkpoint schema and state version.
- Capability-sheet format.
- WIT world.
- Declarative JSON Schema.
- Run-ledger event schema.

A release that changes any serialized artifact family MUST include:

- a migration description;
- committed before/after fixtures;
- verification that old fixtures either load under the migration path or fail closed with a documented compatibility error;
- coverage in the release notes and changelog;
- evidence that golden-package or conformance fixtures were regenerated only for ratified reasons.

## Dependency tuple policy

Each CDF minor release pins one load-bearing dependency tuple. The tuple includes, at minimum, Arrow, DataFusion, Parquet, object_store, duckdb-rs, PyO3, pyo3-arrow, and any destination driver whose wire or type behavior affects receipts or packages.

Patch releases MUST NOT move tuple pins. Security exceptions require a release decision record that names the risk, the exact package movement, compatibility evidence, and why a patch exception is safer than waiting for the next minor.

While Apache DataFusion is sourced from a git dependency, CDF MUST NOT publish crates.io releases. Release jobs MAY produce binary pre-releases and checksummed artifacts, but crate publication remains blocked until the DataFusion crates.io Arrow 59 tripwire fires or a later active decision supersedes the publication constraint.

## LTS policy

CDF has no LTS promise before `1.0.0`.

After `1.0.0`, an LTS line is a minor release line selected by an active release decision. An LTS line MUST name:

- supported artifact versions;
- supported binary targets;
- dependency tuple;
- minimum Rust toolchain;
- security-support window;
- migration-support window;
- supported install channels;
- required conformance and golden fixtures.

Without an active LTS decision, a release is a normal semver release with no long-term support claim.

## Changelog policy

`CHANGELOG.md` is the human release ledger. It MUST keep an `Unreleased` section and per-version dated sections.

Each section uses these headings when applicable:

- Added
- Changed
- Fixed
- Security
- Deprecated
- Removed
- Migration Notes

Machine-output compatibility changes, artifact-version changes, dependency tuple moves, release-channel changes, and security posture changes MUST be called out explicitly. Internal refactors without operator-facing consequence MAY be omitted unless they affect support or migration.

## CI phases

Fast CI runs on pull requests and pushes. It MUST cover formatting, linting, focused tests, dependency metadata sanity, source-only secret scanning, and the fastest supply-chain gates needed for the touched dependency or security vectors.

Slow CI runs on schedule and manual dispatch. It MUST cover the scheduled/release/integration Deep Loop from `QUALITY.md`, plus configured conformance, golden, chaos, property, fuzz, and benchmark smoke gates that are relevant to the release or integration change set.

Local and CI CodeQL Rust checks MUST use a reusable database policy equivalent to `.10x/knowledge/quality-gate-execution.md`; CI MAY cache the database by source, lockfile, CodeQL version, extractor version, and Rust toolchain fingerprint.

Generated quality reports and CodeQL databases MUST NOT be committed.

## Release artifacts

A release workflow MUST produce reproducible, checksummed `cdf` binaries for the mainstream targets that the workflow can build and test honestly.

Initial mainstream targets are:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-pc-windows-msvc`

Targets may be marked unavailable only with evidence naming the missing toolchain, runner, signing, or test constraint.

Release artifacts MUST include:

- binary archive per target;
- SHA-256 checksums;
- generated shell completions once WS2D provides the generator;
- generated man pages once WS2D provides the generator;
- license file;
- changelog excerpt for the release.

## Install channels

The first install channel is a shell installer that downloads a checksummed release artifact and installs the `cdf` binary into a user-selected prefix.

The installer MUST:

- avoid silent privilege escalation;
- verify checksums before installation;
- support dry-run or equivalent inspection before mutation;
- print the installed version;
- fail closed on unsupported operating system, architecture, missing checksum, or download failure.

Brew, cargo-install, package-manager, and signed-update channels remain follow-up channels unless an active child ticket scopes one of them.

## Generated CLI artifacts

Command reference documentation, shell completions, and man pages MUST be generated from the clap command definitions after WS2D lands. Hand-written command reference pages MUST NOT drift from parser truth.

Docs and release jobs MUST fail when generated references are stale.

## Acceptance scenarios

Given a release candidate with no artifact-schema changes, when the release workflow runs, then it produces checksummed binary artifacts, generated CLI artifacts where available, a changelog excerpt, and quality evidence without attempting crates.io publication while the DataFusion git pin remains.

Given a release candidate that changes a package manifest schema, when release validation runs, then it requires migration fixtures and conformance/golden evidence before the release can proceed.

Given a patch release candidate, when dependency tuple versions differ from the previous patch in the same minor line, then release validation fails unless an active security release decision explicitly authorizes the patch exception.

Given a CI run requiring CodeQL, when a reusable database fingerprint is still valid, then the workflow analyzes the existing database instead of recreating it.

Given a user runs the shell installer against a mismatched checksum, then installation fails before writing the target binary.

## Explicit exclusions

This spec does not authorize crates.io publication while the DataFusion git pin remains. It does not create a `1.0.0` release date, LTS support promise, signing key policy, brew tap, package-manager feed, or auto-update system.
