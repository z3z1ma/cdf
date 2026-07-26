# Changelog

All notable operator-facing changes to CDF are recorded here.

This project follows semver. Before `1.0.0`, Rust API compatibility may break in
minor releases, but serialized artifact compatibility changes must be called out
with migration notes under the active versioning policy.

## Unreleased

## [0.2.0-alpha.1] - 2026-07-25

### Added

- Added a checksum-verifying binary installer, generated shell completions,
  generated man pages, and mainstream macOS, Linux, and Windows artifacts.
- Added schema discovery and pinning, multi-file and remote source execution,
  typed coercion, automatic identifier normalization, and evidence-preserving
  quarantine behavior.
- Added a holistic terminal experience with stable human, verbose, quiet, and
  JSON rendering modes.

### Changed

- Release binaries now statically link the pinned DuckDB runtime. Developer and
  benchmark builds retain the prebuilt dynamic runtime for fast iteration, while
  published binaries require no system DuckDB installation.
- The execution path now uses bounded streaming, deterministic partition work,
  destination-neutral package provenance, and destination capability-based
  ingress rather than destination-specific orchestration.

### Fixed

- Fixed remote Parquet full scans, schema-admission reuse, staged-segment memory
  accounting, and wide-schema destination admission regressions found by live
  product smoke tests.

### Migration Notes

- This is a pre-1.0 development prerelease. No compatibility with the obsolete
  `v0.1.0` binary archive layout is provided; reinstall from this release so the
  required relocatable Python runtime is present beside the binary.

## [0.1.0] - 2026-07-08

### Added

- Initial CDF development version with local workspace builds and the first
  release-engineering policy records.
