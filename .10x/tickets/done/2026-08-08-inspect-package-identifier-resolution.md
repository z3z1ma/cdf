Status: done
Created: 2026-08-08
Updated: 2026-08-08

# Resolve package identifiers in inspect package

## Scope

Make `cdf inspect package <identifier>` resolve the durable package identifier advertised by the
CLI argument contract, while retaining explicit package-directory paths.

## Non-goals

- fuzzy or partial identifier matching;
- package mutation, repair, replay, or remote lookup;
- compatibility aliases.

## Acceptance Criteria

- An exact package id shown by `cdf run` and `cdf inspect run` resolves to the same package as its
  explicit directory path.
- Unknown and ambiguous identifiers fail with typed, actionable, secret-safe diagnostics.
- Path traversal and symlink protections remain unchanged.
- Human and JSON inspection results are identical for id and path inputs.

## References

- `.10x/specs/cli-interaction-excellence.md`
- `.10x/knowledge/cli-report-authority.md`

## Assumptions

- Record-backed: `cdf inspect package --help` says its operands are identifiers or paths. The live
  package id `cli-atlas_throughput.depreciation_items_portable` failed as a relative directory,
  while the exact `.cdf/packages/...` path verified successfully.

## Journal

- 2026-08-08: Opened from the final MongoDB Atlas package verification.
- 2026-08-08: Activated. Exact identifiers resolve only within the selected project's effective
  environment package root. Absolute and multi-component operands remain explicit paths. This
  makes identifier lookup traversal-free by construction and leaves the package reader's
  no-follow filesystem authority unchanged.
- 2026-08-08: Implemented exact selected-environment lookup, manifest identity verification, and
  an actionable unknown-id diagnostic. Explicit paths retain the existing capability-based,
  symlink-refusing package reader. Focused tests and strict affected-package Clippy pass.

## Blockers

None.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib inspect_package_ --locked` passed three
  focused tests. Identifier and absolute-path JSON results are byte-for-value identical; unknown
  ids name both the operand and selected `.cdf/packages` root.
- Identifier syntax is exactly one normal path component and lookup uses one selected environment
  root, so exact lookup cannot be ambiguous. No prefix/fuzzy enumeration exists.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-kernel -p cdf-engine -p cdf-project -p cdf-cli-core
  -p cdf-cli --all-features --all-targets --locked -- -D warnings`, formatter, and diff checks
  passed for the combined gap-repair slice.

## Review

Pass. Fresh inspection confirmed identifier lookup does not weaken explicit-path or descendant
symlink protections, and the report/JSON authority is unchanged after path resolution.

## Retrospective

The argument parser already promised identifiers, but the command discarded that distinction and
treated every operand as a path. Resolving a single-component operand only after loading the
selected environment fixes the contract without an index, fuzzy matching, or another authority.
