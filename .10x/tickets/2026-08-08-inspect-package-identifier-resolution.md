Status: open
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

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
