Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-structural-debt-program.md
Depends-On: .10x/tickets/2026-07-07-duckdb-arrow58-transitive-residual.md, .10x/decisions/datafusion-git-pin-arrow59-tuple.md

# P0 Workstream D: Close dependency tuple residual

## Scope

Close or explicitly accept the remaining DuckDB Arrow 58 residual and make the time-boxed DataFusion git pin operationally impossible to forget.

Owns:

- execution or reparenting of `.10x/tickets/2026-07-07-duckdb-arrow58-transitive-residual.md`;
- dependency-tree and conversion-boundary audit records;
- DataFusion crates.io release tripwire knowledge or automation record;
- versioning/LTS spec update for the no-crates.io-release-while-git-pinned constraint;
- `cargo vet` / `deny.toml` posture updates for the git source if needed.

## Required outcome

- The DuckDB Arrow 58 residual is either remediated through a low-risk `duckdb-rs` version/feature path or temporarily accepted with a precise revisit trigger.
- If temporarily accepted, every Arrow data boundary into the DuckDB driver is audited for commit, replay, and receipt verification, proving no Arrow 58/59 structural mismatch crosses a CDF public Arrow API boundary.
- A concrete weekly tripwire checks for a crates.io DataFusion release on the Arrow 59 tuple and opens a migration ticket the day it exists.
- The publication constraint is recorded: CDF must not publish crates.io releases while the DataFusion git pin remains, unless a later decision supersedes that policy.
- Git-source supply-chain posture is explicit in `deny.toml` and cargo-vet records.

## Acceptance criteria

- Dependency-tree evidence includes `cargo tree --workspace --locked -i arrow-array@58.3.0` and the DataFusion git-source path.
- DuckDB conversion-boundary audit names every owned file/function where Arrow data enters or leaves DuckDB.
- Tripwire knowledge/automation record names cadence, command/source of truth, expected output, and ticket-opening trigger.
- Versioning/LTS spec or governing record records the git-pin publication constraint.
- Supply-chain gates run or their limits are recorded.

## Evidence expectations

Record dependency-tree output, registry/source inspection, conversion-boundary audit, supply-chain gate output, and adversarial review.

## Explicit exclusions

No permanent Arrow-major bridge in the engine hot path, no weakening DataFusion usage, no unratified advisory exception, no public release, and no package-format change.

## Progress and notes

- 2026-07-07: Opened from P0 stop-line. Current records already ratify the DataFusion git pin on Arrow 59 and open the DuckDB Arrow 58 residual investigation.

## Blockers

None for investigation and decision. Remediation implementation depends on the residual ticket's findings.
