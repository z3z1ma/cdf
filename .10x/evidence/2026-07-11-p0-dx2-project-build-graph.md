Status: recorded
Created: 2026-07-11
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md, .10x/tickets/2026-07-11-p0-destination-extension-boundary.md

# cdf-project normal build graph is destination-neutral

## What was observed

`cdf-project`'s final normal `cdf-dest-postgres` dependency was used only by `#[cfg(test)]` destination fixture constructors. Moving it beside DuckDB and Parquet in dev-dependencies leaves the production crate with no `cdf-dest-*` dependency. SQL catalog discovery continues through `cdf-declarative` and its source-driver composition, not through a destination.

The new unit law parses the crate manifest and fails if a concrete destination re-enters `[dependencies]`. `cargo tree -p cdf-project -e normal --prefix none | rg '^cdf-dest-'` produced no output. The focused law test passed.

## Procedure

```text
cargo tree -p cdf-project -e normal --prefix none | rg '^cdf-dest-' || true
cargo test -p cdf-project project_normal_build_graph_has_no_concrete_destination_crates
```

## What this supports or challenges

This supports the destination-extension contract's dependency inversion and reduces rebuild fanout when one destination implementation changes.

## Limits

Dev-dependencies intentionally retain first-party destination fixtures for generic orchestration tests. This evidence does not by itself prove all CLI/doctor/replay branches are destination-neutral or close DX3/DX4.
