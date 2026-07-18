Status: done
Created: 2026-07-17
Updated: 2026-07-17
Parent: .10x/tickets/done/2026-07-12-p0-cargo-product-build-graph.md
Depends-On: None

# P0: remove invalid target linker config from the virtual Cargo manifest

## Scope

Restore Cargo workspace parsing after the LLVM-linker spike placed an Apple target configuration in the root virtual manifest. Keep the product build graph lean without committing a host-specific linker flag that is unavailable or invalid on this machine.

## Non-goals

- Ratifying a new linker policy or claiming a compile-speed improvement.
- Adding a compatibility shim, wrapper script, or host-specific build workaround.
- Changing any crate dependency edge or product runtime behavior.

## Acceptance criteria

- The root `Cargo.toml` contains no `[target.*]` table, because Cargo virtual manifests reject target sections.
- No repository Cargo config points at the unavailable `/opt/homebrew/opt/llvm/bin/ld64.lld` linker path or passes the rejected `-fuse-ld=/path` Apple clang argument.
- `cargo metadata --locked --no-deps --format-version 1` succeeds.
- A focused crate test can compile again under `CARGO_BUILD_JOBS=12`.

## References

- `.10x/tickets/done/2026-07-12-p0-cargo-product-build-graph.md`
- Commit `c957b8b7` (`spike(cargo): use llvm linker on mac`)

## Assumptions

- **Record-backed/source-backed:** Build graph acceleration must not make the default workspace unparseable or require a missing host tool.
- **User-ratified:** Fast checks should be lean, but performance-affecting or host-specific behavior should be measured and knobbed rather than hard-coded.

## Journal

- 2026-07-17: `cargo test -p cdf-dest-duckdb arrow_appender_can_fill_omitted_sequence_default --locked -j 12` failed before crate compilation because the root virtual manifest contained `[target.aarch64-apple-darwin]`.
- 2026-07-17: Removed the illegal target table from `Cargo.toml`. Cargo metadata then succeeded, proving the virtual manifest is parseable again.
- 2026-07-17: The migrated `.cargo/config.toml` copy of the same spike flag failed at link time: Apple clang rejected `-fuse-ld=/opt/homebrew/opt/llvm/bin/ld64.lld`, and the referenced `ld64.lld` binary is not installed. Deleted the untracked broken config rather than committing a host-specific nonfunctional optimization.
- 2026-07-17: Focused DuckDB crate compilation resumed under `CARGO_BUILD_JOBS=12`; a retained DuckDB Arrow bridge owner test passed after the exploratory sequence/default probe was removed from the worktree.

## Blockers

None.

## Evidence

- `cargo metadata --locked --no-deps --format-version 1 >/tmp/cdf-metadata.json` passed after the root target table was removed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb arrow_appender_preserves_decimal_and_nested_batches --locked -j 12 -- --nocapture` passed a retained DuckDB Arrow bridge owner test after the broken `.cargo/config.toml` was removed.

## Review

### Self-review (2026-07-17)

#### Findings

- No critical or significant finding. Cargo target configuration belongs in Cargo config, not a virtual manifest, and this repository should not commit a broken host-specific linker path.
- The repair does not claim a performance win. It restores the ability to run the measured performance work without adding a new hard-coded cap or hidden default.

#### Verdict

Pass.

#### Residual risk

Future linker acceleration may still be worthwhile, but it needs a separate measured ticket that detects the installed linker and proves compile-time improvement without breaking default Cargo semantics.

## Retrospective

The failure mode was an unmeasured speed spike landing as default build configuration. The durable lesson is the same as the P3 performance doctrine: optimization knobs need evidence and safe fallback; a hard-coded host assumption that prevents `cargo metadata` is not optimization.
