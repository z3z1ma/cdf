Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a6-spillable-package-dedup.md, .10x/specs/spillable-package-dedup.md

# P3 A6 order-independent map equality

## What was observed

Arrow `RowConverter` deliberately treats map entry order as significant, which conflicts with ratified `cdf-dedup-key-v1` map semantics. Dedup key preparation now recursively canonicalizes every map-bearing selected field before row conversion. Valid map entries sort by exact typed encoded key; duplicate or null logical keys fail before the dedup index; the original `keys_sorted` schema assertion is preserved rather than treated as equality authority.

Canonicalization traverses list, large-list, fixed-size-list, list-view, large-list-view, struct, dictionary, and union containers. List/map slices normalize to referenced child ranges, and list views materialize only referenced logical children, preventing unreferenced backing values from causing false validation failures.

## Procedure

1. Compared reversed physical entry orders after canonicalization and observed identical row bytes.
2. Repeated the comparison for a map nested in a struct.
3. Verified duplicate logical map keys fail with the named data error.
4. Sliced away a row containing duplicate keys and verified canonicalization does not inspect its unreferenced entries.
5. Ran `cargo test -p cdf-contract dedup_key::tests -- --nocapture`: four passed.
6. Ran `cargo clippy -p cdf-contract --all-targets -- -D warnings`: passed.
7. Added dense-union and dictionary slice normalization: only selected union children and referenced dictionary values are recursively canonicalized. Fixtures prove invalid map data in unselected children/values cannot reject a selected row.
8. Ran the full `cdf-contract` suite: 76 passed, zero failed.
9. Refactored the production entry point through one `encode_typed_rows` authority and froze a scalar matrix against byte-for-byte pinned Arrow RowConverter output: booleans; every signed/unsigned integer width; float16/32/64; date, time, timestamp with timezone, duration; decimal128; UTF-8/binary small/large/view; and fixed-size binary. Identical rows match and the differing row does not.

## What this supports or challenges

This closes the known top-level/nested map order gap in the typed dedup key path while retaining Arrow RowConverter semantics for every non-map leaf.

## Limits

The 100 GB RSS stress remains before A6 closure. Sparse unions are normalized row-wise; their inactive child slots are expected Arrow null slots and remain covered by the nested type path rather than this focused map fixture.
