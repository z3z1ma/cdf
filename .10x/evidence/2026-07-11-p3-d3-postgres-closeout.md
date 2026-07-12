Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-d3-postgres-binary-copy.md

# PostgreSQL binary COPY closeout

## What was observed

The current release encoder produced 25,511,661 rows/s and 16,777,229 bytes versus the exact removed scalar CSV shape's 10,618,599 rows/s and 18,819,489 bytes: 2.40x throughput with a smaller wire representation.

A real local PostgreSQL cluster over TCP measured 1,598,429 binary rows/s versus 604,815 CSV rows/s (2.64x). Pure encoder capacity is 15.96x server-inclusive binary throughput, so the encoder is not the limiting stage; PostgreSQL/server transport dominates.

The full suite passed 30 non-ignored tests and strict all-target Clippy. Coverage includes binary NUMERIC vectors, null/endian/epoch framing, append/replace/merge, rollback after staged COPY, duplicate receipts, corrections, quarantine mirror, Decimal128 fidelity, catalog discovery, and source cursor execution.

## Procedure

```text
cargo test -p cdf-dest-postgres --locked
cargo clippy -p cdf-dest-postgres --all-targets --locked -- -D warnings
cargo test -p cdf-dest-postgres --release binary_copy_encoder_is_at_least_twice_csv --locked -- --ignored --nocapture
cargo test -p cdf-dest-postgres --release live_binary_copy_is_at_least_twice_csv --locked -- --ignored --nocapture
```

## What this supports or challenges

This supports the ≥2x target, exact binary type encoding, transactional lifecycle, and absence of a production text fallback. The server-inclusive ratio shows CDF's encoder headroom is substantial enough for remote operation to remain network/server-bound.

## Limits

The remote inference is based on local TCP/server saturation rather than a provisioned WAN PostgreSQL host. D5 owns the shared environment/host matrix; any environment where client CPU becomes limiting must falsify this evidence there.
