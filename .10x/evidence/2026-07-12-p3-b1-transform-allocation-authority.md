Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Byte-transform allocation and expansion authority

## What was observed

The neutral runtime transform API previously accepted only an accounted input stream and cancellation, yet required implementations to return accounted output. It supplied no memory coordinator, consumer identity, chunk bound, or plan-time expansion authority, making reserve-before-allocation impossible without hidden dependencies.

`ByteTransformRequest` now carries the transform-class `MemoryCoordinator` and `ConsumerKey`, preferred output chunk bytes, maximum expanded bytes, maximum expansion ratio, optional positive planned input size, and cancellation. `validate_for` joins request and driver descriptor authority and rejects values that exceed the driver working-set/expansion capabilities, non-transform allocation classes, zero values, and ratio arithmetic overflow.

## Procedure

- `cargo test -p cdf-runtime byte_transform_request_binds_output_allocation_and_expansion_authority --locked` — passed.
- `cargo clippy -p cdf-runtime --all-targets --locked -- -D warnings` — passed.
- `cargo check --workspace --all-targets --locked` — passed; unrelated existing test-only warnings remain outside this surface.
- `cargo fmt --all` and `git diff --check` — passed.

The focused law accepts a bounded transform request and rejects a decode-class consumer and an output chunk exceeding driver working-set authority.

## What this supports or challenges

This supports implementing decompression as an ordinary neutral, ledger-accounted stream with no format/source/runtime leakage.

## Limits

No transform implementation exists yet, so this does not prove decompression correctness, streaming memory, expansion rejection, checksum handling, or throughput. B1 remains open for those outcomes.
