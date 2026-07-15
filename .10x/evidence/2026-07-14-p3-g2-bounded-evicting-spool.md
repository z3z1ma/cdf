Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 bounded evicting spool

## Observation

Finite strong-generation sources no longer need disk proportional to object size when a registered codec proves canonical no-lookback byte frontiers. The file source can select one sequential transfer through a fixed-capacity ring spool, reclaim bytes only after canonical unit completion, and retain generation-bound exact ranges as the deadlock-free path for requests outside current residency. Weak generations remain excluded.

Production admission caps one active ring at the smaller of the per-file spool ceiling, currently available shared spill authority, and 512 MiB, with an 8 MiB minimum below which exact ranges remain the safer useful strategy. These are invocation-local tuning values outside package identity. The shared spill coordinator remains the aggregate authority across concurrent files and destinations.

## Procedure

- Added a neutral `EvictingSpoolByteSource` behind `ByteSource`; source orchestration selects by immutable size, generation strength, exact-range capability, and disk admission, never by format identity.
- Reserved the complete fixed ring capacity before transfer, pre-sized an owner-only temporary file, streamed and optionally hashed one generation, and backpressured the producer at `release_frontier + capacity`.
- Forwarded canonical codec frontiers through `ByteSource::release_before`; final codec EOF releases the generation length so transfer completion cannot deadlock on an unneeded tail.
- Protected ring overwrite versus concurrent local reads with one async read/write storage lock and revalidated residency after lock acquisition.
- Kept requests larger than or ahead of retained residency on the strong generation-bound exact-range source. Requests below a released frontier fail as an internal invariant violation.
- Ran `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --locked`: 48 passed.
- Ran `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-files --all-targets --no-deps --locked -- -D warnings`: passed.
- Ran `cargo fmt --all -- --check` and `git diff --check`: passed.

The deterministic 192-byte fixture uses only 64 bytes of spill, proves the sequential producer stops at the capacity frontier, resumes twice only after explicit codec releases, preserves all three logical windows across wrap/reuse, rejects decreasing and out-of-generation frontiers, and returns memory, disk accounting, and the temporary file to zero after owner release.

## What this supports or challenges

This supports the P3 G2 law that an oversized finite object can use progressively evicting local residency without memory or disk proportional to input size. It also validates the division of authority established by P3 B2/C2: codecs prove complete envelopes, canonical scheduling determines when a unit is truly finished, and the byte source alone owns physical retention.

## Limits

This is fixed-capacity policy and deterministic local conformance, not a high-BDP live benchmark. Adaptive per-origin feedback, retry/throttle telemetry, reusable cache promotion, rolling replay spools for unbounded inputs, and a multi-gigabyte live execution that actually selects the evicting path remain open in G2 or A8. A broader three-crate strict Clippy invocation also surfaced two pre-existing kernel test lints unrelated to this slice; the owning fast-check/build-graph work remains responsible for the repository-wide baseline.
