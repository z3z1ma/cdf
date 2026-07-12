Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-source-files/src/local_byte_source.rs, crates/cdf-memory/src/lib.rs, crates/cdf-runtime/src/format.rs
Verdict: pass

# Review: local byte source

## Assumptions tested

- Buffer allocation is admitted before the read and stays accounted through parser ownership.
- Sequential EOF and exact ranges cannot silently cross a local generation change.
- Parallel range calls do not share a seek cursor.
- Cancellation releases reservations and prevents publication.
- The neutral identity distinguishes strong from weak evidence.
- Test support does not invert the leaf build graph through the engine/DataFusion stack.

## Findings

No critical or significant finding remains. Review replaced `Arc<[u8]>` payload storage—which required a second allocation/copy from a read `Vec`—with owner-preserving `Bytes`. It also added cancellation authority to exact range reads and explicit generation strength; weak sources can no longer accidentally enter Parquet's joined-range path. Unix change-time/device/inode evidence was added so same-size rewrites are not judged solely by modification time.

## Verdict

Pass. The provider is transport-local, memory-authoritative, cancellation-safe, generation-aware, and independent of parser/orchestration types.

## Residual risk

Non-Unix local identity is intentionally weak until a platform-specific strong change token is implemented; the planner must spool or reject for random-access codecs there. Production composition and remote providers remain open under G1/FX1.
