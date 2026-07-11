Status: done
Created: 2026-07-11
Updated: 2026-07-11

# P3 materialization and streaming package-binding audit

## Question

Where does the current run path scale memory with input, and how can destination ingestion overlap package construction when package-token idempotency depends on a final hash that does not yet exist?

## Sources and methods

Traced engine extraction/contract/package execution, `PackageBuilder`, `PackageReader`, project run/replay, kernel destination requests/sessions, and DuckDB/Postgres/Parquet package/session paths. Compared source behavior with package identity, destination receipt, crash-matrix, P3 streaming-commit, and destination-extension records.

## Findings

The engine is partially streaming already: absent package-wide dedup, each accepted output batch is written immediately as its own durable IPC segment. The returned `segments` vector is metadata, not retained batches. This is useful substrate.

Materialization remains fatal at several boundaries:

- `PackageReader::read_commit_segments` reads every requested segment into a `Vec<CommitSegment>` before the runtime starts calling `session.write_segment`;
- DuckDB and Postgres convert Arrow batches to scalar row vectors, and package helpers accept whole segment/package collections;
- Parquet destination retains batch vectors per loaded segment and serializes a complete Parquet object into `Vec<u8>`;
- concrete planning helpers call `read_all_segments()` merely to recover schema despite the canonical runtime schema artifact;
- package-wide exact/keyed dedup stores all accepted batches until extraction ends;
- gzip/zstd and JSON paths retain complete decoded byte/batch collections;
- segment writing closes the file and rereads it for SHA-256.

The current run is extraction/package first and replay/destination second. No destination work overlaps source/decode/package persistence.

The deeper constraint is identity timing. `DestinationCommitRequest` requires `package_hash` and `idempotency_token`; active specs require the token to equal the finalized package hash. That hash is derived only after outcome-dependent segments, quarantine, stats, lineage, state-delta preimage, commit-plan preimage, and manifest identity are complete. Passing a provisional value to the current `begin` API would weaken idempotency/receipt verification or pollute deterministic package identity with a run-specific attempt id.

## Conclusion

Pre-finalization data movement must be explicitly modeled as staging, not commit. A destination may accept durable, hashed segments under a non-identity `LoadAttemptId`, but no final target visibility, receipt, package-token dedup claim, or checkpoint effect is allowed. After package finalization and verification, a seal/finalize request binds the staged segment identities to the real package hash/token and performs the destination's atomic publication/transaction before returning the ordinary receipt.

Destinations unable to provide invisible/rollback-safe staging declare finalized-package-only and use the existing post-finalization path. Capability joins, not destination names, select the mode. The attempt id belongs in the run ledger and destination staging metadata/receipt transaction metadata, never package identity. Crash recovery reattaches/cleans staging by attempt id or redrives the finalized package by package hash; it does not create a new commit-gate state.

Schema planning must use pinned/runtime schema artifacts and manifest metadata rather than reading data pages. Package/destination iteration must become pull-based one segment/batch at a time. Dedup requires its own ledger-accounted partitioned hash/sort/spill operator rather than exemption from the constant-memory law.

## Limits

This audit does not select the final Rust state-machine type names or implement asynchronous sessions. A1 must preserve the existing finalized-package path during migration and conformance must prove that staged acknowledgements cannot be mistaken for receipts.
