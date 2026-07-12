Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-source-files/src/transport.rs, crates/cdf-source-files/src/runtime.rs, crates/cdf-cli/src/http_transport.rs
Verdict: pass

# Concurrent transport and remote spool review

## Findings

- Critical, resolved: the original `Arc<Mutex<Box<dyn FileTransport>>>` serialized all metadata, ranges, listings, and full downloads across otherwise independent partitions. The mutex and mutable trait surface are deleted; concurrency is an explicit transport contract.
- Significant, resolved: moving every file open through another channel would tax local NVMe/Parquet paths. The outer injected scope is remote-only; local paths remain direct.
- Significant, resolved: raw resource/partition strings could exceed the execution-scope label bound. Scope identity is now a fixed 16-hex hash prefix with a static label.
- Significant, bounded and owned: blocking Reqwest still occupies an I/O worker during transfer, and a remote native codec currently has two bounded forwarding edges. `.10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md` owns native async HTTP/cloud byte sources; `.10x/tickets/2026-07-11-p3-g2-range-readahead-spool-controller.md` owns measured controller/channel consolidation.

## Verdict

Pass for the mutex-removal and remote-scope milestone. No source/destination behavior leaked into engine/project orchestration, no legacy shim remains, and local hot-path shape is preserved.

## Residual risk

The test proves simultaneous transport entry and real single-file HTTP Parquet correctness, not high-BDP speedup or multi-origin fairness. Those acceptance claims remain open in G1/G2 and C4.
