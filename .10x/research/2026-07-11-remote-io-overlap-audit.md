Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Remote I/O overlap audit

## Question

How should CDF saturate local/network I/O for immutable remote files without mixing object generations, materializing listings/responses, bypassing memory/rate/egress policy, or embedding transport-specific scheduling in codecs/sources?

## Sources and methods

Inspected file transport/local/HTTP/object-store listing/range/spool behavior, private runtime/mutex use, Parquet ranged discovery/runtime, P2 transport identity contracts, new byte-source/source/format/scheduler specs, and P3 remote/TLC envelope requirements.

## Findings

Object-store listing collects the entire prefix into `Vec<ObjectMeta>` and sorts in memory. Local directory listing also collects. HTTP/object range reads return owned `Vec<u8>`. HTTP response bodies are fully materialized by the transport. Remote non-Parquet formats spool through repeated synchronous range calls; the whole object is written before decode starts.

Object-store operations are hidden behind a private Tokio runtime and synchronous mutex. Store resolution/pooling/capabilities are not exposed to the global scheduler. HTTP uses HEAD/range but does not express strong generation preconditions as a common contract. A multi-range read can therefore only be safe if every request is bound to the planned ETag/version/checksum or the source is spooled/sequentially verified.

There is no per-origin connection/range controller joining RTT, bandwidth-delay product, server stream limits, source quota, memory, and CPU decode rate. Fixed parallelism would underfill high-BDP links or overload small endpoints. There is no bounded readahead/range coalescing policy or instrumentation for network wait versus decoder starvation.

Remote Parquet has a range reader but decode is collected/sequential. Other codecs cannot consume a streaming `ByteSource`. Download and decode do not overlap. Listing cardinality and HTTP template enumeration can also make plan memory scale with file count.

Local reads use ordinary file reads; no measured readahead/pread strategy exists. Adding platform-specific advice or direct I/O without the lab would be speculative and may hurt page-cache behavior.

## Conclusion

Implement async injected transport drivers behind `ByteSource`, with generation-bound requests, streaming listings/chunks, pooled per-origin clients, and accounted range/readahead admission. Add a measured per-origin controller that increases concurrency/read size until network/device saturation or downstream/memory pressure, within configured/capability ceilings.

Overlap listing/planning, download, transform, decode, and package stages where identity/order permit. Use bounded spool/cache only when seekability/generation safety requires it. Keep tuning outside package identity and record it in run/lab evidence.

## Limits

WS-L/G children must select HTTP client/object-store options, default controller gains, local readahead, and cache policy from measurements. Nightly live cloud evidence cannot replace deterministic recorded transport fixtures.
