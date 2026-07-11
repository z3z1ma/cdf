Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Generation-bound overlapped I/O

## Context

CDF needs to saturate remote networks and NVMe while preserving exact source identity. Current transports collect listings/bodies, block behind private runtimes/mutexes, and lack a shared controller. Blind parallel ranges can combine bytes from different object generations.

## Decision

Transport drivers implement the neutral `ByteSource` contract through the injected execution host. They own protocol/auth/pooling but not independent runtimes, memory pools, retry schedulers, or format decoding. Local, HTTP(S), S3, GCS, and Azure expose the same immutable content-generation and sequential/range/list capabilities.

Every planned remote object has an `ObjectGeneration` composed from the strongest available version id/generation, strong ETag, checksum, length, and modification evidence. Every range/stream request uses protocol preconditions (`If-Match`, version/generation selectors, or provider equivalent) where available and reattests returned metadata. Parallel/ranged decoding is allowed only when one generation is enforceable. Weak/no-validator HTTP objects use a single sequential verified spool/read or fail under strict immutable policy; CDF never joins unbound ranges.

Listings are paged/streamed into bounded canonical plan/draft sinks. Continuation tokens and page retries are runtime evidence; logical sorted object membership/identity is plan authority. Planning handles millions of objects without a resident full listing while preserving deterministic glob/filter/order.

Per-origin I/O controllers share connection pools, quota/rate state, and admission. They resolve range size, readahead, and concurrent requests within explicit min/max using low-gain feedback from RTT, achieved throughput, decoder starvation, queue pressure, retries/throttling, and memory permits. Tuning never changes selected objects, requested logical byte ranges, row order, or package identity.

Ranges are coalesced only when the codec declares extra bytes harmless and coalescing stays within memory/egress bounds. Returned chunks retain exact requested/physical offset/bytes/generation. Prefetch is cancellable and accounted; discarded bytes are measured. Retries request the same generation/range and cannot publish partial/duplicate chunks.

Download, byte transforms, decode, validation, package persistence, and destination staging overlap through the common graph. A bounded spool is chosen for nonseekable/weak-generation/random-access combinations, reserves disk before download, hashes while writing, begins sequential decode before completion when framing permits, and atomically promotes reusable cache content only under cryptographic identity. Cache is an optimization, never source identity authority without revalidation.

Local I/O uses the same chunk/range interface. Buffered sequential, pread/readahead, mmap, filesystem advice, and direct I/O are selected only by same-host measurement; unsafe/platform-specific mechanisms require their gates. Page-cache warm/cold modes remain labeled.

## Alternatives considered

- Fixed high request concurrency: rejected because BDP/endpoints/memory vary and throttling can reduce throughput.
- Let each codec issue HTTP/object-store calls: rejected because identity, egress, retries, pooling, and budgets would proliferate.
- Trust URL immutability without validators: rejected as a silent mixed-generation risk.
- Download every remote file first: rejected because it prevents network/decode overlap and doubles latency/disk for streamable formats.
- Global unbounded disk cache: rejected because retention/security/identity become hidden policy.
- Add direct I/O/fadvise now: rejected pending lab evidence.

## Consequences

Transport facades become async streaming providers; current synchronous Vec APIs are compatibility-only. Source/format schedulers consume capabilities. Lab/run evidence records origin class (redacted), protocol, generation strength, requests/ranges/logical/physical bytes, RTT, concurrency, throughput, retries/throttles, prefetch waste, spool/cache, and overlap.
