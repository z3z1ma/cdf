Status: active
Created: 2026-07-11
Updated: 2026-07-14

# Remote and local I/O overlap

## Purpose and scope

This specification governs generation identity, streaming listings/reads, per-origin pools/controllers, range/readahead, spool/cache, local I/O evaluation, telemetry, and conformance.

## Identity and requests

Each byte source MUST expose immutable generation evidence and strength. Strong parallel range mode requires enforceable version/generation/ETag preconditions on every request. Response length/range/content-generation mismatch fails before chunk publication. Provider multipart ETags are version tokens, not claimed checksums.

When strong generation cannot be enforced, planner MUST choose sequential single-generation spool/read with end hash and change detection or reject the plan. Redirects, auth refresh, retries, and resumed streams must retain egress policy and generation authority.

## Listing and planning

Local/cloud listings MUST page/stream and account metadata. Glob/filter membership and canonical lexical/provider-neutral path ordering are deterministic. Duplicate paths/generations, pagination loops, changing continuation snapshots, and identity changes fail/replan.

HTTP has no generic listing. Only ratified explicit/template enumeration is allowed. Controllers cannot probe arbitrary URLs beyond the planned allowlist to discover candidates.

## Pools, ranges, and overlap

Connections and quota state are shared by origin/source authority. Controller settings have hard configured/capability ceilings and memory/network admission. Feedback uses bounded low-gain/hysteresis adjustment; oscillation/throttle storms reduce concurrency. Manual overrides remain ceilings/fixed modes and are reported.

Every in-flight request reserves response/reassembly/codec handoff bytes before network read. Streaming bodies publish accounted chunks incrementally. Range coalescing/readahead requires codec capability and records physical versus logical bytes. Cancellation closes responses and releases permits promptly.

Retry classification follows the typed error taxonomy and smallest safe range/page unit under a run/source budget. Backoff honors provider headers through injected timers. Partial responses are discarded/retried as exact units and never duplicated downstream.

## Spool and cache

Spool files have typed run/source/generation identity, owner-only permissions, disk budget, hash/count, cleanup authority, and no package identity until decoded into ordinary package files. Streaming decode may tail a growing spool only when the codec/framing and failure contract prove no unverified partial fatal window escapes.

Reusable cache promotion requires complete cryptographic hash and atomic install. Lookup revalidates source generation unless the planned source itself is content-addressed by that hash. Cache size/retention/location are explicit; cache miss/eviction cannot change semantics.

## Boundedness and disk strategy

A full or growing spool is legal only for a finite, known-length object whose complete planned length is reserved against the shared disk ledger before payload transfer begins. The growing reader may expose only generation-bound tail metadata or byte ranges already published to the local file. Weak generations MUST finish and reattest the complete sequential spool before seekable decode. Budget refusal is a clean plan/open error and MUST occur before unaccounted growth.

An object larger than the admitted disk envelope MUST NOT require an equally large local copy. The plan MUST instead use generation-bound selective/ranged units, or a progressively evicting spool whose codec session publishes a monotone no-lookback byte frontier proving that evicted regions can never be requested again. Range and eviction policy remain neutral transport/runtime capabilities joined with codec unit evidence; generic orchestration MUST NOT branch on a concrete format. If neither strategy is sound, CDF MUST fail cleanly against the configured disk budget.

Unbounded row/event sources MUST decode directly through bounded batches and backpressure; they MUST NOT enter the finite-object spool path. A non-pausable unbounded source that requires replay MAY use only a bounded rolling spool with explicit byte/time capacity, checkpoint-bound retention authority, and atomic eviction below the committed replay frontier. When the bound is reached, CDF MUST pause/backpressure when the source permits it or fail cleanly before exceeding the bound; silent loss and indefinite disk growth are forbidden.

## Local I/O

Local sequential/range reads use accounted buffers and effective filesystem/device evidence. Readahead/pread/mmap/direct-I/O options are benchmark-selected and cannot be assumed portable. Direct/mmap unsafe paths require separate decisions. Local file identity is reattested before/after relevant reads when mutation is possible.

## Conformance and performance

Recorded transports MUST emulate paging, HTTP/1.1 and HTTP/2 multiplex behavior, range ignored/short/overlong, ETag/version changes, throttling/retry headers, redirects/egress rejection, disconnect/resume, slow streams, high BDP, millions of entries, cancellation, and cache/spool failures. Nightly live tiers cover public HTTPS and secret-backed S3/GCS/Azure without storing secrets.

Conformance MUST additionally cover finite growing-spool overlap, weak-generation full-spool fallback, over-budget large-object range/eviction selection, monotone eviction frontiers, and bounded rolling-spool checkpoint/eviction/recovery for non-pausable streams.

The lab records network/device rooflines, request count, concurrency, RTT, logical/physical bytes, prefetch waste, decoder starvation, queue/memory, retries/throttles, spool/cache, CPU, and overlap. TLC HTTPS ingest must be I/O-bound and meet the P3 1.5x composite target where host/network permit.

## Explicit exclusions

This spec does not create arbitrary HTTP LIST, weaken egress/secret policy, guarantee remote server performance, authorize unsafe/direct I/O, or make cache a correctness dependency.
