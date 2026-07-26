Status: active
Created: 2026-07-25
Updated: 2026-07-25

# Runtime global allocator

## Context

CDF's 100 GiB / 2 GiB constant-memory law repeatedly OOM-killed even though the managed ledger
remained within its 1.5 GiB authority. A process-map capture showed 53 anonymous mappings near
64 MiB each across the long-lived CPU and source worker pools. Glibc's per-thread arenas retained
1.68 GiB after Arrow work moved between workers; a 20 GiB run peaked at 2.119 GiB RSS despite
having no live managed-memory overrun.

Constraining glibc with `MALLOC_ARENA_MAX=2` proved the diagnosis and completed the 100 GiB law,
but it is Linux-specific, requires user/environment tuning, and imposes a fixed allocator
contention policy unrelated to the admitted host. The shipped executable needs a portable default
that scales across its worker pools without changing library or artifact semantics.

## Decision

The `cdf` executable uses mimalloc `0.1.52` as its Rust global allocator. The declaration lives
only in `cdf-cli/src/main.rs`; library crates remain allocator-neutral. Mimalloc's `override`
feature is disabled, so the decision does not interpose on Python, DuckDB, libc, or other native
libraries' own allocation APIs.

The selected tuple is `mimalloc 0.1.52` plus `libmimalloc-sys 0.1.49`, recorded in the lockfile and
cargo-vet policy. Dependency-pin changes require the normal D-28 migration loop and must repeat the
constant-memory and throughput comparison.

## Alternatives considered

- Keep the system allocator and require `MALLOC_ARENA_MAX=2`: rejected. It completed 100 GiB at
  1.701 GiB peak RSS, but is glibc-only, operator-visible, and hard-codes a contention cap.
- Increase native headroom or reduce runtime concurrency: rejected as the primary repair. It would
  trade throughput for memory while leaving multiplicative arena retention intact.
- Call glibc `mallopt` at startup: rejected. It introduces a Linux/glibc-specific unsafe boundary,
  cannot govern allocations that precede the call, and encodes the same fixed arena cap.
- Jemalloc: credible, but not selected because mimalloc already falsified the retention failure
  across the exact workload without a measured throughput regression or executable-policy
  complexity. Reconsider only if a future workload demonstrates a material mimalloc deficit.

## Consequences

On the same EC2 host and identical 20 GiB product workload, elapsed time changed from 48.939s with
glibc to 48.659s with mimalloc while peak RSS fell from 2.119 GiB to 1.671 GiB. The untuned
mimalloc binary then completed the full 100 GiB law in 263.493s at 1.658 GiB peak RSS, with
530,841,600 rows, 500 segments, and verified package, receipt, and checkpoint semantics. The
glibc arena-control comparison completed in 261.432s, a 0.8% difference within ordinary run
variance.

Release builds gain one small native C dependency and must exercise the existing multi-platform
artifact matrix. Supply-chain review remains explicit through cargo-deny and cargo-vet.
