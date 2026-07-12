Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-xz
Verdict: pass

# Native xz driver review

## Assumptions tested

- The decoder recognizes only `.xz`, not ambient auto/LZMA formats.
- Concatenated streams and final integrity checks require `Finish` at EOF.
- The liblzma memlimit equals the admitted native reservation.
- The dependency cannot silently select an unpinned system library.
- Corruption, unsupported checks, truncation, memory limit, stalls, expansion, and cancellation fail closed.

## Findings

No critical or significant leaf-driver defect remains. The implementation selects `new_stream_decoder` with `CONCATENATED`, preserves checks, sets a 64 MiB memlimit, and uses exact total-in/total-out deltas. Tests cover one-byte boundaries, concatenation, integrity corruption, truncation, expansion, and cancellation. Static pinning removed the host-dependent `pkg-config` path found during review. Release throughput is 0.984x the synchronous wrapper.

The dependency includes bundled C and a compiler build script. The script is source-local/no-network and the package is exact-pinned in Cargo.lock, but it remains a native attack/build boundary represented honestly by the supply-chain exemption.

## Verdict

Pass for the leaf implementation and static tuple.

## Residual risk

Coverage-guided malformed-stream fuzzing and RSS/cgroup observation remain B1/F1 obligations. Accepted publication must wait for terminal integrity through the shared barrier.

