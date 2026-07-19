Status: active
Created: 2026-07-19
Updated: 2026-07-19

# Capability-rooted package verification

## Context

Package verification must hash manifest-bound files without following symlinks or allowing a concurrent namespace replacement to escape the package root. `std::fs` check-then-open sequences cannot provide that authority. The locked graph has safe Unix handle-relative APIs but no safe Windows equivalent; using raw Windows FFI would introduce CDF-authored unsafe at an artifact trust boundary. Research in `.10x/research/2026-07-12-e3-cross-platform-safe-package-open.md` found that `cap-std` and `cap-fs-ext` provide the maintained cross-platform directory-capability and no-follow operations needed by E3.

The user granted autonomous ratification authority for the program and requires correctness without architectural leakage or performance regression. This decision applies to explicit verification and reopened-package consumption. Fresh package finalization remains authoritative from hash-while-write receipts and must not pay a redundant verification pass.

## Decision

CDF will pin `cap-std` 4.0.2 and `cap-fs-ext` 4.0.2, subject to the normal cargo-vet and cargo-deny supply-chain gates. `cdf-package` will establish a directory capability before reading a package manifest and will open descendants one validated component at a time with symlink following disabled. No CDF-authored unsafe code is authorized.

Manifest identity paths use one portable exact grammar on every operating system. Components reject backslash, colon/alternate-data-stream syntax, NUL and ASCII controls, case-insensitive DOS device basenames including names with extensions, trailing dot or space, non-normal components, and case-fold-equivalent full-path duplicates. The same spelling therefore names the same relative package object across supported platforms.

The concurrency guarantee is exact and limited: every opened object remains beneath the anchored package capability, no symlink is followed during component lookup, and hashes cover bytes read from the opened handle. Verification does not claim an atomic snapshot of a concurrently mutable whole tree. A later consumer cannot inherit permanent pathname/inode authority after handles are dropped; reopened-package execution must hash exact consumed segment bytes before acknowledgement/final binding or retain equivalent opened-handle authority.

Explicit verification remains bounded. Parallel verification may be introduced only through neutral admitted I/O/memory authority and retained only by same-host evidence; a fixed worker cap is not authority. The default implementation may remain sequential when that is the smallest correct bounded path.

## Alternatives considered

### `fs_at` 0.2.1

This smaller safe wrapper covers Unix and Windows, but it has a substantially narrower maintenance and usage surface, exposes more platform differences, and carries documented Windows caveats. Its lower dependency count does not compensate for higher audit risk at the package trust boundary.

### Raw `rustix` plus Windows FFI

This minimizes third-party abstraction but requires CDF-authored unsafe Windows filesystem code. Moving unsafe behind a local wrapper does not improve its authority and conflicts with the active unsafe gate.

### Check, canonicalize, then `std::fs::File::open`

This is dependency-free but remains vulnerable to replacement between inspection and open. Rechecking after open cannot prove that lookup remained beneath the inspected directory.

### Disable strict verification on Windows

This avoids new dependencies but makes a core package guarantee platform-dependent and conflicts with the mainstream target contract. It is not an acceptable preproduction simplification.

## Consequences

The package crate gains a focused audited dependency subtree and a capability-specific internal module. Generic runtime, sources, and destinations remain unaware of the implementation. Verification obtains a race-safe containment boundary on Unix and Windows without unsafe CDF code. Portable path identity becomes stricter; no backward compatibility is required because CDF has no production artifacts or consumers.

The dependency and additional component opens add compile and explicit-verification cost. Compile cost is isolated to `cdf-package`; runtime cost is measured in E3/E4. Ordinary fresh runs remain on hash-while-write finalization and do not acquire a redundant pass. If a later parallel verifier is measurably useful, it must consume admitted resource authority rather than a hard-coded cap.
