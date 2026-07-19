Status: done
Created: 2026-07-12
Updated: 2026-07-12

# E3 cross-platform safe package open

## Question

What safe, maintained, cross-platform Rust capability can let package verification traverse directories and open leaves relative to already-open handles, without following symlinks or allowing namespace races to escape the package, while preserving CDF's prohibition on CDF-authored unsafe code?

## Sources and methods

Inspected active and terminal 10x records before external research, especially `.10x/tickets/done/2026-07-11-p3-e3-streaming-verification-replay-io.md`, `.10x/specs/package-io-hashing-durability.md`, `.10x/specs/versioning-lts-release-policy.md`, `.10x/research/2026-07-11-package-io-durability-audit.md`, and the E3 parent. Inspected `crates/cdf-package/src/storage.rs`, `crates/cdf-package/Cargo.toml`, `Cargo.lock`, `deny.toml`, and `supply-chain/config.toml`. No build, dependency resolution, or mutation-producing Cargo command was run.

Authoritative upstream material inspected on 2026-07-12:

- `cap-std` 4.0.2 documentation and source: <https://docs.rs/crate/cap-std/4.0.2>, <https://docs.rs/cap-std/4.0.2/src/cap_std/fs/mod.rs.html>
- `cap-primitives` 4.0.2 manual resolver, Windows handle-relative open, and dependency manifest: <https://docs.rs/crate/cap-primitives/4.0.2/source/src/fs/manually/open.rs>, <https://docs.rs/crate/cap-primitives/4.0.2/source/src/windows/fs/create_file_at_w.rs>, <https://docs.rs/crate/cap-primitives/4.0.2/source/src/windows/fs/open_unchecked.rs>, <https://docs.rs/crate/cap-primitives/4.0.2/source/Cargo.toml.orig>
- `cap-fs-ext` 4.0.2 `OpenOptionsFollowExt` and crate metadata: <https://docs.rs/cap-fs-ext/4.0.2/cap_fs_ext/trait.OpenOptionsFollowExt.html>, <https://docs.rs/crate/cap-fs-ext/4.0.2>
- `fs_at` 0.2.1 API/caveats and `openat` 0.1.21/openat2 0.1.2 platform scope: <https://docs.rs/fs_at/0.2.1/fs_at/>, <https://docs.rs/openat/0.1.21/openat/>, <https://docs.rs/openat2/0.1.2/openat2/>
- Linux `openat2(2)` resolve contract: <https://man7.org/linux/man-pages/man2/openat2.2.html>
- Microsoft `CreateFileW`, naming/namespace, streams, case, and reparse-point contracts: <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew>, <https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file>, <https://learn.microsoft.com/en-us/sysinternals/downloads/streams>, <https://learn.microsoft.com/en-us/windows/wsl/case-sensitivity>, <https://learn.microsoft.com/en-us/windows/win32/fileio/reparse-points>

## Findings

### Existing authority and graph

- The active package-I/O spec requires verification to detect path escapes, symlinks, non-files, and unexpected/missing/tampered files. It does not grant an OS exception. The release spec names `x86_64-pc-windows-msvc` as a mainstream release target and permits target unavailability only with evidence of a toolchain, runner, signing, or test constraint. Making package verification unavailable on Windows would therefore be a product/support semantic change, not an implementation fallback already authorized by active records.
- `normalize_relative_path` currently rejects parent/root/prefix components and emits `/`-joined UTF-8, but it does not define a portable exact component grammar. In particular, it does not reject `:`, DOS device basenames, trailing dot/space, or case-fold collisions. Those spellings are not merely cosmetic on Windows: `file:stream` selects an alternate data stream; device basenames remain reserved; ordinary Windows lookup is case-insensitive; and trailing dot/space is normalized or inconsistently addressable depending on namespace/API.
- Locked/audited `rustix` 1.1.4 provides safe Unix `openat`/`O_NOFOLLOW`; its filesystem module is unavailable on Windows. Locked/audited `windows-sys` 0.61.2 is raw FFI. A CDF wrapper around `NtCreateFile`/`CreateFileW` would introduce CDF-authored unsafe and is excluded by E3. `nix` is Unix-only. No capability crate is locked today.

### `cap-std` / `cap-fs-ext`

- `cap-std` 4.0.2 supports Linux, macOS, FreeBSD, and Windows and presents directory capabilities rather than ambient descendant paths. `cap-primitives` resolves paths one component at a time while retaining parent directory handles. On Windows it uses `NtCreateFile` with `OBJECT_ATTRIBUTES.RootDirectory`; on Unix it uses `rustix` handle-relative operations. A rename or replacement of an already-open ancestor therefore does not redirect a later child open through a newly substituted pathname.
- The default `Dir::open` is not E3's symlink policy: it may manually follow symlinks that stay inside the capability. E3 must use `cap-fs-ext::OpenOptionsFollowExt` with `FollowSymlinks::No` and pass exactly one already-validated normal component per descent. Intermediate directories must be opened into new `Dir` capabilities with no-follow; the leaf must be opened read-only with no-follow and then classified from that same opened handle. Passing a multi-component path would allow the library's contained-symlink-following semantics and would not prove E3's stronger “reject every link” rule.
- The package-root capability must be established before reading `manifest.json`, and the manifest, discovery traversal, expected leaves, and later consumer reads must use that capability. Opening the manifest by ambient path first and only capability-opening data later leaves a root/manifest replacement window. A practical anchor is an ambiently opened parent plus a no-follow open of the final package-directory component; whether symlinks in ancestors above that parent are allowed is a package-location policy, not solved by leaf no-follow.
- This design closes the critical containment race: each successful open is bound to the directory handle used for lookup, and no-follow is enforced during that open. It does not create an atomic snapshot of the whole tree. Concurrent actors may add/remove entries between enumeration passes, mutate an already-open regular file while it is hashed, or replace names after hashing. Hashing the opened handle detects bytes actually read against the manifest, but no portable capability API proves that an entire mutable directory tree existed in one simultaneous state.
- Issuing a reusable `VerifiedPackage` and later reopening paths by name has a separate post-verification race. Capability-safe verification alone cannot prove that a later consumer reopened the same object. E3's eventual consumer-read hashing can close exact-byte authority at acknowledgement/final binding; until then, either opened capabilities/handles must flow into consumption or the reusable authority must not claim inode identity after handles are dropped.
- `cap-primitives` explicitly handles Windows DOS device names before manual resolution, uses root-relative native opens, and has Windows-specific trailing-dot/symlink handling. That does not replace CDF canonical spelling policy: ADS colons, case aliases/collisions, separators, ASCII controls, reserved names, and trailing dot/space should be rejected in the manifest grammar on every platform so one package identity cannot name different objects across supported systems.
- Dependency cost is material but bounded. `cap-std`, `cap-primitives`, and `cap-fs-ext` are absent from `Cargo.lock` and the cargo-vet graph. Their published manifests also introduce currently absent `ambient-authority`, `fs-set-times`, `io-extras`, `io-lifetimes` 3, `maybe-owned`, `rustix-linux-procfs` on Linux/Android, and `winx` on Windows; exact resolution may add or reuse more graph nodes. Existing `ipnet`, `rustix` 1, and `windows-sys` 0.61 are already locked/audited. Every new resolved deploy dependency requires cargo-vet audit/exemption and `cargo deny` license/advisory review. The Bytecode Alliance repository, current 4.0.2 release (2026-02-15), multi-platform source, and documented sandbox objective provide a stronger maintenance/security basis than a new CDF platform wrapper, but upstream unsafe FFI remains in the audited dependency trust base.

### Alternatives and null results

- `fs_at` 0.2.1 is the only smaller safe public wrapper found that covers both Unix `openat` and Windows `NtCreateFile`, includes handle-relative `read_dir`, and exposes `follow(false)`. It is not locked, is 75% documented, explicitly says features are added as needed, documents a Windows Procmon-dependent reparse-query caveat, and would resolve additional absent crates (`cvt`, `aligned`) plus an older `nix` line. It exposes lower-level platform differences to CDF and has a much smaller ownership/usage surface. It is a credible fallback for a focused third-party audit, not the first recommendation.
- The `openat` crate is safe and recently released but Unix-only, warns that absolute input ignores the directory descriptor, and requires single-component discipline. The `openat2` crate and Linux `RESOLVE_BENEATH`/`RESOLVE_NO_SYMLINKS` kernel primitive are Linux-only and have runtime kernel-availability concerns. Neither meets the active mainstream platform set.
- Raw `rustix` plus `windows-sys`, the `windows` crate, or direct `CreateFileW`/`NtCreateFile` wrappers cannot meet the “no CDF-authored unsafe” constraint on Windows. Moving the same unsafe into a CDF-owned “platform abstraction” changes location, not authority.
- `std::fs::canonicalize`, `symlink_metadata`, and ordinary `File::open` remain check-then-use and cannot bind the opened leaf to inspected ancestors. Rechecking metadata or canonical paths after open does not repair the escape window.
- Failing closed only on Windows can be made memory-safe and honest, but it conflicts with the current package-verification and mainstream-release contracts. It is not acceptable without explicit spec/support supersession. Failing closed on a specific OS/filesystem error after a capability dependency is selected is normal verification failure; compiling out verification for a mainstream target is a different semantic choice.
- No safe cross-platform standard-library API, already-locked crate, or openat2-style crate with Windows support was found.

## Recommendation

Prefer a ratified, pinned `cap-std` 4.0.2 plus `cap-fs-ext` 4.0.2 dependency tuple, subject to a complete cargo-vet/deny review of the newly resolved graph. The executable design should:

1. establish the package root as a capability before manifest I/O;
2. validate an exact portable manifest component grammar before any descendant open;
3. enumerate only through opened directory capabilities;
4. descend one component at a time with follow disabled and open/hash/classify the leaf through the same handle;
5. retain bounded state and canonical reporting as E3 already requires; and
6. bind later consumption to capability-opened objects or hash exact consumer-read bytes before acknowledgement/final binding rather than treating a dropped-handle verification as permanent pathname authority.

Do not choose `fs_at` merely to reduce dependency count; its narrower maturity and caveats increase the audit burden at the exact security boundary E3 is trying to outsource. Do not accept platform-wide Windows unavailability under current active records.

## Candidate options and ratification questions

### Dependency/platform authority

- **A — recommended:** authorize pinned `cap-std` 4.0.2 + `cap-fs-ext` 4.0.2 and the audited resolved transitive graph for component-wise capability traversal on every mainstream target.
- **B:** commission a separate focused audit of `fs_at` 0.2.1 before choosing it as the smaller low-level wrapper.
- **C:** keep the locked graph and make verification unavailable on Windows; this requires explicit package/release spec supersession and is not compatible with current authority.

Question: May E3 add and supply-chain-audit pinned `cap-std` 4.0.2 and `cap-fs-ext` 4.0.2, using one-component no-follow capability traversal on all mainstream targets? Decision unlocked: dependency/platform authority for a cold-start executor. I recommend A; confirm or correct before implementation.

### Portable path identity

Question: Must package manifest components use one portable exact grammar on every OS, rejecting backslash, colon/ADS, NUL and ASCII controls, case-insensitive DOS device basenames (including extensions), trailing dot/space, and case-fold-equivalent full-path duplicates? Decision unlocked: cross-platform alias and identity semantics. I recommend yes; it is the smallest fail-closed rule that makes one manifest name one portable object. Confirm or correct before implementation.

### Concurrent mutation guarantee

Question: Is the verification guarantee “every opened object remains beneath the anchored package capability and no symlink is followed under concurrent namespace replacement; hashes cover the handle bytes actually read,” while whole-tree atomic snapshot consistency and post-close pathname identity are not claimed? Decision unlocked: TOCTOU acceptance and later-consumer authority. I recommend yes, with consumer-read hashing required before acknowledgement/final binding; stronger atomic-tree semantics would require an explicitly owned snapshot/exclusive-lock protocol. Confirm or correct before implementation.

## Limits

- This was read-heavy shaping only. No candidate was added, resolved, built, tested, fuzzed, or audited locally, and no Windows host experiment was run.
- Published dependency manifests show the minimum likely graph impact, not the exact `Cargo.lock` delta Cargo would select in this workspace.
- Network filesystems, unusual third-party reparse filters, case-sensitive Windows directories, hard-link policy, package-root ancestor-link policy, and filesystem snapshot/lock mechanisms need focused conformance after semantics are ratified.
- The recommendation closes containment for opens; it does not by itself close E3's remaining bounded-memory, I/O admission, million-entry/RSS, 1 TB replay, consumer-fusion, or local replay-strategy evidence.
