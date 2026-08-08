Status: active
Created: 2026-08-07
Updated: 2026-08-07

# In-process Python is admitted as the first batch transform hook substrate

## Context

`.10x/specs/batch-transform-hooks.md` defined a data-plane contract for plan-declared batch
transform hooks but could not be activated: VISION D-23 states Python is an "authoring and
interchange surface, never execution substrate — Python runs to *produce* batches; from the instant
a batch crosses the C Data Interface, everything downstream is Rust."

Source inspection corrected the framing this conflict was originally recorded with. `cdf-python`
already executes Python in-process through PyO3 with `auto-initialize`
(`crates/cdf-python/src/interpreter.rs`). D-23 is therefore not a prohibition on running Python; it
is a *directional* boundary rule placing Python strictly upstream of the C Data Interface. A
pre-contract batch transform hook sits downstream of source decode, which is what conflicts.

The alternative substrate is not available. `cdf-wasm` is a one-line placeholder, and the cancelled
WIT foundation found no ratified acyclic wire projection for recursive `ScopeKey`/`SourcePosition`
values.

The hooks draft recommended an isolated worker process over an in-process interpreter, but scoped
that recommendation explicitly to *untrusted* hooks: "In-process Python has a larger
crash/security/allocator blast radius and is not the recommended default for untrusted hooks."

## Decision

Supersede VISION D-23's downstream clause narrowly, for plan-declared batch transform hooks only,
with four ratified parameters:

1. **Trust model — first-party only.** Hooks are project-owned code carrying the same trust as the
   repository's `cdf/<namespace>/<resource>.cdf.sql` resources. Registry distribution, third-party
   modules, and any untrusted authoring tier are excluded from this decision.
2. **Substrate — in-process PyO3 on the D-25 Python pool.** Hooks execute in-process, reusing
   `cdf-python`'s existing PyCapsule zero-copy path and the dedicated Python pool D-25 defines,
   under D-25's rule of identical semantics on GIL and free-threaded builds.
3. **Environment identity — `uv.lock` plus pinned interpreter.** A fully resolved, hash-pinned
   `uv.lock` and an explicit CPython version hash together into the hook's environment identity.
   Unpinned `latest`, mutable virtualenvs, import-path lookup at runtime, and network-fetched code
   remain forbidden.
4. **Performance floor — measured, then ratified.** Hook pass-through is measured against native
   Arrow pass-through on the same host with dispersion bounds, following the established roofline
   discipline. The committed ratio is ratified from observed data and then becomes a hard closure
   gate.

D-23 otherwise stands: Python remains an authoring and interchange surface everywhere else, and the
kernel's dependency graph still contains no Python.

## What this changes in the hooks specification

The draft's determinism clause states that "time, randomness, network, ambient environment, and
undeclared filesystem access are denied." In-process execution cannot enforce denial. That clause is
narrowed to **declared and compile-time audited**: the hook declares purity, CDF audits imports and
declarations at compile time and rejects on mismatch, but the runtime does not confine a hook that
violates its declaration.

The draft's error taxonomy mapping "worker crash, timeout, OOM" to typed errors is narrowed
likewise: a hard in-process fault terminates the process rather than producing a typed error.

## Alternatives considered

**Isolated worker over `cdf-subprocess`.** Steelmanned, and it was the recommended option: the
~5.2k-line `cdf-subprocess` stack already exists with protocol, runner, and stream layers; it makes
denial genuinely enforceable rather than aspirational; and it isolates arbitrary Python allocator
behavior from the runtime's constant-memory invariants. Rejected because the first-party trust model
removes the threat it defends against, while its cost — an Arrow IPC round trip per batch — falls
directly on the data plane. Stays rejected unless the trust model widens to untrusted hooks, at which
point this decision must be revisited, not extended.

**WASM/Wasmtime.** Steelmanned as the correct long-term portable sandbox and already the D-26
direction. Rejected on readiness: `cdf-wasm` is a placeholder and the recursive-type WIT projection
is unresolved. Stays rejected until those two are closed; this decision does not prejudge D-26.

**Park hooks entirely.** Steelmanned: nothing else in the program depends on E1, and parking would
have closed the program without touching a foundational stance. Rejected by explicit user direction.

## Consequences

- E1 becomes executable once `.10x/specs/batch-transform-hooks.md` is revised to active with the
  narrowed determinism and error clauses above.
- **Accepted residual risk — declaration is not enforcement.** A first-party hook that performs
  network I/O, reads the clock, or uses randomness will run. Determinism becomes a convention backed
  by compile-time audit and golden/repeatability tests, not a runtime guarantee. Replay is protected
  structurally rather than by confinement: packages record post-hook batches, so replaying a
  finalized package never re-executes the hook.
- **Accepted residual risk — crash blast radius.** A segfault or OOM in hook code terminates the
  run. This is bounded by existing authority rather than new work: VISION §20.2's chaos layer kills
  the process at every lifecycle boundary on every merge, and the five-row crash matrix already
  decides this case — no receipt and no finalized package means the transition never happened and is
  re-planned. Hook failure advances no checkpoint and produces no partial receipt.
- **Accepted residual risk — non-preemptible hooks.** `cdf-python` already provides
  `ForeignCancellation`, budget timeouts, and a `Cancelled` terminal status, proven in the producer
  direction. A hook blocked inside a C extension that never yields to the interpreter still cannot be
  preempted; the timeout will not fire until control returns.
- `uv` enters the build and CI graph. Its lock-format stability and cross-platform resolution
  reproducibility are **unverified** and must be confirmed during E0/E1 execution before the
  environment hash is treated as authoritative.
- VISION.md D-23 does not yet carry a pointer to this narrowing. Updating it is a separate action
  requiring user authorization, since VISION.md is canonical product doctrine rather than a `.10x/`
  record.
- Revisit if: hooks are opened to untrusted or third-party authors; `cdf-wasm` and the WIT recursive
  projection become viable; or measured IPC cost proves lower than the in-process risk premium.

## References

- `VISION.md` D-23, D-25, D-26, §20.2
- `.10x/specs/batch-transform-hooks.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `crates/cdf-python/src/interpreter.rs`, `crates/cdf-python/src/driver.rs`
