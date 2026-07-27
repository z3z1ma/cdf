Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Rust safety lint walls

CDF's Rust safety posture is a workspace contract, not a convention repeated by individual
authors.

## Workspace inheritance

The root `Cargo.toml` owns these defaults:

- Rust `unsafe_code = "deny"`;
- Clippy `undocumented_unsafe_blocks = "deny"`.

Every workspace package manifest MUST declare:

```toml
[lints]
workspace = true
```

Adding a workspace member without that declaration fails
`cdf-project::tests::workspace_safety_lint_policy_and_exception_set_are_closed`.

## Closed unsafe exception set

Production unsafe code is limited to the measured FFI owners named by
`.10x/decisions/compiler-enforced-rust-safety-walls.md`:

- `cdf-dest-duckdb`: segment scanning and worker-memory measurement;
- `cdf-python`: Arrow capsule import/export;
- `cdf-subprocess`: Unix child address-space limit installation.

`cdf-benchmarks::references` has one measurement-only exception. Each exception is local to the
smallest module or function, cites the governing decision, and places a `SAFETY:` rationale
immediately before every unsafe block. Every unsafe function also has a `# Safety` contract naming
its caller obligations and the governing record.

The architecture test parses Rust syntax and recursively inspects macro tokens. It closes the exact
allowance target, unsafe-function/contract set, explicit block count, macro unsafe-token count,
unsafe foreign modules, unsafe impls, and unsafe traits. A new owner therefore requires a deliberate
decision supersession plus an explicit gate update; an incidental `#[allow(unsafe_code)]` is not an
extension mechanism, including inside a file that already owns a different exception.

## Foundational panic policy

Production library code in these crates denies Clippy's `unwrap_used` and `expect_used` lints:

- `cdf-kernel`
- `cdf-memory`
- `cdf-runtime`
- `cdf-package`
- `cdf-package-contract`
- `cdf-engine`
- `cdf-task-store`
- `cdf-object-access`

The denial is `cfg_attr(not(test), ...)`, so test setup may keep concise assertions. Production
fallibility MUST be propagated as typed `CdfError` values when the caller can act on it.
Infallible trait and lifecycle surfaces need an explicit recovery or conservative fallback rather
than a hidden unchecked extraction. Do not add crate-wide allowances to restore an old panic.

Mutex poisoning is not automatically recoverable. `PoisonError::into_inner` can expose partially
mutated accounting, admission, or executor state and MUST NOT be used as a mechanical substitute
for `unwrap`. Propagate an internal error from fallible APIs. Where an infallible invariant surface
cannot return an error, explicitly fail-stop rather than reusing poisoned state, and add a focused
test when the invariant protects accounting or lifecycle authority.

## Verification

Run the closed-set architecture test with DuckDB's bundled feature so local machines do not
depend on a system `libduckdb`:

```text
cargo test -p cdf-project --features cdf-dest-duckdb/bundled-duckdb \
  tests::workspace_safety_lint_policy_and_exception_set_are_closed --locked -- --exact
```

The compiler backstop is:

```text
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

The all-features command is important: it compiles the complete FFI exception set and unifies the
bundled DuckDB feature used by release validation.
