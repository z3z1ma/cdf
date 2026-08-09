Status: recorded
Created: 2026-08-08
Updated: 2026-08-08

# SQLite source native-query error-ownership audit

## Observation

The frozen `cdf-source-sqlite` Rust scope contains twelve files and 145 direct `CdfError`
constructor sites. The durable ledger classifies 79 Contract, 54 source Data, seven CDF-invariant
Internal, one run-cancellation Internal, two boundary-classified `new`, and two typed-preservation
test fixtures. Thus 143 constructors are production sites and two are regression fixtures. No
direct `CdfError` struct literal bypasses the constructors.

The direct-enum supplement finds three `ErrorKind::Internal` fallback arms in the rusqlite
classifier. They cover invalid use of CDF-generated statements/parameters and rusqlite states that
cannot arise after compiled-plan validation. The native-query `ErrorKind::Internal` match is only
safe message rendering after classification, not a new kind decision. Seven production `expect`
sites consume cursor/key options immediately after the same function validates the required
cursor shape; none crosses a source or host trust boundary.

The expanded audit found and repaired the native prepare boundary. SQLite syntax errors previously
fell through the coarse `Unknown` SQLite code to Data, and raw SQLite prepare messages could echo
authored query fragments. Syntax, incomplete input, multi-statement input, and authorizer denial
now become Contract with a fixed safe diagnostic. Missing or incompatible live query inputs remain
Data; transient, rate-limit, environment, and internal kinds retain their ownership and retry
metadata while receiving kind-specific text that never includes the query or SQLite message.
Focused regressions prove both a secret-like syntax literal and a private table name are absent.

The foreign-wrapper boundary still walks generic and nested `std::io::Error` source chains before
rusqlite fallbacks. Embedded typed errors retain kind, retry delay, and primary message with safe
operation context. Raw source-file absence/wrong shape is Data; permission/device/resource I/O is
Environment; busy/locked SQLite state is Transient; corruption/type/source-shape failures are Data.
Invalid database paths do not forward their path-bearing display.

## Procedure

The exact file scope and constructor inventory are reproducible without temporary files:

```sh
LC_ALL=C rg --files -g '*.rs' crates/cdf-source-sqlite | sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'CdfError::(new|transient|rate_limited|auth|contract|data|destination|environment|internal|from)' \
  crates/cdf-source-sqlite | sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'ErrorKind::Internal|CdfError \{' crates/cdf-source-sqlite | sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- '\.expect\(' crates/cdf-source-sqlite/src | sort
```

Frozen outputs:

- `.10x/evidence/.storage/2026-08-08-sqlite-source-error-files.txt` — twelve Rust files.
- `.10x/evidence/.storage/2026-08-08-sqlite-source-error-sites.tsv` — one header plus 145 classified
  constructor rows.

Focused validation:

```sh
DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-sqlite
DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-source-sqlite --all-targets -- -D warnings
cargo fmt --all --check
git diff --check
```

Result at audit time: 32 tests passed; strict all-target Clippy, formatting, and diff hygiene
passed. The suite covers typed direct/nested wrapper preservation with a 275 ms retry delay,
corrupt/busy/host-I/O ownership, invalid-path redaction, query syntax versus missing-input
ownership, authored-query redaction, dynamic drift, bounds, cancellation, and live execution.

## What it supports or challenges

This supports the SQLite source contracts' shared taxonomy and redaction requirements. It also
supports the claim that native-query validation and prepare failures do not expose authored SQL in
normal product diagnostics while keeping live source failures owned by Data.

## Limits

The inventory is exact for the frozen twelve-file crate and does not audit unrelated workspace
crates. The tests do not inject every platform-specific SQLite extended error or every virtual
table/function runtime failure. The adapter maps documented SQLite parser message families because
the API exposes syntax and missing-object prepare failures under the same coarse `Unknown` code;
unrecognized parser wording remains secret-safe Data rather than risking query disclosure.
`graphify` is unavailable on this host, so its optional post-change graph refresh could not run.
