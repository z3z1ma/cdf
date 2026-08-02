Status: recorded
Created: 2026-08-02
Updated: 2026-08-02

# SQLite source error-ownership audit

## Observation

The frozen `cdf-source-sqlite` Rust scope contains eleven files and 109 `CdfError` constructor sites.
The durable ledger classifies 58 Contract, 42 Data, six Internal, two boundary-classified `new`,
and one test-only RateLimited constructor. Of those sites, 107 are production constructors and two
are typed-preservation regression fixtures. No direct `CdfError` struct literal bypasses the
constructors.

The direct-enum supplement finds three `ErrorKind::Internal` mappings in the rusqlite classifier
in addition to the six direct Internal constructors. The classifier mappings cover misuse of the
CDF-generated read-only query/parameter lifecycle and validated-plan/Arrow assembly invariants;
none is an external SQLite data, caller-contract, or host-facility failure.

Five direct Internal constructors remain CDF-invariant failures. The sixth is the explicit
run-cancellation translation used when the installed SQLite VM progress hook returns
`OperationInterrupted`; it preserves host cancellation ownership rather than reclassifying the
interruption as source data or retrying it.

The foreign-wrapper boundary walks both the generic source chain and nested `std::io::Error`
payloads before applying rusqlite fallbacks. Embedded typed errors retain kind, retry delay, and
primary message while receiving safe operation context. Raw source-file absence/wrong shape is
Data; permission/device/resource I/O is Environment; busy/locked SQLite state is Transient;
corruption/type failures are Data. A failed open re-stats the selected file so a concurrent
missing/wrong-shape change is not flattened into Environment.

The audit found and repaired two ownership defects: Arrow `RecordBatch` assembly was reclassified
from generic Data to Internal, and rusqlite `InvalidPath` was prevented from forwarding its
path-bearing `Display`. The path regression proves the private locator is absent from the emitted
message.

The final refresh also classifies the contact-free compile and live physical-observation boundary,
including the dedicated schema-policy module introduced by the directional module-DAG repair:
invalid declared projection/identifier shape is Contract, while absent verified observations,
catalog drift, and full/projected physical-schema mismatches are Data owned by the selected SQLite
resource. Those messages identify only bounded resource/table/schema context and never include the
database locator.

## Procedure

The exact scope and sites can be reproduced without `/tmp`:

```sh
LC_ALL=C rg --files -g '*.rs' crates/cdf-source-sqlite | sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'CdfError::(new|transient|rate_limited|auth|contract|data|destination|environment|internal|from)' \
  crates/cdf-source-sqlite | sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'ErrorKind::Internal|\bInternal\b' crates/cdf-source-sqlite | sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'CdfError::internal|\.internal\(' crates/cdf-source-sqlite | sort
```

The frozen outputs are:

- `.10x/evidence/.storage/2026-08-02-sqlite-source-error-files.txt` — eleven Rust files.
- `.10x/evidence/.storage/2026-08-02-sqlite-source-error-sites.tsv` — one header plus 109 classified
  constructor rows.

Focused verification:

```sh
timeout 120s env CARGO_BUILD_JOBS=12 \
  cargo test -p cdf-source-sqlite --lib --offline -j 12 -- --nocapture
```

Result: 25 passed, 0 failed. The suite includes direct and nested typed-wrapper preservation with a
275 ms retry delay, corrupt/busy/host-I/O ownership splits, missing-file/table ownership without
path disclosure, invalid-path redaction, dynamic storage-class drift as Data, and production
snapshot/cursor behavior. It also exercises catalog/live uniqueness rejection, pre-copy
variable-cell bounds, SQLite VM cancellation ownership, declared-schema live physical observation,
and the explicit directional production-module boundary.

## What it supports or challenges

This supports the SQLite source spec's shared-taxonomy and path-redaction requirements and the
ticket criterion that adapter errors preserve exact ownership through rusqlite wrappers. It also
supports the claim that the connector does not leak the database locator through Debug or mapped
error messages.

## Limits

The inventory is exact for the frozen eleven-file Rust crate and does not claim to audit unrelated
workspace crates. Permission, not-found, typed-wrapper, corruption, and busy paths are constructed
deterministically; the suite does not inject every platform-specific SQLite extended code. Full
affected-crate lint, formatting, product-boundary, conformance, and certification gates are
recorded in the owning ticket rather than inferred here. Graph validation is excluded by the
orchestration boundary for this ticket.
