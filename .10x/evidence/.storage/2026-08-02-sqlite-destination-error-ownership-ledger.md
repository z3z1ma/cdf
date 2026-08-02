# SQLite destination error-ownership ledger

Frozen scope: `crates/cdf-dest-sqlite/**/*.rs` after the 2026-08-02 closure repair.

## Reproduction

```sh
rg --files crates/cdf-dest-sqlite -g '*.rs' | sort
rg -n --no-heading \
  'CdfError::(new|auth|contract|data|destination|environment|internal|rate_limited|transient)|ErrorKind::' \
  $(rg --files crates/cdf-dest-sqlite -g '*.rs' | sort)
```

The frozen manifest contains 16 Rust files. Twelve files bear constructor or
direct-kind matches; `identifier.rs`, `lib.rs`, `models.rs`, and the 18-line
`transaction.rs` facade bear none. The second command yields 145 syntactic
matches: 144 actual CDF constructor/direct-kind sites plus the explicitly
classified `std::io::ErrorKind` false positive at `error.rs:12`.

## Frozen Rust manifest

```text
crates/cdf-dest-sqlite/src/error.rs
crates/cdf-dest-sqlite/src/identifier.rs
crates/cdf-dest-sqlite/src/lib.rs
crates/cdf-dest-sqlite/src/mapping.rs
crates/cdf-dest-sqlite/src/mirrors.rs
crates/cdf-dest-sqlite/src/models.rs
crates/cdf-dest-sqlite/src/package.rs
crates/cdf-dest-sqlite/src/plan.rs
crates/cdf-dest-sqlite/src/receipts.rs
crates/cdf-dest-sqlite/src/runtime.rs
crates/cdf-dest-sqlite/src/sheet.rs
crates/cdf-dest-sqlite/src/tests.rs
crates/cdf-dest-sqlite/src/transaction.rs
crates/cdf-dest-sqlite/src/transaction/session.rs
crates/cdf-dest-sqlite/src/transaction/verifier.rs
crates/cdf-dest-sqlite/src/transaction/writer.rs
```

## Exact per-site classification

Every emitted line appears exactly once below. Comma-separated sites share a
classification but remain individually enumerated.

| Sites | Resulting owner | Rationale |
|---|---|---|
| `error.rs:12` | n/a | `std::io::ErrorKind` false positive. |
| `error.rs:14` | Destination | Invalid/truncated durable destination bytes require destination restore. |
| `error.rs:16` | Environment | Other raw host I/O belongs to host/path repair. |
| `error.rs:18,102` | Dynamic, provenance-preserving | Central wrappers retain embedded typed errors, then add safe action context. |
| `error.rs:40,89` | Contract | Invalid/NUL configured paths are caller contract failures. |
| `error.rs:50` | Transient | Busy/locked/schema-change/abort conditions can succeed after external state changes. |
| `error.rs:57` | Environment | Permission, read-only, OOM, host I/O, disk-full, open, and large-file facilities belong to the host. |
| `error.rs:63,74,81,88` | Destination | Durable corruption, wrong database/shape, target constraints, missing rows, and foreign decode contradictions belong to the destination. |
| `error.rs:72` | Data | Payload NOT NULL and datatype extended constraints contradict finalized package schema/value authority. |
| `error.rs:80,99,100` | Internal | Interrupt without injected cancellation and prepared-statement/API misuse contradict adapter lifecycle. |
| `error.rs:117` | Internal | An injected run cancellation is execution control, preserving the runtime cancellation owner. |
| `mapping.rs:51,65` | Data | Finalized package schema disagrees with its prepared mapped schema. |
| `mapping.rs:111,315` | Contract | Unsupported Arrow types or impossible caller-selected conversions fail preflight. |
| `mapping.rs:325` | Internal | A concrete array downcast contradicts the already matched Arrow type. |
| `mirrors.rs:41,642` | Internal | CDF-owned immutable-evidence or mirror serialization failed. |
| `mirrors.rs:143,213,257,277,316,379,647` | Destination | Immutable evidence conflict, vanished readback, state-history conflict, or malformed durable mirror JSON belongs to the SQLite artifact. |
| `mirrors.rs:654` | Data | Package/state numeric evidence exceeds SQLite INTEGER representation. |
| `package.rs:16,26,36,48,56,66,71` | Data | Finalized package hash, segment identity/order/count, and manifest values contradict replay authority. |
| `package.rs:64` | Internal | A segment vanished from CDF's own manifest-order lookup. |
| `plan.rs:16,25,38,44,50,101,129` | Contract | Token, reserved-name, merge-key, disposition, and identifier declarations are caller contracts. |
| `plan.rs:32` | Internal | A framework-owned flag on a non-governed name contradicts CDF schema classification. |
| `receipts.rs:150` | Internal | CDF-owned quarantine evidence serialization failed. |
| `receipts.rs:156,201` | Data | Package quarantine or duplicate-segment counts exceed declared bounds. |
| `receipts.rs:205` | Destination | A stored duplicate receipt contradicts durable segment counts. |
| `runtime.rs:145,217,240,331,334,343` | Contract | Missing merge/execution binding, target mismatch, or invalid URI/path is caller/orchestration configuration. |
| `runtime.rs:238` | Internal | A resolved runtime lost its validated target. |
| `sheet.rs:23,57` | Contract | Empty/unconnected paths are invalid API use before file mutation. |
| `tests.rs:264,268,1146` | Internal, test-only | Sentinels reject interface paths the scenario must never reach. |
| `tests.rs:1188` | Internal, test-only assertion | Injected SQLite VM interruption retains cancellation ownership. |
| `tests.rs:1205,1262` | Destination, test-only assertion | Missing durable DB and existing target constraints classify to destination repair. |
| `tests.rs:1217` | Environment, test-only assertion | A host path whose parent is a file classifies to host repair. |
| `tests.rs:1240` | Data, test-only assertion | Payload NOT NULL contradiction classifies to producer repair. |
| `transaction/session.rs:113,116,146,242,246,284,398,494,503,539` | Internal | Missing journal, connection, receipt, managed state, or representable host time violates session lifecycle. |
| `transaction/session.rs:158` | Destination | In-transaction quarantine readback differs from finalized evidence. |
| `transaction/session.rs:233,300,381,386,391,523` | Destination | Invalid durable session phase or request/plan mismatch is safely rejected before another mutation. |
| `transaction/session.rs:432,435,452` | Data | Segment provenance or acknowledgement arithmetic exceeds package authority. |
| `transaction/verifier.rs:27,59,62,69,71,73,81,83,90,93,100,104,112,130,139,145,152,157,164,193,213,227,237,277,283,285,289,295,305,319,324,329,336,342` | Destination | Fresh verification found missing, malformed, contradictory, non-unique, or non-exact durable receipt, evidence, schema, index, state, quarantine, segment, or provenance authority. |
| `transaction/writer.rs:69,75,228,234,237,247,250,310,312,342,348,362,420` | Data | Package counts, identities, payload ordinals, row keys, or duplicate merge keys contradict finalized package authority. |
| `transaction/writer.rs:87,120,444,550` | Destination | Corrupt allocator/target shape, ambiguous merge target, or impossible durable count belongs to destination repair. |
| `transaction/writer.rs:139` | Contract | Adding a required existing-target column would invent a default. |
| `transaction/writer.rs:361,383,511` | Internal | CDF-owned cardinality, unreachable CDC dispatch, or merge arithmetic invariant failed. |

## Foreign-wrapper and context audit

`classify_sqlite_error_in` first walks the complete source chain for an embedded
`CdfError`, including errors embedded in `io::Error`, retaining kind and retry
metadata. Only raw foreign errors are reclassified. Stable primary SQLite codes
separate contention (`Transient`), host facilities (`Environment`), durable
artifact state (`Destination`), caller configuration (`Contract`), and
adapter-owned API invariants (`Internal`). Payload context additionally reads
the stable extended code: NOT NULL/datatype contradictions are `Data`, while
unique/check/foreign-key and other target constraints remain `Destination`.

Verifier open first uses fallible `try_exists`: a missing durable receipt file
is `Destination`, while metadata/open permission, device, symlink-loop, missing
parent, and analogous host failures retain `Environment`. Runtime health also
uses `try_exists`; no authoritative path check uses lossy `exists()`.

Injected cancellation flows through `ExecutionServices`, session, writer,
mirror backend, merge, and verifier. The SQLite VM progress hook interrupts long
statements; `OperationInterrupted` with an injected cancelled authority becomes
the typed Internal cancellation result rather than a destination failure.

## Layout and supporting boundary

The transaction authority is an explicit acyclic module graph:

```text
transaction.rs (18-line facade)
  -> session.rs -> writer.rs
                -> verifier.rs -> writer.rs
```

The exact product-matrix repair in `cdf-contract/src/policy.rs` remains outside
the frozen crate roots and adds no constructor. The roofline content identity
now includes all three transaction modules plus the runtime execution authority.
The earlier raw roofline sample was not rerun and remains inconclusive.

## Limits

- Ownership never depends on unstable SQLite message text.
- The focused macOS tests exercise the named context boundaries; they do not
  inject every kernel/device failure.
- `graphify update .` remains unavailable because the executable is absent.
