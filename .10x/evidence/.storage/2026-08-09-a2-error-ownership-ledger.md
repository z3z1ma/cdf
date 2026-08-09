Status: recorded
Created: 2026-08-09
Updated: 2026-08-09

# A2 finite-drain error ownership ledger

## Scope and reproduction

This ledger covers the error constructions added while closing the neutral finite-drain runtime
certificate. The frozen production-file manifest is:

```text
crates/cdf-engine/src/execution/orchestration.rs
crates/cdf-kernel/src/position/cdc.rs
crates/cdf-project/src/runtime/orchestration.rs
crates/cdf-project/src/runtime/validation.rs
crates/cdf-runtime/src/source.rs
crates/cdf-source-rest/src/runtime.rs
```

After the closing commit, reproduce the added-construction inventory without temporary files with:

```sh
git diff --unified=0 6687f80c..HEAD -- \
  crates/cdf-engine/src/execution/orchestration.rs \
  crates/cdf-kernel/src/position/cdc.rs \
  crates/cdf-project/src/runtime/orchestration.rs \
  crates/cdf-project/src/runtime/validation.rs \
  crates/cdf-runtime/src/source.rs \
  crates/cdf-source-rest/src/runtime.rs \
  | rg '^\+.*CdfError::|^\+.*ErrorKind::'
```

The final inventory is five constructions across three site-bearing files; the other three files
change typed authority or control flow without constructing a new error.

## Per-site classification

| File and construction family | Kind | Repair owner and rationale |
|---|---|---|
| `cdf-project/src/runtime/validation.rs`: declared unordered cursor | `Contract` | The project author must declare an ordered cursor or remove it from a bounded resource. |
| `cdf-project/src/runtime/validation.rs`: drain without a compiled source frontier | `Contract` | The project/source configuration does not provide restart authority required by the requested drain extent. |
| `cdf-runtime/src/source.rs`: empty resume-token scope capability | `Contract` | A source driver supplied an invalid compiled capability declaration. |
| `cdf-runtime/src/source.rs`: duplicate or noncanonical resume-token scopes | `Contract` | A source driver supplied invalid compiled capability ordering. |
| `cdf-source-rest/src/runtime.rs`: failure serializing CDF-owned full-scan authority | `Internal` | The tuple is entirely validated, CDF-owned typed state; serialization failure is an invariant failure. |

No changed path catches or stringifies an embedded typed external error, changes retry metadata, or
adds credential-bearing context. The new diagnostics contain only resource/cursor identifiers or
fixed capability language. The REST authority serializes the compiled-plan hash and scan identity,
not endpoint credentials, and its error message does not include serialized values.

## Behavioral matrix

- A bounded REST replacement with no cursor completes through package, destination receipt, and
  checkpoint using a typed `rest.full_scan_completion.v1` foreign-state authority.
- A MongoDB event-prefix drain with no declared cursor is accepted only when the compiled source
  advertises its exact resume-token scope; a receipt/checkpoint crash recovers without reopening
  the source or repeating destination writes.
- The formerly overflowing finite-drain settlement test and the new MongoDB certificate both pass
  on the ordinary test-thread stack.

Limits: these focused cases do not inject OS faults or third-party SDK wrapper errors because this
change does not alter those ownership boundaries. `graphify update .` could not run because the
`graphify` executable is not installed in this environment (`zsh: command not found: graphify`).
