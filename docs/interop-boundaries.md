# Foreign source boundaries

CDF converts Python and supervised-process producers into ordinary Arrow batches
at one source-neutral boundary. Planning records the mechanisms a source may
use. Execution records what actually happened, including the transfer mode,
rows, logical bytes, control-event count, and copy classification.

The copy labels are deliberately strict:

- `payload_zero_copy_verified` means that the observed batch passed a
  buffer-identity and lifetime proof.
- `payload_copy_known` means the boundary can count copied payload bytes
  exactly.
- `copy_unknown` means CDF cannot prove the count. Arrow compatibility alone
  never earns a zero-copy claim.

`cdf plan --verbose` renders the planned source boundary. `cdf run --verbose`
renders the observed aggregate, and `cdf run --json` exposes the same structured
report as `result.source_transfer`. This telemetry is operational evidence; it
does not participate in package identity.

## Implemented modes

| Producer | Transfer | Execution lane | Copy authority | Memory boundary |
|---|---|---|---|---|
| Embedded Python Arrow C Data | Arrow C Data Interface | blocking; one worker with the GIL, runtime-resolved CPU concurrency on free-threaded CPython | production reports unknown; dedicated PyArrow cells verify aliasing and lifetime separately | imported payloads and CDF conversion windows are ledger-accounted |
| Embedded Python dict rows | row compatibility | same embedded-Python lane | serialized row-window bytes are known copies | the configurable row/byte window is ledger-accounted |
| Supervised process Arrow | Arrow IPC stream | isolated process | unknown unless a future probe proves exact copies | pipe, decoder, batches, and child policy are bounded |
| Supervised process rows/Singer/Airbyte | NDJSON row compatibility | isolated process | unknown unless a future probe proves exact copies | pipe, parser, row window, diagnostics, and child policy are bounded |
| WASM | prospective | sandbox | unknown | no runtime or performance claim |

An embedded producer can allocate arbitrary native memory before yielding an
observable batch. CDF cannot enforce a total process ceiling around that code.
Use the supervised-process boundary when forceful cancellation and an
OS-enforced producer-memory ceiling are required.

## Host-labelled release observations

The following cells were measured on 2026-07-25 on the local
`aarch64-apple-darwin` development host. They are warm release-mode boundary
measurements, not complete project runs and not dedicated-host promotion
evidence.

| Cell | Observation | Managed peak |
|---|---:|---:|
| Python dict rows, 1,024 rows/window | 2.712M rows/s; 977 batches | 68,894 bytes |
| Python dict rows, default 8,192 rows/window | 2.815M rows/s; 123 batches | 549,150 bytes |
| Python dict rows, 65,536 rows/window | 2.537M rows/s; 16 batches | 4,391,198 bytes |
| Subprocess Arrow IPC, 524,288 rows | 33.599ms total; 13.535ms first batch | 20,750,593 bytes |
| Subprocess NDJSON, 524,288 rows | 60.156ms total; 11.126ms first batch | 57,540,609 bytes |

The Python 8K default is a tuning choice, not a hard ceiling. Larger row and
byte windows remain explicit project knobs. The subprocess observations include
process startup and `cat` transport on this host; Arrow IPC and NDJSON both
remain `copy_unknown` because output size is not a valid proxy for copied bytes.
