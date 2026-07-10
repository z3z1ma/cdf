Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-d5-binary-format-autodetection.md, .10x/tickets/done/2026-07-09-p2-ws-i5-recorded-http-request-capture-race.md
Verdict: pass

# P2 D5 and I5 integration review

## Findings

The first D5 review raised one significant and one minor finding. Declared-schema HTTPS Arrow could pass planning and fail only at preview/run even though the transport was excluded, and a format mismatch carried a discovery-specific phase label outside discovery. The repair moved the support guard to common HTTP partition resolution before metadata/range access, added plan/deep no-contact/no-write coverage, and centralized command-neutral wording. Independent re-review passed with no findings.

The first I5 review raised two significant findings. The accepted socket remained nonblocking for response writes, so a transient `WouldBlock` could replace the original read race, and a worker panic combined with `Drop::join().unwrap()` could double-panic during unwinding. The repair restored bounded blocking writes and converted worker failures into stored fixture state while making teardown non-panicking. Independent re-review passed with no findings.

## Assumptions tested

- Binary inference was traced from glob-derived compilation through every matched local file, partition evidence, preview/run revalidation, and bounded HTTPS confirmation.
- Unsupported remote Arrow was tested through both plan and deep validation before transport contact or project/runtime writes.
- Explicit and inferred formats share the same resolved plan branch; local Arrow and HTTPS Parquet preservation cases remained green.
- The HTTP fixture was challenged on split headers, `WouldBlock`, EOF, cap, deadline, response backpressure, and teardown during failure.
- S5's request count, authorization, package, receipt, checkpoint, cursor, and redaction assertions were inspected after repair and remained intact.

## Verdict

Pass. D5 and I5 meet their bounded contracts after repair and may close with `.10x/evidence/2026-07-09-p2-d5-i5-integration.md`.

## Residual risk

The only residuals are explicitly outside these tickets: unsupported format aliases/text inference, remote Arrow/compressed binary transport, and resource-level multi-file schema aggregation. A10 is the active architectural owner for the latter and remains blocked pending semantic ratification; these exclusions do not weaken D5 or I5.
