Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-i4-s5-s7-standalone-conformance.md

# P2 WS-I5 remove S5 recorded HTTP request-capture race

## Scope

Make the S5 conformance `RecordedHttpServer` read a complete HTTP request header deterministically under parallel workspace load. The accepted socket currently inherits nonblocking behavior and maps `WouldBlock` to a zero-byte EOF, so it can record only the request line before the authorization header arrives and fail an otherwise successful S5 run.

## Acceptance criteria

- The recorded server retries bounded `WouldBlock` reads until `\r\n\r\n`, EOF, the existing 8192-byte cap, or an explicit short deadline.
- The fixture cannot hang indefinitely and reports a useful failure when the header is incomplete.
- S5 still proves four authenticated `GET /items` requests and all existing package/receipt/checkpoint assertions.
- Focused S5 repeats and the full parallel workspace nextest suite pass without weakening the authorization assertion.

## Evidence expectations

Focused repeated S5 execution, full parallel workspace nextest, formatting/diff checks, and adversarial review of timeout/error behavior.

## Explicit exclusions

No product HTTP transport, authentication, discovery, runtime, S5 semantics, or request-count behavior changes. This ticket repairs only the deterministic recorded-server test harness.

## Progress and notes

- 2026-07-09: Opened after two consecutive full-workspace nextest runs each produced 791/792 with only `p2_s5_rest_discover_pin_preview_run_package_checkpoint_conformance` failing its all-requests-contain-authorization assertion, while the same test passed immediately in isolation. Inspection found the nonblocking fixture converts every read error, including `WouldBlock`, to zero and breaks before the header terminator. Under parallel load this records a partial request but still sends a response, exactly matching the observed concurrency-only failure.
- 2026-07-09: Repaired only `RecordedHttpServer`: accepted sockets are explicitly nonblocking and a bounded header reader now retries `WouldBlock` (and interrupted reads) until `\r\n\r\n`, EOF, the retained 8192-byte cap, or a one-second deadline. EOF, cap, and deadline exits return distinct errors; the server thread reports capture failures with fixture context instead of recording partial requests and returning a successful response.
- 2026-07-09: Added fixture-level regressions for a request whose authorization header arrives after a readiness gap, a deterministic `WouldBlock`-then-complete reader, and incomplete-header EOF, cap, and timeout behavior. The two helper tests passed 2/2; the unchanged S5 conformance passed once directly and 20/20 focused repeated nextest invocations, retaining its exact four-request authorization assertion and package/receipt/checkpoint checks. Formatting, the scoped diff check, and strict all-target `cdf-conformance` Clippy passed.
- 2026-07-09: A full parallel workspace nextest attempt did not reach execution because concurrent WS-D5 edits temporarily left `cdf-declarative/src/file_runtime.rs` uncompilable (`local_file_discovery_candidate` arity and missing `ResolvedFileMatch::format`). I5 did not edit that out-of-scope surface; full-workspace nextest and Clippy remain to be rerun after the shared tree stabilizes.
- 2026-07-09: Strict all-target `cdf-conformance` Clippy subsequently passed. A second full parallel workspace attempt began after the shared tree compiled but was interrupted at 285/800 because WS-D5 was still integrating format inference: unrelated CLI and live-run tests failed on changed-format and in-progress diagnostic behavior. The owning D5 lane confirmed it was mid-refactor and asked I5 to defer the next workspace run until its final stabilization. After those concurrent changes, both fixture regressions passed again 2/2 and exact S5 passed again 1/1. Parent integration still owes the clean full-workspace pass; I5 remains open and unmoved for review as directed.
- 2026-07-09: Independent review found two fixture-lifecycle gaps in the first repair: the accepted socket stayed nonblocking for `write_all`/`flush`, and a capture panic followed by `Drop::join().unwrap()` could double-panic during unwinding. The worker now returns errors into shared fixture state instead of panicking, `requests()` surfaces that state deterministically, and `Drop` joins without panicking even if the worker itself unexpectedly panics. After bounded nonblocking header capture, the socket returns to blocking response I/O with a one-second write timeout.
- 2026-07-09: Added regressions proving a four-MiB response completes after delayed client reading and an incomplete header is surfaced with useful context while explicit teardown remains non-panicking. Final focused verification passed the four fixture lifecycle tests plus exact S5 and S7 (6/6), a post-repair S5 repetition passed 10/10, and warnings-denied all-target `cdf-conformance` Clippy, workspace formatting, and scoped diff checks passed. Full parallel workspace integration remains assigned to the parent after concurrent lanes settle; the ticket remains open and unmoved for parent closure.
- 2026-07-09: Closed after independent re-review passed and final parallel workspace nextest passed 809/809, including S5 and all long-run goldens. Evidence: `.10x/evidence/2026-07-09-p2-d5-i5-integration.md`. Review: `.10x/reviews/2026-07-09-p2-d5-i5-integration-review.md`.

## Blockers

None. The repair is test-harness-only and must not weaken the S5 assertion.
