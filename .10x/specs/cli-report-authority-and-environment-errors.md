Status: active
Created: 2026-07-26
Updated: 2026-07-26

# CLI report authority and environment errors

## Purpose and scope

This specification completes the existing typed `CommandOutput`/`RenderDocument` migration and
corrects the lower-layer error taxonomy before the holistic CLI experience pass. It preserves
the stable JSON success/error envelopes, command grammar, redaction, and stdout/stderr contract.

## Typed report authority

Every product command outcome MUST be represented by one serializable typed report. That report
is the sole input to both the JSON envelope and a report renderer. Command execution modules MAY
construct domain values and the report, but MUST NOT assemble human layout.

A report renderer maps a report to `RenderDocument` under the shared design system. Styling,
glyphs, width adaptation, panels, tables, humanized values, truncation, terminal mode, and
operator affordances belong only to rendering modules. The contract MAY use a trait or exhaustive
report enum only if it reduces real duplication; it MUST NOT introduce one service trait per
command or erase typed report fields into string maps.

The existing `CommandOutput::rendered` boundary remains the transport authority. Migration removes
command-local layout functions and direct rendering primitives from execution modules; it does
not duplicate reports or preserve legacy plain-string shims.

Every report family MUST have headless and rich-TTY snapshots. Machine JSON tests prove the same
report fields drive JSON. Redaction tests run before both render paths.

## Environment error kind

Add `ErrorKind::Environment` for failures caused by the executing host or process environment
rather than CDF invariants, user contract, source data, authentication, or destination semantics.
Examples include:

- current-directory and local filesystem availability;
- file-descriptor/resource-limit exhaustion;
- missing required executables or unsupported host facilities;
- process environment and temporary-directory failures;
- local I/O failures not attributable to malformed source/package data.

`Internal` remains reserved for violated CDF invariants, impossible states, poisoned ownership
that indicates a program defect, and serialization/logic failures that should be reported as a
bug. Environment failures MUST provide host-specific remediation and MUST NOT say to repair
project configuration unless configuration is actually the cause.

The active error catalog guarantees stable exit codes. Reclassification therefore retains exit
code 70 for the new environment kind in this program; only kind, code area/remediation, and
diagnostic detail change. A future exit-code revision requires a separately ratified compatibility
decision.

## Holistic experience pass

After report and error authority are complete, the CLI pass MUST be renderer-wide rather than a
sequence of unrelated command beautification patches. It must:

- define one information hierarchy for inspect, plan, execute, mutate, recover, and list outcomes;
- use modern Rust CLI precedents for density, verbs, progress, color restraint, error locality,
  copyable next actions, and headless degradation;
- preserve one-screen normal summaries while verbose/inspect expose proof detail;
- remove duplicated facts, generic remediation, ornamental noise, and inconsistent terminology;
- benchmark one million progress events and representative large reports;
- snapshot 40/80/160-column TTY, headless, ASCII, no-color, redirected, JSON, success, warning,
  failure, no-op, and recovery modes.

## Acceptance scenarios

- Given any command success, one typed report produces both JSON and human output; command
  execution source contains no layout construction.
- Given a missing current directory or exhausted file descriptor limit, the error is
  `Environment`, retains exit 70, names the host failure, and offers a relevant recovery.
- Given an invariant mismatch, the error remains `Internal` and tells the operator how to capture
  evidence/report the defect rather than blaming the environment.
- Given rich TTY and redirected output for the same report, facts are identical while presentation
  adapts without ANSI/progress contamination.
- Given the established performance workloads, renderer/progress overhead remains within
  `.10x/specs/cli-interaction-excellence.md`.

## Explicit exclusions

No full-screen TUI, web UI, dynamic report plugins, command grammar rewrite, JSON field removal,
or change to execution/package semantics is included. Concrete first-party names in useful help
examples are not architectural coupling and need not be removed.
