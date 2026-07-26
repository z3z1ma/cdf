Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/2026-07-26-prewave-d2-typed-cli-report-authority.md`

# Deliver the holistic CLI experience

## Scope

Apply one renderer-wide information architecture and visual language to every report family using
the completed typed-report authority. Reconcile prior modern Rust CLI research with current
product behavior, then implement the normal/verbose/headless/JSON experience as one coherent
system.

## Non-goals

- No full-screen TUI, command grammar rewrite, web surface, or execution semantic change.
- No command-by-command aesthetic patches outside the shared renderer.
- No ornamental output that obscures performance, gate state, remediation, or copyability.

## Acceptance criteria

- Inspect, plan, execute, mutate, recover, list, no-op, warning, and failure reports share one
  explicit information hierarchy and vocabulary.
- Normal bounded outcomes fit one screen where feasible; verbose/inspect retain full proof.
- Errors name cause, context, exact remediation, and next command without generic filler.
- TTY/headless/ASCII/no-color/redirected/JSON snapshots cover 40/80/160 columns and all report
  families.
- One-million-event and large-report benchmarks remain within the established overhead envelope.
- Real local and public-HTTPS smoke recordings demonstrate first-attempt readability and preserve
  package/receipt/checkpoint facts.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/specs/cli-interaction-excellence.md`
- `.10x/decisions/cli-progressive-disclosure-terminal-contract.md`
- `.10x/decisions/cli-design-language-and-renderer.md`
- `.10x/research/2026-07-11-rust-cli-experience-study.md`

## Assumptions

- User-ratified: CLI UX requires a holistic rewrite informed by the best modern Rust tools.
- Record-backed: output rendering is outside identity and must remain below 1% overhead at the P3
  reference workload.

## Journal

- 2026-07-26: Sequenced after error/report authority so the visual pass edits renderer ownership
  once rather than reopening thirty command modules.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
