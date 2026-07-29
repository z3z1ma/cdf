Status: done
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/done/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-d2-typed-cli-report-authority.md`

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
- 2026-07-27: Activated after D2 closure. Reconciliation found the 2026-07-25 WS9 tranche already
  implemented and evidenced the required outcome-first visual grammar, terminal policies,
  nonblocking progress, hosted `<1%` overhead, and canonical public-HTTPS recordings. D3 therefore
  preserves that authority and closes only current gaps: explicit shared section vocabulary,
  all-family current-state conformance, and a large-static-report benchmark alongside the
  existing million-event benchmark.
- 2026-07-27: Centralized `Summary`, `Proof`, `Effects`, `Recovery`, and `Attention` as shared
  panel constructors and replaced the ambiguous `Writes` heading. Added a representative
  nine-family policy matrix and a full-lifecycle 10,000-row benchmark with completeness checks.
- 2026-07-27: Reused the prior accepted hosted overhead/canonical-recording authority, ran fresh
  local and public-HTTPS product smokes, completed one frozen two-reviewer OCR batch, repaired the
  benchmark blind spot, accepted the explicitly bounded integration-matrix residual, and closed.

## Blockers

None.

## Evidence

- Shared hierarchy and vocabulary: `KeyValuePanel` owns the five cross-family headings; the
  representative family matrix covers inspect, plan, execute, mutate, recover, list, no-op,
  warning, and failure. Static search found no duplicate construction of those headings and no
  remaining `Writes` section heading.
- Terminal and disclosure behavior: 55/55 `cdf-cli-core` tests cover 40/80/160-column
  TTY/headless, ASCII/Unicode, no-color, exact rich/headless primitive snapshots, JSON isolation,
  progressive disclosure, copyable errors, and the nine representative families.
- Product composition: 299/299 `cdf-cli` tests cover actual typed report renderers, rich/headless
  command paths, redaction, JSON parity, and the renderer migration gate. Strict affected-root
  Clippy, benchmark compilation, formatting, and diff checks pass.
- Performance: the final local Criterion cell measured 50.101 milliseconds for one million
  buffered events (19.960 million events/second), 516.48 microseconds for 10,000 high-partition
  events, 3.3964 milliseconds for a prebuilt 10,000-row report, and 4.5571 milliseconds for full
  10,000-row construction plus rendering. The existing hosted P3 result remains `-0.4407%`
  enabled-versus-disabled progress overhead against the maximum `+1%` criterion.
- Product smokes: the current binary planned one local 2 GiB Parquet partition and completed the
  public five-partition HTTPS run with 5,000 rows, five segments, package, verified DuckDB receipt,
  committed checkpoint, and copyable inspect command.
- Reproducible commands, exact outputs, artifact checksums, host details, claims, and limits:
  `.10x/evidence/2026-07-27-d3-cli-conformance.md`.

## Review

`open-code-review-delegate` deterministically selected commit `dc4b94f0` and resolved the project
rules. Two independent reviewers reported no critical or high findings and converged on one
medium concern: the cross-family Cartesian matrix uses representative `RenderDocument`
compositions rather than constructing every private product report through every output mode.
A second reviewer also found that the initial large-report cell excluded construction and did not
prove completeness.

The benchmark finding was repaired once: an untimed preflight proves exactly 10,000 resource rows
plus first/last identities, and a separate measured cell constructs and renders the full document.
Focused compile/test/strict-Clippy checks and the benchmark pass after repair.

Verdict: concerns with accepted residual risk. The production delta is shared title constructors
and `Writes` to `Effects` wording, not new command rendering or transport behavior. Actual
production composition remains covered by 299 command tests, D2's typed-report/static gate, and
fresh local/public product smokes. Exporting private reports or duplicating every command fixture
solely to create a second Cartesian harness would add test-only authority disproportionate to this
medium, non-correctness risk. The limitation is explicit in the evidence record; no second review
cycle was commissioned per the bounded-review policy.

## Retrospective

The apparent holistic rewrite was mostly already complete: WS9 had paid for the visual language,
terminal contracts, progress design, hosted overhead, and canonical recordings. Searching durable
authority before editing prevented a second aesthetic rewrite. The smallest complete D3 was to
turn repeated vocabulary into code-owned constructors and close two evidence gaps.

The first benchmark measured only rendering of a hot prebuilt document. Review correctly exposed
that this was narrower than the ticket language; adding a separately labelled full construction
cell and completeness preflight preserved the useful kernel measure without overstating it.

The first local smoke failed because an old shared project retained a version-1 discovery
artifact. Preserving that typed failure, then rerunning in a fresh isolated project, distinguished
environment drift from a product regression. These lessons are distilled into
`.10x/knowledge/cli-report-authority.md` and the canonical/mirrored
`audit-cli-report-authority` skill.
