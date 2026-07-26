Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/tickets/cancelled/2026-07-11-p1-cx1-terminal-policy-help.md

# P1 CX2: compact renderer and actionable errors

## Scope

Add activity, compact summary, borderless table/list, narrow-stack, contextual error/help, and value-expansion primitives; migrate static command families without changing report/JSON truth.

## Acceptance criteria

- Ordinary output removes ornamental rules and unnecessary boxed grids.
- Default hierarchy is outcome, essential evidence, next action; verbose adds proof detail.
- Errors render stable code/context/help through renderer primitives.
- Unicode display width, narrow fallback, redaction, and JSON stability are proven.

## Blockers

None. CX1's implemented terminal/help foundation is retained; its cancelled pager remainder is not
a prerequisite.

## Evidence expectations

Representative normal/verbose/narrow/ASCII/error snapshots, JSON compatibility tests, truncation-detail access, redaction adversarial cases, and focused CLI quality commands.

## References

- `.10x/decisions/cli-design-language-and-renderer.md`
- `.10x/specs/cli-error-experience-catalog.md`

## Journal

- 2026-07-14: Live `fineweb.documents` smoke testing correctly rejected Hugging Face's redirect from configured `huggingface.co` to unlisted `cas-bridge.xethub.hf.co`, but the contextual remediation incorrectly said to inspect secret references/credential providers. The error must name the denied redirect host, identify the source allowlist as the governing input, and offer the concrete allowlist edit or an intentional no-follow alternative; this is a CX2 catalog/renderer defect, not a transport fail-open request.
- 2026-07-25: Backlog grooming removed the unratified pager remainder from CX2's dependency path.
  Existing terminal/help behavior remains the input foundation; this ticket owns the next visible
  daily-driver improvement.
- 2026-07-25: Activated as the static-output slice of one holistic WS9 rewrite, not an isolated
  command-family restyle. The tranche starts at the shared renderer/output boundary, removes
  ornamental rules and boxed-grid defaults for every command, establishes outcome/evidence/action
  hierarchy and visibility levels, then rewrites execution summaries and errors against those
  primitives before CX3 adds the live subscriber.
- 2026-07-25: Replaced the ornamental rule/boxed-grid vocabulary with shared status, activity,
  borderless table, stacked key-value, `Next:`, and structured error primitives. Normal
  plan/explain/run/replay/preview output now leads with the decision or outcome; `-v` retains
  contract, destination, migration, receipt, gate, and diagnostic evidence without forking report
  truth. All migrated command families consume the same renderer boundary.
- 2026-07-25: Error rendering now emits a stable `error[CODE]` heading, structured context,
  `help:` remediation, and `try:` suggestions. Corrective commands and identifiers remain
  copyable even when a terminal must wrap them; compact display primitives use Unicode display
  width and stacked 40-column fallbacks. JSON error/result envelopes remain unchanged except for
  additive run byte/duration facts.

## Evidence

- Ordinary/verbose/narrow/ASCII/Unicode/error/JSON/redaction evidence is recorded in
  `.10x/evidence/2026-07-25-cli-experience-rewrite.md`.
- Strict all-target Clippy passed for `cdf-cli-core`, `cdf-cli`, and the isolated CLI benchmark
  crate.
- The complete 320-test CLI/core suite passed, including generated-artifact freshness, renderer
  migration, redaction, terminal matrix, progressive disclosure, and JSON isolation.
- The real `github.userdata` plan in `/Users/alexanderbut/code_projects/tmp` rendered the compact
  decision summary in normal mode and the full proof surface in verbose mode.

## Review

Fresh-hat adversarial review found and corrected three significant issues before closure:

1. hard-wrapping errors split resource ids and commands, so actionable records now remain exact and
   copyable while the terminal owns visual wrapping;
2. normal preview output could enumerate thousands of field names, so it now reports the field
   count and reserves the list for verbose output;
3. long key/value/status content could exceed narrow display framing, so shared compact primitives
   now wrap or stack using Unicode display width.

Verdict: pass. CX4 owns the hosted performance/recording residual, not static renderer semantics.

## Retrospective

The effective seam was information priority, not decoration: once outcome/evidence/action and
normal/verbose/diagnostic visibility became renderer concepts, command-family cleanup became
mechanical. Width limits must never corrupt copyable authority such as ids, paths, or corrective
commands; terminal wrapping is safer than inserting semantic newlines into those values.
