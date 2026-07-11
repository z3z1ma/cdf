Status: active
Created: 2026-07-11
Updated: 2026-07-11

# CLI progressive disclosure and terminal contract

## Context

P1 established a centralized renderer and rich evidence, but the resulting default output treats nearly every fact as primary. P3 adds high-rate parallel execution where replaying all milestones and rendering heavy tables is both cognitively noisy and a performance risk. Research in `.10x/research/2026-07-11-rust-cli-experience-study.md` found a stable pattern across uv, Jujutsu, ripgrep, bat, and Cargo: compact primary outcomes, environment-aware adaptation, explicit depth controls, and plain composable output.

## Decision

Human output follows progressive disclosure:

1. Default interactive execution shows a compact aligned activity stream, one current line per active partition/resource where useful, and one outcome summary containing the values needed to trust and continue the run.
2. `-v` adds evidence identifiers, phase timing, capability choices, and per-partition detail. Repeating `-v` enables diagnostic detail and tracing selection without changing command semantics. `-q` suppresses progress and non-primary success narration while preserving primary query/list data, warnings, and errors. Quiet and verbose are mutually exclusive.
3. Full historical evidence is rendered by `cdf inspect`, `cdf explain`, or explicit verbose detail; ordinary success output never dumps the complete event history.

Primary command results go to stdout. Progress, warnings, diagnostics, and errors go to stderr. `--json` remains a stable compatibility alias for single-envelope JSON and emits no human progress. A future streaming machine mode may be added only with its own artifact/version contract; it is not implied by this decision.

The global terminal policy is explicit and centralized:

- `--color auto|always|never`, with `--no-color` retained as an alias for `never`; `NO_COLOR` is honored unless the operator explicitly selects `always`;
- `--progress auto|always|never`; noninteractive `auto` emits bounded line milestones, never cursor control;
- `--pager auto|never`; only bounded read-only output such as help, diff, explain, inspect, and large query results may auto-page, and never when stdout is redirected;
- `--unicode auto|always|never`; plain ASCII remains semantically complete;
- width comes from the terminal when available, then `COLUMNS`, then a deterministic fallback, and all layout uses terminal display width rather than byte/character count.

Visual grammar is restrained. Default tables are borderless aligned columns or compact lists; boxed grids are reserved for cases where cell boundaries materially prevent ambiguity. Color reinforces status and hierarchy but never carries meaning alone. Full-width ornamental rules are removed from ordinary output. Stable activity verbs (for example `Planned`, `Read`, `Validated`, `Packaged`, `Loaded`, `Verified`, `Committed`) form the left gutter, with counts, rates, and duration aligned after them.

Errors render as `error[CODE]` plus the shortest causal statement, relevant context, and one or more exact `help:` actions. Details and evidence identifiers follow under verbose mode. Structured error JSON remains additive and stable.

Progress rendering is a subscriber, never runtime authority. Interactive refresh is rate-limited and coalesced; terminal rendering must not backpressure execution. Final output is deterministically reconstructable from the same event/report facts, and headless output remains line-oriented.

## Alternatives considered

- Preserve the current report-like default and add more color: rejected because repetition and hierarchy, not palette, are the problem.
- Build a full-screen TUI: rejected because it weakens logs/composition and adds a resident UI architecture CDF does not need.
- Copy one tool's visual identity: rejected because CDF's package/receipt/gate evidence is distinct; the shared laws matter more than branding.
- Print nothing on success by default: rejected because data movement requires visible trust signals and next actions.

## Consequences

This decision refines `.10x/decisions/cli-design-language-and-renderer.md`; its centralized renderer, redaction, semantic palette, JSON isolation, and headless guarantees remain active. WS9 must migrate snapshots and docs deliberately because the human presentation changes substantially while machine contracts and execution semantics do not.

The event/report model remains source authority. Renderer code may choose presentation and depth but may not re-derive execution truth. P3 lab measurements include event ingestion and render consumption so aesthetics cannot become a throughput tax.
