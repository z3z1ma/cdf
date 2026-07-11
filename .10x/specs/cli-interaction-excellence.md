Status: active
Created: 2026-07-11
Updated: 2026-07-11

# CLI interaction excellence

## Purpose and scope

This specification governs CDF's default human information hierarchy, terminal adaptation, verbosity, output channels, help, progress, errors, paging, and renderer performance. It refines—not replaces—the existing command grammar, error catalog, live-progress, redaction, and JSON contracts.

## Behavioral contract

Every command MUST identify one primary outcome. Default human output MUST present that outcome before supporting evidence and SHOULD fit within one terminal screen when the result set itself is bounded. Repeated facts MUST be elided rather than shown in both progress history and summary panels.

Execution commands MUST show phase activity using shared semantic verbs and end with rows/bytes, elapsed time and rate when known, admitted/quarantined counts, package/receipt/checkpoint gate state, and the most useful next action. Evidence hashes and full event histories MUST be available through verbose or inspect paths without dominating normal output.

Primary result data MUST use stdout. Progress, warning, diagnostic, and error narration MUST use stderr. Redirected human stdout MUST contain no ANSI, cursor movement, ornamental framing, or incidental progress. `--json` MUST remain one valid JSON envelope on its existing channel with no human contamination.

Global quiet, verbose, color, progress, pager, and Unicode policies MUST follow `.10x/decisions/cli-progressive-disclosure-terminal-contract.md`. Invalid/conflicting combinations MUST fail at parse time with an exact correction.

Help MUST give every public command and material option a concise operator-facing description. Short help MUST prioritize common commands/options; long help MUST expose complete flags, environment variables, defaults, examples, and related commands. Generated shell completions, man pages, and reference docs MUST derive from the same grammar authority.

Tables MUST measure Unicode display width, adapt to terminal size, and preserve access to truncated values. Narrow output MUST switch to stacked records instead of producing unreadable columns. Color and Unicode glyphs MUST have text/ASCII equivalents.

Interactive progress MUST coalesce/rate-limit redraws, preserve bounded memory, handle multiple partitions without unbounded terminal lines, and leave a stable final summary. Headless progress MUST be timestamped, line-oriented, bounded, and deterministic. Neither mode may block or authorize runtime progress.

Errors MUST include stable code, causal message, relevant context, and exact help. Secret redaction MUST occur before rendering and before pager/process handoff.

## Performance and conformance

The renderer/event subscriber MUST be benchmarked with at least one million synthetic events and a high-partition run. Event publishing MUST remain nonblocking; rendering enabled versus disabled MUST consume no more than 1% of end-to-end wall time at the P3 reference workload, excluding time the terminal itself is externally backpressured. Refresh frequency MUST default to at most 10 Hz.

Permanent conformance MUST cover TTY, redirected stdout/stderr, 40/80/160-column widths, ASCII-only terminals, `NO_COLOR`, explicit color override, quiet/verbose conflicts, progress modes, pager eligibility, Windows-compatible behavior, secret redaction, and JSON isolation.

## Acceptance scenarios

Given a multi-file run, normal interactive output shows coalesced phase activity and a compact final proof summary without a milestone-history table; `-v` exposes partition/evidence detail; `cdf inspect run` exposes the complete durable history.

Given `cdf run ... >result.txt`, stdout contains only the stable primary human result while progress remains plain on stderr. Given `--json`, stdout is one JSON success envelope and stderr is empty on success.

Given a 40-column terminal, all facts remain readable through stacked layout and no line exceeds the width solely because of renderer framing. Given ASCII/never modes, meaning and status remain complete without color or Unicode.

Given a usage error, the first line identifies `error[CODE]`, the next context names the bad input, and `help:` supplies a copyable correction.

## Explicit exclusions

No full-screen TUI, web dashboard, theme marketplace, dynamic output plugins, or machine-event streaming protocol is specified here. Execution semantics and JSON field removals are out of scope.
