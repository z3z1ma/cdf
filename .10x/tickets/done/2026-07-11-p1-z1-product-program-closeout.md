Status: done
Created: 2026-07-11
Updated: 2026-07-26
Parent: .10x/tickets/done/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/tickets/done/2026-07-08-p1-product-ws8-release-engineering.md, .10x/tickets/done/2026-07-11-p1-ws9-cli-experience-excellence.md

# P1 Z1: product-experience program closeout

## Scope

After WS8/WS9 close, map every P1 parent criterion and active CLI scenario to evidence, rerecord the Chapter 23 terminal session, reconcile coverage/status/references, run final source/destination-extension and UX/redaction/accessibility reviews, extract retrospective learning, and move the P1 graph terminal only when supported.

## Acceptance criteria

- TTY/headless/JSON/help/error/progress/width/color/Unicode and performance scenarios map to permanent evidence.
- Hosted pre-release checksums/install/completions/man pages and canonical demo are recorded.
- No old raw human-output path or source/destination-specific product branch remains unowned.
- Active specs, snapshots/generated docs, statuses/dependencies, and coverage agree.
- All residual risks/follow-ups have durable owners before P1 moves done.

## Evidence expectations

Aggregate criterion matrix, terminal recordings/snapshots, release run/artifacts, generated freshness, redaction/accessibility/performance output, architecture review, closure audit, and retrospective records.

## Explicit exclusions

No implementation repair, CI polling, evidence invention, JSON breaking change, or source/destination feature work under closure bookkeeping.

## Blockers

None. WS8 and WS9 are done.

## References

- `.10x/specs/cli-interaction-excellence.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Journal

- 2026-07-26: Activated after WS8 published and installed `v0.2.0-alpha.1`. Existing WS1-WS9
  evidence is being reconciled without rerunning scale or hosted performance cells. The remaining
  direct execution proof is the current Chapter 23 deterministic terminal transcript; closure
  also inspects the renderer migration gate and registry/capability extension topology directly.
- 2026-07-26: Closed with `.10x/evidence/2026-07-26-p1-z1-program-closure.md`. The current
  Chapter 23 test passed and produced a redacted 150-line terminal session; all P1 parent criteria
  map to permanent evidence; hosted release, generated artifacts, renderer/progress performance,
  and architecture boundaries pass aggregate review.

## Evidence

- `.10x/evidence/2026-07-26-p1-z1-program-closure.md`
- `.10x/evidence/.storage/2026-07-26-p1-chapter23-terminal-session.txt`
- `CDF_DEMO_TRANSCRIPT_OUTPUT=... DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p
  cdf-conformance mvp_acceptance_demo --locked -j 12 -- --nocapture`: 1 passed, 0 failed.
- WS1-WS9 aggregate evidence named in the closure criterion matrix.

## Review

Pass. Direct source inspection confirms that the CLI renderer has no plain/raw output variant,
command modules are guarded against bypasses, source and destination additions enter explicit
composition registries, and generic destination orchestration dispatches on ingress capability
rather than adapter identity. Hosted and local recordings cover the terminal/channel/performance
matrix. No critical or significant finding remains.

## Retrospective

Aggregate closure should not repeat every child suite; it should join the strongest independent
observations and inspect the actual topology for the cross-cutting invariants children cannot
prove alone. The fresh Chapter 23 transcript provides the human-facing narrative while its
conformance assertions remain the correctness authority. Future distribution and product breadth
belong in the roadmap until explicitly activated, not as permanent open tickets.
