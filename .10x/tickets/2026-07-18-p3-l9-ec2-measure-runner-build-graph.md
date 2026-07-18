Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md, .10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md

# P3 L9: slim EC2 measured-command runner build graph

## Scope

Split the EC2 `measure-cdf` path away from the heavyweight all-in-one `cdf-p3-lab` reference binary so ordinary CDF command measurements do not relink every lab reference workload after unrelated benchmark diagnostics. The retained path must still emit the same host-labeled `run-cell` observation schema, supervise nested `cdf` children, enforce timeouts, preserve fresh-workspace defaults, and remain compatible with L6/L7 preflight.

## Non-goals

- No weakening of release optimization for the measured `cdf` binary.
- No removal of `cdf-p3-lab` reference workloads, baseline-run, package probes, or envelope tooling.
- No benchmark result schema churn unless the old schema is intentionally replaced everywhere in one change.
- No source/destination dataplane optimization.

## Acceptance Criteria

- A `measure-cdf` invocation can use a smaller release binary that does not link `references.rs` or first-party destination/reference workload diagnostics.
- The EC2 helper builds/verifies the measured-command runner as a first-class release artifact and records it in the build marker or an equivalent preflight-checked marker.
- A cached source-only record commit does not force a heavyweight relink, and a measured-command-only change has materially lower on-host release build time than the current `cdf-p3-lab` `8m35s`–`8m39s` relink class.
- `measure-cdf` output for a tiny fixture and the full-year TLC prepared workspace remains schema-valid and comparable to existing `cdf-p3-lab run-cell` output.
- Existing `cdf-p3-lab` reference, baseline, package-shape/read, and compare commands continue to build and pass focused tests.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md`
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`

## Assumptions

- Record-backed: L6/L7 repeatedly measured `cdf-p3-lab` release relinks at about `8m35s`–`8m39s` when benchmark diagnostics touch the lab/reference module graph.
- Record-backed: `measure-cdf` needs `cdf_command`, host fingerprinting, macro-cell observation, and canonical JSON output; it does not need DuckDB/Parquet reference workloads.

## Journal

- 2026-07-18: Opened after the DuckDB stream-scan tranche confirmed the benchmark host is reliable but iteration is still throttled by the monolithic `cdf-p3-lab` build graph. The current helper builds both release `cdf` and release `cdf-p3-lab` for every tranche refresh; `cdf` is often a cache hit while lab relink burns one CPU for ~8.5 minutes. This ticket owns the build-graph cut rather than letting each G4 diagnostic pay that tax.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
