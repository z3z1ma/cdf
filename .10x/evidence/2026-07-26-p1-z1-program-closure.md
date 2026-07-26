Status: recorded
Created: 2026-07-26
Updated: 2026-07-26
Relates-To: .10x/tickets/done/2026-07-11-p1-z1-product-program-closeout.md, .10x/tickets/done/2026-07-08-p1-product-experience-program.md

# P1 Z1 product-experience closure evidence

## Observation

Every P1 workstream is terminal and the aggregate product surface is internally coherent:

- one typed event spine feeds durable evidence, tracing, and bounded live progress;
- one clap grammar owns parsing, generated help, completions, and man pages;
- one renderer owns human output, with machine JSON isolated;
- one typed catalog owns error codes and remediation;
- documentation and checked-in examples execute the product path;
- Python enters through the generic source/resource boundary;
- `v0.2.0-alpha.1` is a five-target checksummed hosted prerelease with a working public installer;
- the Chapter 23 crash/resume/replay/drift session is freshly recorded at current product code.

No P1 implementation or compatibility shim remains active.

## Aggregate criterion matrix

| P1 criterion | Permanent evidence | Result |
|---|---|---|
| TTY, headless, width, color, Unicode, and JSON modes | `.10x/evidence/2026-07-25-cli-experience-rewrite.md`; `.10x/evidence/2026-07-25-cli-hosted-conformance.md` | pass |
| Compact outcome-first normal output and full verbose/inspect evidence | same WS9 evidence and canonical recording archive | pass |
| Grammar shortest forms parse and resolve | `.10x/evidence/2026-07-10-p1-ws2-ws4-aggregate-closure.md` | pass |
| System-derived identifiers avoid unnecessary user-minted IDs | WS2 grammar semantics evidence aggregated above | pass |
| Run, replay, resume, and backfill progress | `.10x/evidence/2026-07-10-p1-event-progress-aggregate-closure.md` | pass |
| Stable error codes, remediation, suggestions, and generated catalog | `.10x/evidence/2026-07-10-p1-ws2-ws4-aggregate-closure.md` | pass |
| Python plan/preview/run/replay through the ordinary spine | `.10x/evidence/2026-07-10-p1-python-front-door-closure.md` | pass |
| Stranger-runnable quickstart and examples | `.10x/evidence/2026-07-10-p1-runnable-examples-docs-closure.md`; WS6A/WS6D evidence | pass |
| Checksummed hosted prerelease, installer, completions, and man pages | `.10x/evidence/2026-07-26-p1-ws8-hosted-release.md` | pass |
| Chapter 23 plan/contract/crash/resume/sql/history/replay/drift demo | `.10x/evidence/2026-07-08-mvp-acceptance-demo-fixture-harness.md`; fresh transcript below | pass |
| Rendering and progress overhead | hosted enabled/disabled median delta `-0.4407%` within variance; bounded subscriber microbenchmarks | pass |
| No P1 source/destination abstraction leak | direct topology inspection plus the source/destination invariant and DX conformance evidence | pass |

The hosted generated-artifact freshness job passed at release head `2928ee09`. The only later
changes before this closeout were `.10x/` records, so parser, generated help, completions, man
pages, error docs, and snapshots remain source-identical to that successful job.

## Fresh Chapter 23 recording

Command:

```text
CDF_DEMO_TRANSCRIPT_OUTPUT=/tmp/cdf-p1-z1-chapter23.txt \
DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 \
cargo test -p cdf-conformance mvp_acceptance_demo --locked -j 12 -- --nocapture
```

Observed: one test passed, zero failed. The current harness proved:

- plan before package bytes;
- contract freeze/test;
- a simulated crash after verified destination receipt and before checkpoint commit;
- resume with `source_contact: false`;
- accepted destination rows and local system SQL;
- committed checkpoint history backed by the receipt/package hash;
- replay into a second DuckDB destination;
- duplicate destination replay as a no-op;
- one accepted and one quarantined drift row with receipt-gated checkpointing.

The exact 150-line transcript is
`.10x/evidence/.storage/2026-07-26-p1-chapter23-terminal-session.txt`,
`sha256:f9bc757cc9a604f03aeea281e6463cfa3871f638f9bc86f7fba1915067385d82`.
It is byte-identical to the test output and contains no fixture secret, temporary directory, or
operator home path. Runtime timestamps and minted run IDs are observation facts, not package
identity or a byte-stable golden.

## Architecture and legacy-surface review

The final review inspected current source rather than inferring architecture from ticket status:

1. `crates/cdf-cli-core/src/output.rs` exposes only `HumanOutput::Rendered` and
   `RenderedWithProgress`; no plain/raw variant exists.
2. `renderer_migration_gate_rejects_raw_human_output_bypasses` scans every CLI command module and
   rejects `HumanOutput::Plain`, direct `CommandOutput` construction, and retired raw output
   shims. A repository search found `HumanOutput::Plain` only as the forbidden test pattern.
3. `crates/cdf-cli/src/source_registry.rs` is the first-party source composition root.
   `SourceRegistry` owns compile/resolve/discovery authority; generic project/runtime code consumes
   source traits and compiled plans.
4. `crates/cdf-cli/src/destination_registry.rs` is the first-party destination composition root.
   Generic orchestration branches on the closed capability sum
   `DestinationIngress::{FinalizedPackage, StagedSegments}`, not on DuckDB, Parquet, or Postgres
   identity.
5. The Quasar extension law and the Python source closure already falsified the most likely leak:
   adding a same-contract adapter does not require a shared runtime/command branch.

Concrete adapter imports in the CLI composition roots are deliberate registration, not
orchestration leakage. No old human-output path or P1-specific compatibility shim remains.

## Review

The adversarial pass tried to falsify closure through channel contamination, narrow terminals,
ANSI leakage, secret leakage, progress backpressure, generated-reference drift, source-specific
command branching, destination-specific runtime branching, published-artifact portability, and
state advancement before receipt verification.

All previously significant findings were repaired in their owning workstreams. The current
renderer migration gate, registry/capability topology, hosted recordings and performance cell,
published release, and freshly executed Chapter 23 session leave no critical or significant P1
finding. Verdict: pass.

## Residual risk and limits

- The hosted performance recording is one representative columnar/DuckDB workload; permanent
  renderer stress laws cover the terminal-specific adversaries.
- Only aarch64 macOS was installed after GitHub publication; all other targets executed and
  inspected natively in the hosted matrix.
- Notarization, package-manager channels, post-1.0 LTS selection, and future GitHub Action runtime
  upgrades are explicitly parked in
  `.10x/knowledge/active-backlog-and-future-roadmap.md`; they are not promises of this prerelease.
- Broader connector, streaming-supervisor, distributed, WASM, and format ambitions remain future
  programs, not P1 residuals.

## Retrospective

P1 became coherent only after the CLI was treated as one information architecture rather than a
sequence of command-specific cosmetics. The most reliable boundaries are executable negative
laws: command modules cannot bypass the renderer, progress cannot block execution, JSON cannot
share the human channel, and new adapters must enter registries/capabilities. Release portability
also required testing the published artifact, not merely the archive fixture. Future experience
work should extend these authorities instead of adding local presentation or adapter branches.
