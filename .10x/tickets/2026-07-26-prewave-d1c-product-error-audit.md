Status: active
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-d1b-adapter-error-audit.md`

# Audit product and governance environment errors

## Scope

Complete the internal-error semantic audit in project, CLI/CLI-core, state, contract,
declarative, conformance, benchmark, and other product/governance crates. Regenerate the complete
error catalog and prove no unaudited `CdfError::internal` owner remains.

## Non-goals

- No CLI visual redesign; D3 owns presentation after taxonomy/report authority.
- No change to domain-specific non-environment kinds or stable exit codes.
- No weakening of benchmark/conformance failures that identify CDF defects.

## Acceptance criteria

- Every remaining internal-error construction site is classified and the repository-wide audit
  has no unowned site.
- Project path/environment/state-store failures receive exact Environment remediation.
- Benchmark host/setup failures are distinct from measured product failures.
- Genuine configuration/contract, data, destination, transient, auth, and invariant failures keep
  their authoritative kinds.
- Generated docs, all error-kind exhaustiveness tests, and product error snapshots pass.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`
- `.10x/tickets/done/2026-07-26-prewave-d1b-adapter-error-audit.md`

## Assumptions

- Record-backed: D1/D1b establish the taxonomy and adapter semantics before this aggregate sweep.

## Journal

- 2026-07-26: Split from the original monolithic error ticket to give product/remediation
  semantics an independent closure review.
- 2026-07-27: Activated after D1b closure using the canonical
  `.10x/skills/audit-error-ownership/SKILL.md`. Froze a null-delimited 203-file manifest across
  the 16 remaining product/governance crate roots.
- 2026-07-27: Final inventory is 344 `CdfError::internal` sites across 64 files and 59 direct
  `ErrorKind::Internal`/`Internal` matches. Nine roots contain Internal constructors and seven are
  explicit zero-site roots. Review also reopened the shared `cdf-kernel::embedded_cdf_error`
  helper outside the frozen roots; that supporting repair added no Internal site and now preserves
  typed ownership through arbitrarily nested I/O wrappers.
- 2026-07-27: Classified configured paths separately from CDF-owned defaults, made managed
  filesystem reads no-follow and canonical-parent based, preserved local source redaction through
  read/copy races, and kept promotion-package artifacts Data-owned under their existing
  create-or-verify durable-recovery contract.
- 2026-07-27: Hardened SQLite component/schema/path admission and typed private-row decoding.
  Whole-history validation initially ran on ordinary opens; consolidated review rejected that
  unbounded default-path cost. Ordinary opens are now schema-only, typed reads validate consumed
  rows, and raw diagnostic/recovery readers invoke explicit `validate_integrity`.
- 2026-07-27: Immutable schema, discovery, promotion, lock, and project-file publication now uses
  synced temporary files, no-clobber installation, and directory synchronization through the
  durable root, including identical retries. A pre-existing process-loss window between
  `cdf.toml` and final `cdf.lock` publication is outside this error audit and is owned by
  `.10x/tickets/2026-07-27-cdf-add-crash-publication-recovery.md`.
- 2026-07-27: The consolidated frozen-snapshot review returned three significant findings:
  configured/default state ownership was inferred from path components, the bounded schema-streak
  query did not decode full checkpoint authority, and the exact inventory was only temporary.
  Repaired all three together by propagating explicit path ownership, decoding bounded streak rows
  through `row_to_checkpoint`, and adding a durable manifest and per-site ledger. The user
  requested an immediate quota-preserving checkpoint before the repair verification rerun.

## Blockers

None.

## Evidence

- `.10x/evidence/2026-07-27-product-error-ownership-audit.md` records the exact inventory,
  classifications, commands, results, and limits.
- Complete-site ownership: the pre-repair baseline is 203 frozen files, 344 Internal constructors
  in 64 files, and 59 inspected direct-kind matches. Durable storage is recorded in the evidence;
  final post-repair counts remain to be refreshed.
- Project path/environment/state-store remediation: focused path, symlink, configured/default
  state, private-row, duplicate-ID, read-only, and publication crash/race tests passed before the
  final repair. The post-repair all-target state suite reached 66 passes and one stale assertion;
  the assertion was corrected, and its rerun was interrupted for this immediate checkpoint.
- Benchmark host/setup distinction: the benchmark library and fixture suites pass 27/27 and 7/7,
  including direct, nested-I/O, and raw-host wrapper ownership.
- Kind authority and product snapshots: the CLI suite passes 296/296; focused conformance MVP,
  REST, registry, and nested destination regressions pass; the generated command/error reference
  is fresh.
- Quality: strict all-target/all-feature Clippy passes for every scoped root plus the supporting
  kernel helper; formatting and diff checks pass.
- Limits: the broad project gate retains one known intermittent HTTP concurrency timing failure;
  the broad conformance gate stalled in an existing chaos cell and was terminated with its spawned
  Postgres service stopped. `graphify` is unavailable. None is represented as passing evidence.

## Review

The consolidated final frozen-snapshot review reported three significant concerns and no critical
finding. Their repairs are present in this checkpoint. Bounded repair verification is still
pending; no further exploratory review is authorized for D1c.

## Retrospective

The semantic audit was materially broader than its ticket seam: 16 roots and hundreds of sites
coupled error ownership to filesystem trust, immutable publication, SQLite admission, CLI raw
readers, and wrapper provenance. Reviewing partial snapshots while those repairs were still moving
caused findings to arrive piecemeal and made pre-existing architecture risks look like D1c churn.

The most expensive dead end was solving filtered-read corruption by scanning all retained SQLite
history on every open. It was locally correct but imposed unbounded startup work. Separating schema
admission, typed row decoding, and explicit diagnostic integrity validation preserved the semantic
fence without taxing ordinary runs. A second dead end was relying on the generic error source
chain for nested `std::io::Error`; recursive `get_ref()` traversal is the actual shared boundary.

What worked was a frozen null-delimited site manifest, owner-first classification, no-follow
filesystem tests, failpoint-driven immutable-publication tests, and a single final review snapshot.
The five-whys lesson is procedural: findings were serial because the review target moved; it moved
because implementation and review were interleaved; they were interleaved because the ticket was
too broad to reason about as one batch; it became broad because taxonomy sites concealed several
independent trust boundaries; those boundaries should have been split before execution. The
canonical audit skill now requires an explicit supporting-boundary ledger and a frozen,
deduplicated closure review.
