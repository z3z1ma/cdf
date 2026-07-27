Status: done
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Add environment errors and audit internal failures

## Scope

Add `ErrorKind::Environment`, catalog mappings, generated docs, and exact remediation. Audit the
foundational construction sites in kernel, memory, runtime, engine, package/package-contract,
task-store, object-access, and HTTP, reclassifying host/filesystem/process/resource-limit failures
without changing genuine invariant failures.

## Non-goals

- No change to existing stable exit codes; Environment uses 70 in this program.
- No blanket text replacement based on message keywords.
- No hiding program defects as environmental failures.

## Acceptance criteria

- `Environment` is serialized, cataloged, rendered, documented, redacted, and exhaustively
  matched.
- Every current internal-error site in the named foundational crates is classified by ownership;
  migrated sites carry relevant context/remediation.
- Missing current directory, temp directory, file descriptors, and local I/O have focused
  production-boundary tests; the catalog also renders a missing-executable Environment example.
- Poisoned invariants, impossible state, and internal serialization/authority failures remain
  `Internal`.
- Generated error reference and TTY/headless/JSON snapshots are fresh.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/specs/cli-error-experience-catalog.md`

## Assumptions

- Record-backed: stable exit codes are preserved; classification/remediation is the intended
  behavior change.

## Journal

- 2026-07-26: Source inventory counted 1,094 internal construction sites. This first bounded slice
  owns roughly 470 foundational sites; adapter and product slices have separate owners.
- 2026-07-26: Began execution from the kernel taxonomy and CLI catalog authorities. The audit will
  classify every `CdfError::internal` construction in the named crates by ownership, migrate only
  host/process/local-resource failures, and leave poisoned locks, impossible state, serialization,
  and authority mismatches internal. `graphify query` was unavailable because the executable is
  not installed.
- 2026-07-27: The first delegated review found that the original executable-test wording crossed
  the established foundation/adapter ticket seam: actual process spawning belongs to D1b's
  explicit subprocess scope. D1 retains the serialized/catalog/rendered missing-executable
  example; D1b owns production spawn classification and a nonexistent-executable invocation test.
- 2026-07-27: Audited the foundational ownership boundaries and migrated host current-directory,
  temp/workspace, permission, file-descriptor, physical I/O, and local resource failures to
  `Environment`. External artifact absence/truncation/shape remains `Data`; corrupt private CDF
  scratch and embedded counter/serialization invariants remain `Internal`.
- 2026-07-27: Review-driven repairs removed unsafe journal-free SQLite retry behavior, admitted
  complete record/tree growth before mutation, raised page ceilings before writes, and poisoned
  unexpected failures. Arrow/Parquet/JSON/manifest/archive wrappers now preserve typed source
  chains instead of stringifying the primary error.
- 2026-07-27: CLI mapping now renders `CDF-ENV-HOST` with exit 70 and identical JSON/headless/TTY
  facts. Redaction is recursive across message, remediation, details, suggestions, multiple URI
  userinfo occurrences, sensitive-key values, and private-key labels.
- 2026-07-27: The broader CLI gate exposed a separate C1 receipt-clock binding regression in
  direct schema promotion. It is outside D1 and now owned by
  `.10x/tickets/2026-07-27-prewave-c1b-promotion-receipt-clock-injection.md`.

## Blockers

None.

## Evidence

- Taxonomy, focused fault injection, product boundary, generated docs, static gates, and stated
  platform limits are recorded in
  `.10x/evidence/2026-07-27-foundational-error-ownership-audit.md`.
- Kernel 75, package 95/4 ignored, task-store 22/1 ignored, object-access 44/1 ignored, CLI-core
  45, engine 208/7 ignored, runtime 151/2 ignored, seven runtime build-graph tests, and one
  compile-fail doctest passed. Strict all-target/all-feature Clippy for the changed foundational
  scope passed.
- The generated command/error reference freshness check and `git diff --check` passed.
- `graphify update .` remains unavailable (`command not found`); no graph refresh is claimed.

## Review

Delegated OCR review ran repeatedly against deterministic file selection and project rules. Early
passes found and drove repairs for SQLite overflow-page admission and unsafe retry, scratch/data
shape ownership, production CLI current-directory paths, replay recovery roots, JSON and
Arrow/Parquet source-chain erasure, archive wrapper erasure, recursive redaction, private-key
labels, and embedded canonical writer failures.

After the final repairs, two independent reviewers returned `pass` with no findings. Residual risk
is limited to real platform EMFILE/ENOSPC/device behavior and platform-specific filesystem-loop
codes not exhaustively induced by the hermetic suite; the focused tests cover their production
classifiers and macOS no-follow behavior.

## Retrospective

- What broke: one name, “I/O error,” covered three different owners—external evidence, private CDF
  scratch, and the host. Stringifying codec and wrapper errors erased the only information needed
  to distinguish them. SQLite's journal-free scratch mode also made a conventional retry loop
  unsafe.
- What surprised: SQLite leaf free bytes do not prove a large `WITHOUT ROWID` record avoids
  overflow pages; the maximum local index payload is a separate bound. A logical spill
  reservation likewise does not prove physical disk capacity, so `SQLITE_FULL` is Internal only
  when the page ceiling itself is observed. Redacting the primary message was insufficient
  because details, suggestions, multiple URIs, and private-key values are independent output
  paths.
- What worked: fault-injected real production helpers, exact minimum-budget SQLite fixtures,
  source-chain tests, a full renderer matrix, and repeated adversarial review falsified polished
  but incomplete classifications. Keeping the taxonomy ownership-based made every repair local and
  predictable.
- Five whys: misclassification persisted because wrappers used `to_string`; constructors then
  chose a kind from the local operation name; tests asserted only returned text or a happy path;
  scratch and artifact readers shared generic helpers; and no durable ownership vocabulary
  existed. The repair preserves typed causes, separates boundary helpers, and records the
  vocabulary in `.10x/knowledge/error-ownership-taxonomy.md`.
- Distillation: the ownership rules and journal-free SQLite protocol belong in knowledge. No new
  skill is warranted: classification still requires semantic judgment, while the existing OCR
  delegation workflow already captures the repeatable review procedure.
