Status: done
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/done/2026-07-26-pre-wave-architecture-hardening-program.md`

# Extract the typed external task-set reader

## Scope

Move the duplicated Iceberg/Glue external-task authority retention, typed record decoding,
ordinal/content verification, parse-memory accounting, and retained executable-payload lifecycle
into one lower shared implementation. Migrate both sources.

## Non-goals

- No common catalog client, task schema, source position, retry policy, or partition planner.
- No merge of Iceberg and Glue source crates.
- No JSON/stringly task API.

## Acceptance criteria

- One typed shared reader owns authority/task bytes and parse leases exactly once.
- Iceberg and Glue inject their authority/task validation and partition-plan semantics through
  typed hooks or generics.
- Wrong task type/hash/ordinal/content, parse overflow, cancellation, and decode failure have
  shared fail-closed tests.
- Source-specific schema-observation, snapshot/generation, authorization, and retry semantics are
  unchanged and explicitly tested.
- Package hashes, task-set hashes, positions, jobs invariance, and measured source throughput do
  not regress.

## References

- `.10x/specs/catalog-task-source-commons.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Assumptions

- Source-backed: both task readers use `ExternalTaskSetReader`, accounted encoded/parse memory,
  typed JSON decode, ordinal/content validation, and retained payloads.

## Journal

- 2026-07-26: Direct diff confirmed the common skeleton and the source-specific semantics that
  must remain outside it.
- 2026-07-26: Activated after A3 closure. `graphify query "B1 typed task-set reader Iceberg Glue
  shared catalog task authority"` could not run because the `graphify` executable is unavailable;
  the implementation inventory therefore uses the two source readers, `cdf-task-store`, and Cargo
  dependency inspection directly.
- 2026-07-26: Added a typed `cdf-task-store` codec/reader boundary that owns shared authority and
  record bytes, parse leases, cancellation checks, authority/ordinal/content verification, and
  retained-byte accounting. Iceberg and Glue now supply typed decode/validation codecs and keep
  only their distinct partition-plan and executable semantics.
- 2026-07-26: The shared synthetic matrix covers wrong task-set type, authority identity,
  requested/model ordinal, task content identity, malformed decode, parse-reservation overflow,
  cancellation, and exact lease release. Full task-store, Iceberg, and Glue library suites pass.
- 2026-07-26: The first delegated review found two significant resource-semantics regressions:
  extraction had changed Iceberg's fail-fast discovery admission to blocking control admission,
  and executable-task clones shared the lease while deep-cloning the decoded model. The reader
  now takes explicit memory class/admission policy, and the entire retained decoded payload is
  one `Arc`-shared unit. New tests falsify both regressions.
- 2026-07-26: A five-sample same-machine comparison of the complete Iceberg library suite against
  detached baseline `5add468b` measured median wall time 1.89 s before and 1.91 s after (+1.1%),
  within ordinary local-run variance. The final delegated re-review passed.

## Blockers

None.

## Evidence

- Shared lifecycle and fail-closed matrix: `cargo test -p cdf-task-store --lib --locked --quiet`
  passed 11 tests with the pre-existing one ignored test. The four typed-reader tests prove exact
  authority/task encoded and parse-lease retention/release, clone pointer identity and singular
  accounting, type/hash/ordinal/content/decode/cancellation rejection, parse overflow, and
  fail-fast pressure behavior.
- Source semantics: `cargo test -p cdf-source-iceberg -p cdf-source-glue --lib --locked --quiet`
  passed all 41 Iceberg and 19 Glue tests. In particular, Iceberg's canonical jobs-invariant
  planning, append snapshot resume, schema projection, and snapshot execution tests remain green;
  Glue's retry/auth classification, governed partition scope, format override, and registered
  decoder tests remain green.
- Static quality: `cargo clippy -p cdf-task-store -p cdf-source-iceberg
  -p cdf-source-glue --all-targets --all-features --locked -- -D warnings`,
  `cargo fmt --all -- --check`, and `git diff --check` passed.
- Performance proxy: five cached debug executions of
  `cargo test -p cdf-source-iceberg --lib --locked --quiet` used the same machine and target cache.
  Baseline `5add468b` wall times were 1.90, 2.09, 1.88, 1.89, and 1.88 s (median 1.89);
  current wall times were 1.91, 1.91, 1.89, 1.91, and 1.94 s (median 1.91). The +1.1%
  movement is ordinary variance. Limit: this is a source-lifecycle latency proxy, not a release
  throughput roofline, and no credentialed remote catalog was contacted.
- `graphify update .` could not run because the executable is unavailable; all changed code,
  dependency edges, and references were inspected directly.

## Review

Delegated OCR review first returned `fail` with two significant findings: lost Iceberg
fail-fast/cancellation behavior and deep-cloned decoded payload memory hidden behind a shared
lease. Both were repaired and covered by explicit tests. Independent re-review returned `pass`
with no remaining concrete findings. Residual risk: Glue intentionally retains its prior
blocking control-memory reservation, whose cancellation checks bracket rather than interrupt an
in-flight wait; the tests do not claim otherwise. The local timing proxy cannot prove remote
catalog throughput.

## Retrospective

- What worked: a narrow typed codec centralized byte retention, decoding, and identity checks
  without pulling catalog, position, retry, schema, or partition semantics into the shared crate.
- What surprised us: retaining the same byte formula is insufficient when memory class and
  admission mode change; both are observable liveness policy. Likewise, clone semantics are part
  of accounting—a shared lease must cover the exact shared allocation, not an independently
  cloned typed model.
- Root cause: the first extraction modeled common data flow before making each adapter's resource
  policy and ownership topology explicit.
- Durable change: the resource-policy and whole-payload sharing invariants are now recorded in
  `.10x/knowledge/source-destination-extension-invariant.md`. No procedure recurred, so a new
  operational skill would add ceremony rather than prevent a demonstrated failure mode.
- Follow-up: none; the next bounded owner is B2's planning-workspace lifecycle.
