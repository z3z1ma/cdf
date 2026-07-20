Status: done
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: None

# P0 runtime egress URI build-graph regression

## Scope

Restore the enforced `cdf-runtime` normal dependency ceiling after generic source-egress target parsing pulled the heavyweight `url`/ICU graph into the neutral runtime. Preserve exact credential-free authority normalization, IPv4/IPv6, generic schemes, default HTTP(S) ports, and fail-closed malformed/userinfo handling through a materially lighter existing dependency or minimal parser authority; delete the superseded dependency and any duplicate parsing path.

## Non-goals

- Weakening or raising the existing 67-package ceiling.
- Changing egress allowlist policy, source-driver APIs, retry/transport behavior, or accepted operational URI semantics.
- Moving parsing into a concrete source or copying parser logic across drivers.

## Acceptance criteria

- `cargo tree -p cdf-runtime -e normal` contains at most 67 unique packages and still excludes every implementation/codec named by the existing architecture test.
- Source egress targets retain canonical scheme/host/port behavior for HTTP(S), Postgres, object-store schemes, IPv4, bracketed IPv6, trailing-dot/case normalization, default ports, explicit ports, user information, query/fragment/path removal, and malformed/zero-port rejection.
- The replacement adds no source-specific branch, raw credential exposure, duplicate authority parser, compatibility shim, or measurable hot-path regression.
- The affected runtime/source/REST/file/Postgres source suites, graph gate, strict lint, and workspace check pass.

## References

- `.10x/specs/product-build-graph-boundaries.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/decisions/lean-cli-and-package-contract-build-boundaries.md`
- `.10x/decisions/source-driver-registry-and-resource-plan-boundary.md`
- `.10x/tickets/done/2026-07-12-p0-cg3-package-contract-leaf.md`

## Assumptions

- **Record-backed:** the <=67 normal-package ceiling is an active architecture acceptance criterion and test, not a stale advisory number.
- **Record-backed:** egress parsing belongs to the neutral injected source authority, not a concrete transport/source implementation.
- **Observed:** `cargo test -p cdf-runtime --all-targets --locked -j 12` on 2026-07-19 passed 87 runtime unit tests but failed `runtime_graph_excludes_package_implementation_and_codecs`: 85 unique normal packages versus the 67 ceiling. The printed excess is the `url` IDNA/ICU subtree introduced with `SourceEgressTarget` parsing after the last recorded 55-package baseline.

## Journal

- 2026-07-19 — Opened from H2 broad verification. H2 changed no dependencies; this is an earlier neutral-runtime graph regression that the permanent architecture gate correctly caught. Raising the ceiling is explicitly rejected because it would encode the leak rather than remove it.
- 2026-07-19 — Activated after H2 closure. Governing records confirm that URI normalization remains one neutral runtime authority, while implementation/client parsers stay in concrete source crates. The repair will first inventory the exact accepted syntax and dependency delta, then eliminate the heavyweight edge without adding a source-specific parser or relaxing the permanent graph gate.
- 2026-07-19 — Returned to the executable backlog before implementation when the user reprioritized the P3 critical chain (`WX1 -> C5` and `A7 -> A8 -> C5`). No source or dependency changes were made under this ticket; the graph regression remains accurately owned here.
- 2026-07-19 — Replaced the neutral runtime's `url` dependency with the already-locked `http` authority grammar plus one shared `ParsedSourceUri`. The parser normalizes credential-free scheme/host/port state once for egress, compiled-artifact validation, evidence redaction, and health-detail sanitization. It preserves operational DSN user information only long enough to discard it, rejects ambiguous user information and invalid/zero ports, canonicalizes IP literals and default ports, and treats hostless `file:///` as the one RFC file form rather than a transport branch.
- 2026-07-19 — Adversarial performance pass removed evidence rendering from the egress parse path and replaced whole-URI parsing with scheme/authority-only validation. A temporary release comparison (removed before commit) ran 250,000 parses per URI over HTTPS, Postgres IPv6+userinfo, S3, and HTTPS IPv4. Both implementations remained sub-microsecond (approximately 0.15-0.27 microseconds per parse); the high-frequency S3 and IPv4 cases improved by roughly 4-8%, ordinary HTTPS remained within a few nanoseconds/noise, and the slower IPv6 DSN case is connection-time rather than payload-path work. No retained benchmark shim or old parser remains.

## Blockers

None. Existing build-graph and source-egress records fully govern the repair.

## Evidence

- **Graph ceiling and isolation:** `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --test build_graph --locked -j 12` passed all 7 architecture laws. `cargo tree --locked -p cdf-runtime -e normal --prefix none | awk '{print $1}' | sort -u | wc -l` reports 58 unique packages, down from 85 and nine below the immutable 67-package ceiling; the normal graph contains `http v1.4.2` and no `url`, IDNA, or ICU package.
- **URI and affected source behavior:** `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime -p cdf-source-rest -p cdf-source-files -p cdf-source-postgres --all-targets --locked -j 12` passed: runtime 144 passed/2 ignored plus 7 graph laws, files 46 passed, Postgres 12 passed, REST 8 passed/1 performance test ignored. The permanent tests cover credentials, query/fragment/path removal, HTTP defaults, explicit ports, Postgres, S3/GS/Azure, IPv4, canonical bracketed IPv6, case/trailing dots, hostless file evidence, and malformed inputs.
- **Workspace integration:** `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets --locked -j 12` passed. `CARGO_BUILD_JOBS=12 cargo clippy --workspace --all-targets --locked -j 12 -- -D warnings` passed. `cargo fmt --all` and `git diff --check` passed.
- **Credential containment:** the recording-authorizer and evidence/health tests prove that user information, passwords, query values, and fragments never enter the egress request debug surface or recorded evidence. Compiled artifacts independently reject user information, queries, fragments, malformed network URIs, while retaining valid `file:///` locations.

## Review

Verdict: **pass**.

Fresh review traced every parser consumer and falsified the high-risk boundaries: a concrete source does not own parsing; egress receives only scheme/host/port; default ports match the previous URL semantics; path/query/fragment cannot affect allowlist identity; raw credentials are absent from both returned values and errors; malformed ports and ambiguous `@` forms fail closed; local file evidence remains valid. No critical, significant, minor, or nit finding remains. Residual risk is limited to wire-form URI spelling differences outside the recorded ASCII source URI grammar; concrete transports still own protocol-specific URL interpretation.

## Retrospective

The graph gate caught a conceptually correct abstraction implemented with the wrong dependency weight. The first replacement draft also rendered evidence during every egress authorization; measuring the tiny control path exposed that unnecessary allocation before it could become permanent. The durable technique is to separate one shared semantic parse from consumer-specific rendering while keeping protocol-client parsing in concrete drivers. Package-count gates and micro-comparisons are complementary: the former protects architecture, while the latter prevents a leaner build graph from hiding request-path overhead.
