Status: open
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

## Blockers

None. Existing build-graph and source-egress records fully govern the repair.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
