Status: open
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/source-extension-runtime-contract.md

# P0 SX1: source driver registry and resource-plan boundary

## Scope

Implement neutral source driver/registry/plan/config schema contracts, extend capabilities, migrate file/REST/Postgres compilation/discovery/runtime/product hooks through drivers, extract Postgres source ownership from the destination crate, and prove a mock external source. Preserve existing TOML ergonomics/artifacts through an explicit migration.

## Acceptance criteria

- Generic declarative/project/CLI/discovery code contains no file/REST/SQL/Postgres source-kind match tree.
- Standard JSON Schema remains precise for common and driver fields; runtime parser is registry-open.
- Project/generic CLI have no `cdf-source-*` dependency/import; Postgres source is independent of its destination.
- Existing file/REST/Postgres add/discover/deep/preview/plan/run/doctor behavior passes compatibility/golden conformance.
- Extended capability declarations are live-falsified and sufficient for P3 scheduler admission without source ids.
- A mock source adds only driver crate/test, composition/schema catalog, and fixture entries and inherits every applicable law.
- Cargo graph/rebuild evidence proves source dependency isolation.

## Evidence expectations

Config/schema migration goldens, dependency/static tests, mock source, first-party parity matrix, discovery/package artifact hashes, redaction/egress/retry/memory/jobs tests, build graph evidence, and adversarial extension review.

## Explicit exclusions

No new source protocol, parser optimization, dynamic ABI, or distributed execution.

## Blockers

Depends on neutral runtime, memory, and execution-host contracts. P3 source scheduling and remote overlap must use this boundary.

## References

- `.10x/decisions/source-driver-registry-and-resource-plan-boundary.md`
- `.10x/research/2026-07-11-source-extension-boundary-audit.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Progress and notes

- 2026-07-11: Added the engine-neutral `SourceDriver`, `SourceRegistry`, compiled source plan, resolution context, and scheduler-facing execution capability contracts to `cdf-runtime`. Compiled plans bind driver/version/option-schema authority and canonical redacted-option/physical-plan hashes; registry resolution rejects authority drift. A mock driver proves deterministic registration, compilation, serialization, and resolution without source-id scheduling branches. First-party source migration and dependency isolation remain open.
- 2026-07-11: Extracted Postgres table scan and catalog discovery into dependency-isolated `cdf-source-postgres`; the destination no longer owns or exports source behavior. Shared, protocol-level identifier/target validation now lives in leaf crate `cdf-postgres`, used independently by source and destination. Declarative and project discovery currently consume the source crate directly pending registry-driven composition.
- 2026-07-11: Implemented the first first-party `SourceDriver` in `cdf-source-postgres`. It strictly compiles driver-owned options, preserves only secret references in the plan, declares an installable bounded blocking lane and retry/attestation/working-set semantics, resolves secrets through injected context, and executes scans through the host lane. The neutral registry now installs source-declared lanes generically before resolution.
- 2026-07-11: Declarative Postgres table compilation now stamps the neutral driver plan onto `CompiledResource`, updates that plan when discovery/effective schema changes, and fails invalid dialect/table options at compile time. CLI plan/preview/run resolution uses the single source composition module and neutral registry whenever execution services are present; the legacy SQL wrapper remains only for compatibility/inspection tests while the remaining drivers migrate.
- 2026-07-11: Extracted the REST plan, HTTP/pagination execution engine, discovery sampler, and resource implementation into dependency-isolated `cdf-source-rest`. The source resource now owns only neutral descriptor/schema/capability/type-policy inputs rather than a declarative compiler object; `cdf-declarative` retains a thin construction/discovery compatibility bridge while driver compilation is added.
- 2026-07-11: Added `RestSourceDriver` with strict driver-owned source/resource options, canonical physical/redacted artifacts, secret-ref validation, transport-factory composition, complete source execution declarations, and managed host-lane execution. Declarative REST resources now carry the neutral plan; CLI resolves every available neutral source plan before entering compatibility dispatch. Resolution context owns a shared secret provider, and neutral plans now carry type-policy allowances so drivers cannot silently drop schema policy.
- 2026-07-11: Extracted file format/compression declarations, file/object-store transport facade, ranged readers, partition resolution, attestation, and runtime execution into dependency-isolated `cdf-source-files`. `FileResource` now consumes neutral descriptor/schema/capability/type-policy/effective-schema inputs; declarative retains only a thin compatibility constructor while the driver artifact is added.
- 2026-07-11: Added `FileSourceDriver` with strict canonical option/physical artifacts, source composition transport factory, complete scheduler capabilities, and neutral resolution for local/HTTP/S3/GCS/Azure schemes. Effective schema runtime/catalog/budget evidence is now serializable source-plan authority and updates after reconciliation. CLI composition registers all three first-party drivers and production file/REST/Postgres execution resolves uniformly before compatibility dispatch.
- 2026-07-11: Replaced the nine-argument compiled-plan constructor with a cohesive `CompiledSourcePlanInput` artifact. Adding a source now supplies one payload plus generic resource/execution capabilities, avoiding positional wiring and keeping strict Clippy clean as the extension surface grows.
- 2026-07-11: Deleted source-kind runtime construction from generic CLI execution. Every executable declarative resource must carry a neutral source plan and execution services; registry resolution is the only file/REST/Postgres runtime path. Inspection uses the neutral compiled resource surface without resolving secrets or constructing transports. Source-specific file dependencies remain only in discovery/deep/doctor compatibility hooks pending hook migration.
- 2026-07-11: Removed the direct Postgres source implementation dependency/import from `cdf-project`; catalog compatibility exports are temporarily mediated by declarative while discovery hooks move to the registry.
- 2026-07-11: Migrated the canonical live local-file conformance fixture from executable `CompiledResource` compatibility behavior to a neutral `FileSourceDriver` plan resolved through `SourceRegistry`, injected execution services, shared secret authority, and the file transport facade. The fixture now plans and runs against the resolved `QueryableResource`; the old anonymous `seg-000001` golden was promoted to the current deterministic `p00000000-s00000000` artifact rather than preserved through a shim. The DuckDB live golden passes across 100 rebuilds under strict Clippy. This is a file-fixture milestone, not SX1 closure; REST/Postgres/general conformance catalog and remaining compatibility hooks are still open. Evidence: `.10x/evidence/2026-07-11-p0-sx1-live-file-registry-golden.md`.
- 2026-07-11: Hardened the neutral transport side of source extension: both file and REST transport contracts now require shared `Send + Sync` implementations and no longer impose adapter-wide mutable mutexes. Generic project discovery, CLI commands, benchmarks, and conformance consume the shared HTTP contract without source-id branches. Concrete async provider extraction and remaining compiler/discovery hook migration remain open. Evidence: `.10x/evidence/2026-07-11-p3-g1-concurrent-rest-transport.md` and `.10x/evidence/2026-07-11-p3-g2-concurrent-transport-spool.md`.
- 2026-07-12: Extracted concrete Reqwest ownership from CLI into `cdf-transport-http`; CLI now only composes the provider. File runtime consumes its optional neutral byte source through the transport hook, and source access is joined from format capabilities rather than source/format ids. Project/CLI discovery hooks and the blocking REST compatibility surface still block SX1 closure. Evidence/review: `.10x/evidence/2026-07-12-p3-g1-async-http-byte-source.md`, `.10x/reviews/2026-07-12-p3-g1-async-http-byte-source-review.md`.
