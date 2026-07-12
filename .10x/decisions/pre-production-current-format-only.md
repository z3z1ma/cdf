Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Pre-production CDF has one current format and no compatibility paths

## Context

CDF has no production users and no external artifact population to preserve. Compatibility aliases, old artifact readers, deprecated helpers, and slower fallback implementations therefore add branches, build cost, test cost, ambiguity, and hot-path risk without protecting a real customer. The user explicitly ratified that CDF's day-zero customer is the project itself and that no backward compatibility is required for old files, artifacts, APIs, or CLI forms before the first production release.

This decision supersedes pre-production compatibility requirements wherever they appear in `.10x/specs/versioning-lts-release-policy.md`, `.10x/decisions/cli-command-grammar-and-parser.md`, `.10x/decisions/spillable-package-order-dedup.md`, and destination-specific compatibility language. Protocol interoperability required to consume a currently supported external system is not legacy compatibility.

## Decision

Before the first production release, CDF writers and readers MUST implement exactly the current canonical artifact version. Old artifact versions and migrations MUST be deleted. CLI and Rust APIs MUST expose only current canonical forms; deprecated aliases and shims MUST be deleted. When a correct, faster implementation supersedes an internal or destination path, the old path MUST be removed once the new path's correctness and performance are proven.

A slower path MAY remain only when it serves a current, evidenced capability that the faster path cannot express exactly. It MUST be selected by a truthful capability descriptor and MUST NOT exist merely as a precautionary fallback. Unsupported current schemas or capabilities fail during planning with a precise remediation rather than silently selecting a legacy path.

Benchmarks may retain isolated test-only controls representing removed algorithms when needed for before/after evidence. Such controls cannot be linked into production code or advertised as capabilities.

The first production compatibility promise requires a new decision defining its release boundary and supported artifact/API surface. Nothing in this decision weakens compatibility with current external protocols such as PostgreSQL, Arrow, Parquet, Airbyte, or Singer.

## Alternatives considered

- Preserve compatibility until 1.0: rejected because there is no installed base and it taxes every execution and maintenance path now.
- Keep old paths behind feature flags: rejected because dormant code still expands the build, test, review, and security surfaces.
- Delete only measured hot-path fallbacks: rejected because stale artifact readers and CLI shims create the same architectural drag outside throughput profiles.

## Consequences

The codebase and published contracts become smaller and more legible. Development snapshots may stop reading artifacts or accepting invocations produced by earlier snapshots. Fixtures and tests must target the current canonical form. A repository-wide removal ticket owns existing vestiges so performance tickets can delete superseded local paths immediately without silently widening their scope.
