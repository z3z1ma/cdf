Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Built-in driver catalog composition

## Context

The standard product currently constructs source, format, transform, and destination registries
inside `cdf-cli`. This preserves one explicit composition root, but it makes the production
binary crate own approximately two hundred lines of concrete adapter construction and prevents
non-CLI first-party tools from consuming the exact shipped catalog without importing CLI code.
The next source and destination wave would expand that fan-out.

The current source also corrects two overstatements from the pre-wave audit:

- `cdf-project` has no production dependency on a concrete source or destination crate; its
  concrete adapters are test-only dependencies.
- `cdf-cli-core` has no concrete adapter dependency. A few DuckDB/Parquet strings are help,
  remediation, redaction, and rendering fixtures. They are product examples, not composition
  logic.

## Decision

Create a leaf `cdf-builtin-drivers` crate as the single construction authority for the complete
first-party source, format, transform, and destination catalogs. It may depend on concrete
first-party adapters and the transport implementations required to construct them. It MUST expose
typed catalog construction/access, not a service locator, linker inventory, plugin ABI, or
destination/source-name dispatch.

`cdf-cli` remains the application composition root and production binary. It installs the catalog
from `cdf-builtin-drivers` and owns CLI-only inspection/rendering context. The new crate therefore
delegates catalog construction; it does not create a second product or a second semantic registry.

First-party tests, benchmarks, and conformance code that need the shipped catalog MUST consume the
same leaf rather than recreate the adapter list. Adapter-unit tests MAY construct one driver
directly. `cdf-project` production code remains concrete-adapter-free; its tests SHOULD use
neutral fixtures or the built-in catalog instead of carrying a second concrete dependency list.

Adding a first-party adapter changes its implementation crate, `cdf-builtin-drivers`, and its
data-driven conformance fixture. It MUST NOT require edits to generic CLI command modules,
`cdf-project`, `cdf-runtime`, `cdf-cli-core`, or the conformance engine.

Concrete first-party names MAY remain in dependency-light help text, examples, error remediation,
and rendering fixtures. Static graph tests, not string bans, enforce dependency isolation.

## Alternatives considered

- Keep construction in `cdf-cli`: coherent for one binary, but already prevents reuse by
  benchmarks/conformance and makes the incoming catalog wave inflate the product crate.
- Linker or inventory registration: rejected because hidden registration harms deterministic
  embedding, auditability, and catalog identity.
- Move catalogs into `cdf-runtime`: rejected because the neutral runtime must not depend on
  concrete adapters.
- Remove every concrete name from `cdf-cli-core`: rejected because product examples are useful
  operator documentation and do not create dependency or control-flow coupling.

## Consequences

This decision activates the previously deferred leaf from
`.10x/decisions/destination-runtime-composition-boundary.md`. It refines, rather than supersedes,
`.10x/decisions/lean-cli-and-package-contract-build-boundaries.md`: `cdf-cli` remains the complete
binary composition root while catalog construction moves to one reusable dependency leaf.

The leaf intentionally has a large concrete dependency graph. It is never a dependency of
`cdf-cli-core`, kernel, runtime, project, package-contract, or individual adapters.
