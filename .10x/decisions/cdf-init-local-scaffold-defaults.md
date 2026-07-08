Status: active
Created: 2026-07-08
Updated: 2026-07-08

# cdf init local scaffold defaults

## Context

`VISION.md` lists `cdf init` as the command that scaffolds a project and one example resource per tier. `.10x/specs/project-cli-observability-security.md` requires the CLI command surface to include `init`, and requires `cdf.toml` to define project metadata, default environment, normalizer, environments, Python interpreter, defaults, and resource source mappings. `.10x/tickets/done/2026-07-07-cli-init-scaffold.md` owns the first executable `cdf init [DIR] [--name NAME] [--force]` slice and requires any non-obvious default shape to be ratified before implementation.

The full "one example resource per tier" scaffold would touch Python, WASM, subprocess, and external-resource examples that are not all equally complete in the current command surface. The smallest useful scaffold should validate immediately, contain no resolved secrets, and not create package, checkpoint, or destination runtime artifacts.

## Decision

The initial `cdf init` scaffold will create a minimal local development CDF project:

- `cdf.toml` with project name, `default_environment = "dev"`, `normalizer = "namecase-v1"`, a single `dev` environment, and a resource source mapping for `local.*`.
- `dev` environment defaults:
  - `state = "sqlite://.cdf/state.db"`
  - `packages = ".cdf/packages"`
  - `destination = "duckdb://.cdf/dev.duckdb"`
- `resources/files.toml` containing one declarative local file resource named `events` under source `local`, with NDJSON input rooted at `data`, primary key `id`, `append` disposition, `governed` trust, and a small explicit schema.
- `data/` as an empty input directory.

`cdf init` MUST NOT create `.cdf/`, package directories, checkpoint databases, destination databases, lockfiles, resolved secrets, or data files as part of this first scaffold. Those artifacts are created by explicit runtime commands or later lock/scaffold owners, not by initialization.

The default project name is the selected directory basename unless `--name NAME` is supplied. If the selected directory has no usable basename, the CLI may use `cdf_project`.

Existing scaffold paths MUST NOT be overwritten unless `--force` is supplied. With `--force`, the command may replace only files and directories it owns for this scaffold: `cdf.toml`, `resources/files.toml`, `resources/`, and `data/`. It must not remove `.cdf/`, package data, checkpoint state, destination data, lockfiles, or unrelated files.

## Alternatives considered

Create no example resource.

Rejected. It would produce a valid shell but not the book's intended fast local developer loop, and it would leave `cdf validate` unable to prove declarative resource resolution.

Create one example resource per authoring tier immediately.

Rejected for this first executable slice. Several tiers need broader runtime or security context, and adding placeholder examples would either fail validation or create misleading unsupported paths.

Create `.cdf/`, package, checkpoint, or DuckDB files during init.

Rejected. Initialization is a scaffold operation, not a runtime operation. Creating runtime artifacts would blur no-write CLI behavior and make overwrite semantics riskier.

## Consequences

The initial scaffold is intentionally local and conservative. It advances CLI/project ergonomics without ratifying hidden run defaults or secret behavior. Later scaffold expansion may supersede this decision or add separate templates for Python, WASM, subprocess, REST, SQL, warehouse, or production layouts.
