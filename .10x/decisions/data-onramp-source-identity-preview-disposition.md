Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Data onramp source identity, preview parity, and disposition policy

## Context

The P2 directive identifies product footguns around source identity, normalization, keys, and preview:

- The compiled resource id can be hard to infer from `[source.<name>]` and `[resources."pattern"]` mapping behavior.
- `namecase-v1` exists but does not run automatically at the live source/destination boundary.
- `append` currently nudges users toward fake keys in scaffolds and examples.
- Active preview semantics allowed file preview and run to diverge for multi-file globs.

This decision cites `VISION.md` sections 7.4, 8.2, 8.6, 13.3, 18, and 19.3. It supersedes `.10x/decisions/superseded/preview-one-batch-sampling-semantics.md`.

## Decision

The canonical compiled resource id is `<source>.<resource>`. Project mapping patterns match that id. New scaffolds and validators must render the compiled id and report mapping patterns that match zero compiled resources. Explicit id overrides are not a convenience path for new source definitions; compatibility handling must either require the explicit id to equal the canonical id or carry a migration warning under a bounded child ticket.

Destination identifiers are derived by the system. `namecase-v1` and the destination sheet's identifier rules run at plan time and feed the destination commit path. Source names are preserved as `cdf:source_name` metadata automatically. A TOML `source_name` entry is an ambiguity override, not a field-by-field requirement. Post-normalization collisions are plan-time errors with rename hints.

`append` is the default write disposition and requires no key. Scaffolds for append omit key fields. `merge` requires a non-empty merge identity and fails at plan time when missing, naming the two fixes: add `merge_key`, or use append. Key suggestions from `cdf add` are advisory and only appear when evidence from discovery supports them.

Preview/run parity replaces the older one-file preview exception. `cdf preview` remains no-write and bounded, but it must use the same resource resolution, file partition listing, decode path, discovery/snapshot selection, schema reconciliation, and normalization front end as `run`. A preview that succeeds must not hide a failure that the same bounded preview work could have detected. Preview does not need to prove unseen future rows or unexamined remote files are valid, but it must be honest about what was sampled and what was only planned.

Ad-hoc `cdf run <url-or-path> --to <dest>` is allowed only as evidence-preserving front-end sugar. It synthesizes a real resource under `.cdf/adhoc/`, pins discovery, plans, packages, commits, and gates through the same pipeline, then prints the corresponding `cdf add` command.

## Alternatives considered

Keep preview's deterministic first-file behavior while run rejects multi-file globs.

- Rejected because it made green preview untrustworthy and directly conflicts with the P2 S8 golden path.

Let destinations reject unnormalized identifiers.

- Rejected because identifier normalization is CDF identity policy, not a destination-side surprise.

Require keys for append.

- Rejected because append semantics do not require a key, and fake keys pollute configuration and guarantees.

## Consequences

The old preview decision is superseded. Historical tickets and evidence that reference it remain valid as history only.

CLI, project compilation, destination planning, and conformance must migrate together so identity, normalization, and preview/run parity do not split by command.

Any implementation ticket that changes preview, run, `cdf add`, or validation for source resources must cite this decision and the focused P2 specs.
