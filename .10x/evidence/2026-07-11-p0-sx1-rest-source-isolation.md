Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# REST source ownership isolation

## What was observed

The REST physical plan, resource implementation, HTTP/pagination/retry execution, cursor pushdown, and bounded sample discovery now live in `cdf-source-rest`. The crate depends on neutral kernel/format/HTTP libraries and does not depend on declarative, project, CLI, destinations, or sibling sources.

`RestResource` is constructed from a neutral descriptor, schema, capabilities, plan, type allowances, and runtime dependencies. It performs its own partition planning and negotiation. The declarative crate contains only a compatibility bridge that translates an already compiled resource into those neutral inputs and delegates discovery.

## Procedure

- `cargo test -p cdf-source-rest --lib` — passed.
- declarative REST compatibility tests — passed.
- strict Clippy across REST source, declarative, project, and CLI targets — passed.

## What this supports

REST execution can evolve, optimize, and eventually register independently of the Tier-0 compiler. Concrete declarative compiler types no longer cross the source runtime boundary.

## Limits

The REST driver/options artifact and composition path remain to be implemented. The compatibility compiler still owns REST plan construction and the CLI still constructs REST runtime dependencies in a source-kind branch.
