Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# REST neutral source driver and product path

## What was observed

REST source/resource options are validated and converted by `RestSourceDriver`, including auth secret references, egress allowlists, scalar parameters, pagination variants, quota headers, record selectors/transforms, and cursor fidelity. The compiled source plan records canonical redacted and physical artifacts plus type-policy allowances.

The source driver accepts an HTTP transport factory at composition. Resolution owns the shared secret provider and execution services, installs the declared bounded `rest-source.sync` lane generically, and returns a neutral queryable resource. The CLI tries neutral source-plan resolution before its remaining compatibility dispatch, so REST and Postgres share the same production construction path without scheduler/source-id branches.

## Procedure

- declarative REST plan/capability/type-policy compilation law — passed.
- REST discover/pin/plan/preview/run CLI product scenario — passed.
- neutral registry mock round-trip — passed after shared-secret/type-policy expansion.
- strict Clippy across runtime, Postgres/REST sources, declarative, and CLI targets — passed.

## What this supports

REST product execution now receives driver validation, secret handling, lane admission, discovery-updated schemas, and type policy through one neutral artifact and registry boundary.

## Limits

REST discovery/add/doctor still delegate through compatibility project/CLI hooks rather than driver hooks. The old REST runtime branch remains reachable only when no execution services are supplied and will be removed after inspection hooks migrate. File sources remain the final first-party runtime migration.
