Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md

# File neutral source driver and product path

## What was observed

`FileSourceDriver` validates canonical source/resource options for normalized root, glob, inferred/declared format, compression, auth/credential secret references, and egress policy. It emits the neutral plan with working-set/concurrency/pause/spill/retry/attestation/order declarations and resolves transport dependencies only through the CLI composition factory.

Effective schema evidence—including physical schema catalog entries and the discovery executor budget—is serializable in the compiled source plan and updated by schema reconciliation. File runtime resolution therefore retains multi-file discovery and attestation authority without a file-specific scheduler path.

CLI composition registers file, REST, and Postgres drivers. Generic runtime construction first resolves any neutral source plan; the local file validate/plan/preview/run product scenario traverses that path successfully.

## Procedure

- declarative file inference/neutral-plan capability law — passed.
- keyless append file validate/plan/preview/run CLI scenario — passed.
- neutral registry serialization/resolution mock law — passed.
- strict Clippy across kernel, runtime, all source adapters, declarative, and CLI targets — passed.

## What this supports

Every current first-party source now compiles into and executes from the same driver/version/hash/capability artifact. The scheduler and generic runtime no longer need source-specific construction for executable commands.

## Limits

Compatibility plans remain for discovery/add/deep/doctor and inspection call sites. Generic declarative parsing is still a closed enum, project discovery imports source implementations, and the source trait does not yet expose typed inspection/discovery/product hooks required to delete those branches.
