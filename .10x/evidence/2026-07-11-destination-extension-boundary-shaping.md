Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-destination-extension-boundary.md, .10x/decisions/destination-runtime-composition-boundary.md, .10x/specs/destination-extension-runtime-contract.md

# Destination extension boundary shaping evidence

## What was observed

The current generic settlement spine becomes destination-neutral only after resolution. Adding a fourth destination still requires concrete edits in `cdf-project` dependencies/runtime adapters and lock generation, CLI context/doctor/replay, and repeated conformance factories. The dependency direction prevents destination crates from owning the existing `cdf-project` trait implementations without a lower neutral crate.

## Procedure

Searched workspace Cargo dependencies, concrete `cdf_dest_*` imports, destination URI/scheme matches, built-in registry construction, resolved-destination conveniences, lockfile sheet construction, CLI doctor/replay models, and conformance destination factories. Read the owning implementations and compared the observed touch surface to the standing extension invariant and P3 streaming/bulk requirements.

## What this supports

The source inspection supports superseding the former permission for built-in adapters inside `cdf-project`, extracting `cdf-runtime`, moving adapters into destination crates, composing built-ins explicitly at the product boundary, and enforcing a data-driven conformance catalog. It also supports making P3 WS-A and WS-D depend on that boundary so performance capabilities do not create new destination-name branches.

## Limits

This is shaping evidence, not implementation or rebuild-graph evidence. DX1-DX4 must prove behavioral preservation, dependency inversion, external-driver extension, and build impact before the parent closes.
