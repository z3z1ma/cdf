Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md

# P0: repair discovery-to-pinned file-inventory identity

## Scope

Repair the external file-inventory authority introduced by `b918f3b8` so a cold discovery plan can hand its retained metadata inventory to the pinned/effective execution plan from the same compiler lifecycle. Inventory authority must bind the stable compiled-source discovery identity; final partition-task authority must continue binding the complete compiled execution-plan identity.

## Non-goals

- No weakening of external task/inventory validation.
- No re-listing fallback when retained inventory identity fails.
- No compatibility acceptance for the ambiguous one-hash runtime API.
- No change to package, destination, or schema epoch identity.

## Acceptance Criteria

- `FileResource` requires distinct discovery-binding and complete execution-plan hashes; no single ambiguous setter remains.
- `FileInventoryTaskAuthority` validates against the discovery-binding hash, while `FilePartitionTaskAuthority` remains bound to the complete compiled source-plan hash.
- A lifecycle regression proves unpinned discovery inventory is accepted after pin/effective-schema compilation without a second inventory pass.
- The P2 S1 CLI Parquet auto-pin/run smoke completes through the real project lifecycle.
- Focused source-file/project/CLI tests, strict Clippy, and formatting pass.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/decisions/fixed-schema-discovery-and-stream-admission.md`
- `.10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md`
- `.10x/tickets/done/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md`

## Assumptions

- Record-backed: `CompiledSourcePlan::discovery_binding_hash()` intentionally excludes the effective-schema mutation that distinguishes cold discovery compilation from the linked pinned execution plan.
- Record-backed: complete compiled source-plan identity remains required for executable partition tasks and is not interchangeable with discovery reuse authority.
- User-ratified: clearing state or repinning is not an acceptable workaround; this regression must be fixed in code and exercised through the real P2 lifecycle.

## Journal

- 2026-07-18: Activated after a real TLC auto-pin run failed before extraction. Static inspection attributed the regression to `b918f3b8`: the reusable inventory cache key uses the stable discovery-binding hash, but `FileInventoryTaskAuthority` records and validates the full compiled source-plan hash, which legitimately changes when the discovered schema is pinned. Existing unit fixtures used one synthetic identity on both sides and therefore could not falsify the lifecycle.
- 2026-07-18: Split `FileResource` identity binding into the stable source discovery-binding hash and the complete compiled execution-plan hash. `FileInventoryTaskAuthority` now serializes and validates only the discovery binding; `FilePartitionTaskAuthority` remains bound to the full plan plus request. Deleted the ambiguous one-hash setter and made the driver supply both identities in one construction call.
- 2026-07-18: Strengthened the real HTTP Parquet lifecycle regression: the cold and pinned source plans must share discovery identity while differing in complete identity, auto-pin performs one discovery lifecycle, planning consumes the retained inventory without a second transport inventory, and preview/execution use the resulting task authority.

## Blockers

None.

## Evidence

- `http_parquet_auto_pin_plan_preview_and_run_use_file_runtime` passed through cold discovery, pin/effective compilation, retained inventory reuse, external task planning, preview, and run. Its transport assertions prove no second inventory pass.
- `run_local_parquet_discover_autopins_and_commits_pinned_schema` passed through the real CLI project lifecycle and DuckDB commit; `plan_local_parquet_discover_autopins_snapshot_and_reports_hash` passed the write-bounded plan path.
- The file/runtime owner sweep passed 325/325 across `cdf-kernel`, `cdf-runtime`, `cdf-source-files`, `cdf-source-glue`, and `cdf-source-iceberg`; the three exact project/CLI lifecycle cases passed 3/3.
- Strict all-target Clippy passed for `cdf-source-files`, `cdf-project`, and `cdf-cli`; workspace formatting is clean.
- `rg` finds no `with_compiled_source_plan_hash`; both production construction sites call `with_compiled_source_identities(discovery_binding_hash, artifact_hash(plan))`.

## Review

Fresh-hat self-review traced the prepared-payload key, inventory artifact header, inventory reader validation, partition-task header, driver discovery session, resolved execution resource, and request binding. The stable identity is used only for metadata inventory reuse; executable tasks still require the full pinned plan and request. No fallback relisting or weakened hash validation was added. Verdict: pass. Residual risk from string category substitution was not accepted; `.10x/tickets/done/2026-07-18-p0-typed-compiled-source-identities.md` replaced these interim strings with distinct types.

## Retrospective

Cache/reuse identity and execution identity were both valid hashes but answered different questions. Naming them both “compiled source plan hash” allowed a lifecycle bug to compile and survive synthetic tests. The regression test became effective only when it asserted both the equality that must survive pinning and the inequality that must not. The next typed-identity ticket is the durable prevention mechanism.
