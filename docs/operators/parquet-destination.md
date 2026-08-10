# Parquet destination

CDF writes finalized packages to a local filesystem object tree through the
built-in `parquet` destination. The destination URI names a root or prefix, not
one Parquet file; a commit may publish several data objects plus manifests,
pointers, and receipt evidence below it. Relative roots resolve beneath the
selected project root, and absolute roots are also accepted.

## Configure the destination

Use the URI in an environment destination:

```toml
[environments.dev]
destination = "parquet://.cdf/parquet-output"
```

The full current syntax is:

```text
parquet://<root>[?compression=<codec>&object_target_bytes=<size>&max_segments_per_object=<count>]
```

Options are optional, order-independent, and may each appear at most once.
Only these options are accepted:

| Option | Default | Accepted values | Controls |
| --- | --- | --- | --- |
| `compression` | `zstd` | `none`, `snappy`, `lz4`, or `zstd` | The Parquet column-chunk compression codec. CDF uses Zstandard level 1 for `zstd`. |
| `object_target_bytes` | `256MiB` | A positive integer byte size, optionally suffixed with `B`, `KiB`, `MiB`, `GiB`, or `TiB` | The maximum sum of canonical package-segment bytes normally grouped into one Parquet object. |
| `max_segments_per_object` | `8` | A positive decimal integer no greater than 65,535 | The maximum number of consecutive package segments normally grouped into one Parquet object. |

For example:

```toml
[environments.dev]
destination = "parquet://.cdf/parquet-output?compression=snappy&object_target_bytes=512MiB&max_segments_per_object=16"
```

```bash
cdf run events --to 'parquet:///var/lib/cdf/events?compression=lz4&object_target_bytes=1GiB&max_segments_per_object=12'
```

An empty root, a fragment, credentials or a nested URI in the root, an unknown
or duplicate option, an unsupported codec, zero, overflow, and malformed
`key=value` syntax are contract errors. CDF rejects them before destination
mutation and does not include supplied option values or credentials in the new
diagnostics.

## Object layout and writer admission

CDF groups consecutive package segments deterministically and closes the
current object before adding a segment that would exceed either configured
bound. A segment larger than `object_target_bytes` is still written, alone, as
an oversized singleton; it does not weaken the bounds for later objects.

`object_target_bytes` measures canonical package input bytes used by the
grouping plan. It is not an exact target for compressed Parquet file size and
does not configure Parquet row groups, data pages, or destination batch sizes.
The destination bulk path separately selects bounded row and byte batch sizes
for encoding.

Object grouping does not set writer concurrency. For each run, CDF admits the
minimum supported writer count across the effective host logical CPU slots,
the run's jobs ceiling, accounted per-writer memory, and the adapter's safe
maximum. This keeps parallel encoding inside the shared execution and memory
authorities instead of treating one machine-specific concurrency value as a
universal optimum.

## Recorded authority and replay

CDF records the resolved compression and object-layout policy in the prepared
physical plan and reads it back exactly before payload mutation. Publication
metadata, immutable manifests, and receipt verification preserve the same
authority, including deterministic object membership and ordinals.

These settings choose a physical representation; they do not change canonical
CDF package identity or logical row meaning. When the same finalized package
is replayed after ambient defaults change, the first verified immutable
publication remains authoritative. Replay verifies and returns that recorded
receipt rather than reinterpreting the package with current defaults.

## Related contracts

- [Package delivery](package-delivery.md)
- [Recovery](recovery.md)
- [Destination bulk-path runtime](../../.10x/specs/destination-bulk-path-runtime.md)
- [Destination introspection and replay policy](../../.10x/decisions/destination-introspection-package-and-current-replay-policy.md)
