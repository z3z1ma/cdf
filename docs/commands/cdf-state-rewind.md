# `cdf state rewind`

Generated from the CLI's clap definitions.

```text
Create a marker that rewinds checkpoint state

Usage: cdf state rewind [OPTIONS] [RESOURCE]...

Arguments:
  [RESOURCE]...  Resource identifier

Options:
      --pipeline <ID>          Pipeline identifier
  -q, --quiet                  Suppress progress and non-primary success narration
      --scope <KEY=VALUE>      Checkpoint scope entry as key=value; may be repeated
  -v, --verbose...             Show evidence detail; repeat for diagnostics
      --color <WHEN>           Color policy: auto, always, or never [possible values: auto, always, never]
      --scope-json <JSON>      Checkpoint scope encoded as JSON
      --progress <WHEN>        Progress policy: auto, always, or never [possible values: auto, always, never]
      --to <CHECKPOINT>        Destination URI or cursor upper bound, as shown in usage
      --unicode <WHEN>         Unicode policy: auto, always, or never [possible values: auto, always, never]
      --memory-budget <BYTES>  Process memory budget, e.g. 4GiB or 512MiB
      --spill-budget <BYTES>   Spill/disk budget, e.g. 64GiB or 512MiB
  -h, --help                   Print help
```
