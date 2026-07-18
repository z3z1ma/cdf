# `cdf run`

Generated from the CLI's clap definitions.

```text
Execute a governed resource run

Usage: cdf run [OPTIONS] [RESOURCE]...

Arguments:
  [RESOURCE]...  Resource identifier

Options:
  -q, --quiet                  Suppress progress and non-primary success narration
      --to <DEST>              Destination URI or cursor upper bound, as shown in usage
      --jobs <N>               Maximum concurrent jobs
  -v, --verbose...             Show evidence detail; repeat for diagnostics
      --color <WHEN>           Color policy: auto, always, or never [possible values: auto, always, never]
      --stats-profile          Write the typed statistics profile artifact
      --loop                   Continue polling for work
      --progress <WHEN>        Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>         Unicode policy: auto, always, or never [possible values: auto, always, never]
      --memory-budget <BYTES>  Process memory budget, e.g. 4GiB or 512MiB
      --spill-budget <BYTES>   Spill/disk budget, e.g. 64GiB or 512MiB
  -h, --help                   Print help
```
