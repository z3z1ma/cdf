# `cdf run`

Generated from the CLI's clap definitions.

```text
Execute a governed resource run

Usage: cdf run [OPTIONS] [RESOURCE_SELECTOR]...

Arguments:
  [RESOURCE_SELECTOR]...  Exact or glob resource selectors; quote shell-sensitive globs

Options:
      --exclude <RESOURCE_GLOB>       Exclude resources matching this glob; may be repeated
  -q, --quiet                         Suppress progress and non-primary success narration
      --locked                        Require sufficient unchanged cdf.lock authority
  -v, --verbose...                    Show evidence detail; repeat for diagnostics
      --color <WHEN>                  Color policy: auto, always, or never [possible values: auto, always, never]
      --to <DEST>                     Destination URI or cursor upper bound, as shown in usage
      --jobs <N>                      Maximum concurrent jobs
      --progress <WHEN>               Progress policy: auto, always, or never [possible values: auto, always, never]
      --stats-profile                 Write the typed statistics profile artifact
      --unicode <WHEN>                Unicode policy: auto, always, or never [possible values: auto, always, never]
      --explain-memory                Include memory-ledger detail in the run report
      --memory-budget <BYTES>         Process memory budget, e.g. 4GiB or 512MiB
      --loop                          Continue polling for work
      --spill-budget <BYTES>          Spill/disk budget, e.g. 64GiB or 512MiB
      --segment-target-rows <ROWS>    Set the value named in this command's usage
      --segment-target-bytes <BYTES>  Set the value named in this command's usage
      --segment-max-rows <ROWS>       Set the value named in this command's usage
      --segment-max-bytes <BYTES>     Set the value named in this command's usage
      --microbatch-min-rows <ROWS>    Set the value named in this command's usage
      --microbatch-max-rows <ROWS>    Set the value named in this command's usage
      --microbatch-min-bytes <BYTES>  Set the value named in this command's usage
      --microbatch-max-bytes <BYTES>  Set the value named in this command's usage
  -h, --help                          Print help
```
