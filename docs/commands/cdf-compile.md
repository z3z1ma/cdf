# `cdf compile`

Generated from the CLI's clap definitions.

```text
Compile selected resources independently. Ordinary compile may establish missing first-use schema authority; --locked requires existing unchanged state-backed schema authority. Successful resources are retained when another resource fails.

Usage: cdf compile [OPTIONS] [RESOURCE_SELECTOR]...

Arguments:
  [RESOURCE_SELECTOR]...
          Exact or glob resource selectors; quote shell-sensitive globs

Options:
      --exclude <RESOURCE_GLOB>
          Exclude resources matching this glob; may be repeated

  -q, --quiet
          Suppress progress and non-primary success narration

      --locked
          Require existing unchanged state-backed schema authority

  -v, --verbose...
          Show evidence detail; repeat for diagnostics

      --color <WHEN>
          Color policy: auto, always, or never

          [possible values: auto, always, never]

      --progress <WHEN>
          Progress policy: auto, always, or never

          [possible values: auto, always, never]

      --unicode <WHEN>
          Unicode policy: auto, always, or never

          [possible values: auto, always, never]

      --memory-budget <BYTES>
          Process memory budget, e.g. 4GiB or 512MiB

      --spill-budget <BYTES>
          Spill/disk budget, e.g. 64GiB or 512MiB

  -h, --help
          Print help (see a summary with '-h')

Examples:
  cdf compile local.events
  cdf compile 'warehouse.*' --exclude warehouse.experimental
  cdf compile --locked
```
