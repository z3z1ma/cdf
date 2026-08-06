# `cdf add`

Generated from the CLI's clap definitions.

```text
Add a source resource to the project

Usage: cdf add [OPTIONS] [RESOURCE_ID] [URL_OR_PATH]...

Arguments:
  [RESOURCE_ID] [URL_OR_PATH]...  Identifiers or paths shown in usage

Options:
      --dry-run                     Show the proposed change without writing it
  -q, --quiet                       Suppress progress and non-primary success narration
      --source <CONFIGURED_SOURCE>  Configured source name; defaults to the resource namespace
  -v, --verbose...                  Show evidence detail; repeat for diagnostics
      --color <WHEN>                Color policy: auto, always, or never [possible values: auto, always, never]
      --option <KEY=VALUE>          Source-driver option as KEY=VALUE; may be repeated
      --progress <WHEN>             Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>              Unicode policy: auto, always, or never [possible values: auto, always, never]
      --memory-budget <BYTES>       Process memory budget, e.g. 4GiB or 512MiB
      --spill-budget <BYTES>        Spill/disk budget, e.g. 64GiB or 512MiB
  -h, --help                        Print help
```
