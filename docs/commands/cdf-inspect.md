# `cdf inspect`

Generated from the CLI's clap definitions.

```text
Inspect durable project and run evidence

Usage: cdf inspect [OPTIONS] [COMMAND]

Commands:
  project       Show resolved project information
  resources     List project resources
  resource      Show one resolved resource
  destinations  List resolved destinations
  package       Show durable package evidence
  run           Show durable run evidence

Options:
  -q, --quiet                  Suppress progress and non-primary success narration
  -v, --verbose...             Show evidence detail; repeat for diagnostics
      --color <WHEN>           Color policy: auto, always, or never [possible values: auto, always, never]
      --progress <WHEN>        Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>         Unicode policy: auto, always, or never [possible values: auto, always, never]
      --memory-budget <BYTES>  Process memory budget, e.g. 4GiB or 512MiB
      --spill-budget <BYTES>   Spill/disk budget, e.g. 64GiB or 512MiB
  -h, --help                   Print help
```
