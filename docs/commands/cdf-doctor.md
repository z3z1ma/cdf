# `cdf doctor`

Generated from the CLI's clap definitions.

```text
Check host-dependent readiness without writing project, destination, package, receipt, checkpoint, or run authority. Bare doctor checks only the local runtime; doctor all is the only implicit whole-project probe.

Usage: cdf doctor [OPTIONS] [COMMAND]

Commands:
  runtime      Operate on cdf project evidence
  resource     Show one resolved resource
  source       Operate on cdf project evidence
  destination  Operate on cdf project evidence
  all          Operate on cdf project evidence

Options:
  -q, --quiet
          Suppress progress and non-primary success narration

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
```
