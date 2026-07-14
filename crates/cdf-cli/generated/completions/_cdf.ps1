
using namespace System.Management.Automation
using namespace System.Management.Automation.Language

Register-ArgumentCompleter -Native -CommandName 'cdf' -ScriptBlock {
    param($wordToComplete, $commandAst, $cursorPosition)

    $commandElements = $commandAst.CommandElements
    $command = @(
        'cdf'
        for ($i = 1; $i -lt $commandElements.Count; $i++) {
            $element = $commandElements[$i]
            if ($element -isnot [StringConstantExpressionAst] -or
                $element.StringConstantType -ne [StringConstantType]::BareWord -or
                $element.Value.StartsWith('-') -or
                $element.Value -eq $wordToComplete) {
                break
        }
        $element.Value
    }) -join ';'

    $completions = @(switch ($command) {
        'cdf' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help (see more with ''--help'')')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help (see more with ''--help'')')
            [CompletionResult]::new('-V', '-V ', [CompletionResultType]::ParameterName, 'Print version')
            [CompletionResult]::new('--version', '--version', [CompletionResultType]::ParameterName, 'Print version')
            [CompletionResult]::new('help', 'help', [CompletionResultType]::ParameterValue, 'Show help for a command')
            [CompletionResult]::new('version', 'version', [CompletionResultType]::ParameterValue, 'Print the cdf version')
            [CompletionResult]::new('init', 'init', [CompletionResultType]::ParameterValue, 'Create a new cdf project')
            [CompletionResult]::new('add', 'add', [CompletionResultType]::ParameterValue, 'Add a source resource to the project')
            [CompletionResult]::new('validate', 'validate', [CompletionResultType]::ParameterValue, 'Validate project configuration and contracts')
            [CompletionResult]::new('plan', 'plan', [CompletionResultType]::ParameterValue, 'Plan a resource run without executing it')
            [CompletionResult]::new('explain', 'explain', [CompletionResultType]::ParameterValue, 'Explain resolution, capabilities, and execution choices')
            [CompletionResult]::new('run', 'run', [CompletionResultType]::ParameterValue, 'Execute a governed resource run')
            [CompletionResult]::new('preview', 'preview', [CompletionResultType]::ParameterValue, 'Read a bounded preview without committing data')
            [CompletionResult]::new('sql', 'sql', [CompletionResultType]::ParameterValue, 'Query cdf system metadata')
            [CompletionResult]::new('inspect', 'inspect', [CompletionResultType]::ParameterValue, 'Inspect durable project and run evidence')
            [CompletionResult]::new('diff', 'diff', [CompletionResultType]::ParameterValue, 'Compare durable schemas')
            [CompletionResult]::new('schema', 'schema', [CompletionResultType]::ParameterValue, 'Discover, pin, compare, and promote schemas')
            [CompletionResult]::new('contract', 'contract', [CompletionResultType]::ParameterValue, 'Freeze, show, and test contracts')
            [CompletionResult]::new('state', 'state', [CompletionResultType]::ParameterValue, 'Inspect and recover checkpoint state')
            [CompletionResult]::new('resume', 'resume', [CompletionResultType]::ParameterValue, 'Resume interrupted work from the run ledger')
            [CompletionResult]::new('replay', 'replay', [CompletionResultType]::ParameterValue, 'Replay a verified package')
            [CompletionResult]::new('backfill', 'backfill', [CompletionResultType]::ParameterValue, 'Plan or execute a bounded cursor backfill')
            [CompletionResult]::new('package', 'package', [CompletionResultType]::ParameterValue, 'List, verify, archive, and collect packages')
            [CompletionResult]::new('doctor', 'doctor', [CompletionResultType]::ParameterValue, 'Check local runtime and destination health')
            [CompletionResult]::new('status', 'status', [CompletionResultType]::ParameterValue, 'Summarize project freshness and run state')
            break
        }
        'cdf;help' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;version' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;init' {
            [CompletionResult]::new('--name', '--name', [CompletionResultType]::ParameterName, 'Project name')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--force', '--force', [CompletionResultType]::ParameterName, 'Replace an existing artifact when safe')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;add' {
            [CompletionResult]::new('--records', '--records', [CompletionResultType]::ParameterName, 'Record selector within the source')
            [CompletionResult]::new('--cursor', '--cursor', [CompletionResultType]::ParameterName, 'Cursor field')
            [CompletionResult]::new('--cursor-param', '--cursor-param', [CompletionResultType]::ParameterName, 'Request parameter carrying the cursor')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--dry-run', '--dry-run', [CompletionResultType]::ParameterName, 'Show the proposed change without writing it')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;validate' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--deep', '--deep', [CompletionResultType]::ParameterName, 'Run probes that may contact configured systems')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;plan' {
            [CompletionResult]::new('--select', '--select', [CompletionResultType]::ParameterName, 'Comma-separated projected fields')
            [CompletionResult]::new('--filter', '--filter', [CompletionResultType]::ParameterName, 'Filter expression; may be repeated')
            [CompletionResult]::new('--limit', '--limit', [CompletionResultType]::ParameterName, 'Maximum rows to read')
            [CompletionResult]::new('--order-by', '--order-by', [CompletionResultType]::ParameterName, 'Ordering field and optional direction')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'Destination URI or cursor upper bound, as shown in usage')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--no-pin', '--no-pin', [CompletionResultType]::ParameterName, 'Do not pin newly discovered schema')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;explain' {
            [CompletionResult]::new('--select', '--select', [CompletionResultType]::ParameterName, 'Comma-separated projected fields')
            [CompletionResult]::new('--filter', '--filter', [CompletionResultType]::ParameterName, 'Filter expression; may be repeated')
            [CompletionResult]::new('--limit', '--limit', [CompletionResultType]::ParameterName, 'Maximum rows to read')
            [CompletionResult]::new('--order-by', '--order-by', [CompletionResultType]::ParameterName, 'Ordering field and optional direction')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'Destination URI or cursor upper bound, as shown in usage')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--no-pin', '--no-pin', [CompletionResultType]::ParameterName, 'Do not pin newly discovered schema')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;run' {
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'Destination URI or cursor upper bound, as shown in usage')
            [CompletionResult]::new('--jobs', '--jobs', [CompletionResultType]::ParameterName, 'Maximum concurrent jobs')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--loop', '--loop', [CompletionResultType]::ParameterName, 'Continue polling for work')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;preview' {
            [CompletionResult]::new('--select', '--select', [CompletionResultType]::ParameterName, 'Comma-separated projected fields')
            [CompletionResult]::new('--filter', '--filter', [CompletionResultType]::ParameterName, 'Filter expression; may be repeated')
            [CompletionResult]::new('--limit', '--limit', [CompletionResultType]::ParameterName, 'Maximum rows to read')
            [CompletionResult]::new('--order-by', '--order-by', [CompletionResultType]::ParameterName, 'Ordering field and optional direction')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;sql' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('project', 'project', [CompletionResultType]::ParameterValue, 'Show resolved project information')
            [CompletionResult]::new('resources', 'resources', [CompletionResultType]::ParameterValue, 'List project resources')
            [CompletionResult]::new('resource', 'resource', [CompletionResultType]::ParameterValue, 'Show one resolved resource')
            [CompletionResult]::new('lock', 'lock', [CompletionResultType]::ParameterValue, 'Show the project lock')
            [CompletionResult]::new('destinations', 'destinations', [CompletionResultType]::ParameterValue, 'List resolved destinations')
            [CompletionResult]::new('package', 'package', [CompletionResultType]::ParameterValue, 'List, verify, archive, and collect packages')
            [CompletionResult]::new('run', 'run', [CompletionResultType]::ParameterValue, 'Execute a governed resource run')
            break
        }
        'cdf;inspect;project' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;resources' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;resource' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;lock' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;destinations' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;package' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;run' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;diff' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('schema', 'schema', [CompletionResultType]::ParameterValue, 'Discover, pin, compare, and promote schemas')
            break
        }
        'cdf;diff;schema' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('discover', 'discover', [CompletionResultType]::ParameterValue, 'Discover the current physical source schema')
            [CompletionResult]::new('pin', 'pin', [CompletionResultType]::ParameterValue, 'Pin a discovered schema into the project contract')
            [CompletionResult]::new('show', 'show', [CompletionResultType]::ParameterValue, 'Show the selected durable record')
            [CompletionResult]::new('diff', 'diff', [CompletionResultType]::ParameterValue, 'Compare durable schemas')
            [CompletionResult]::new('promote', 'promote', [CompletionResultType]::ParameterValue, 'Plan or execute residual schema promotion')
            break
        }
        'cdf;schema;discover' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;pin' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;show' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;diff' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;promote' {
            [CompletionResult]::new('--type', '--type', [CompletionResultType]::ParameterName, 'type')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--execute', '--execute', [CompletionResultType]::ParameterName, 'Apply the planned operation')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;contract' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('freeze', 'freeze', [CompletionResultType]::ParameterValue, 'Freeze a contract snapshot')
            [CompletionResult]::new('show', 'show', [CompletionResultType]::ParameterValue, 'Show the selected durable record')
            [CompletionResult]::new('test', 'test', [CompletionResultType]::ParameterValue, 'Test data against a contract')
            break
        }
        'cdf;contract;freeze' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;contract;show' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;contract;test' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('show', 'show', [CompletionResultType]::ParameterValue, 'Show the selected durable record')
            [CompletionResult]::new('history', 'history', [CompletionResultType]::ParameterValue, 'Show checkpoint history')
            [CompletionResult]::new('rewind', 'rewind', [CompletionResultType]::ParameterValue, 'Create a marker that rewinds checkpoint state')
            [CompletionResult]::new('recover', 'recover', [CompletionResultType]::ParameterValue, 'Recover state from a committed package receipt')
            break
        }
        'cdf;state;show' {
            [CompletionResult]::new('--pipeline', '--pipeline', [CompletionResultType]::ParameterName, 'Pipeline identifier')
            [CompletionResult]::new('--scope', '--scope', [CompletionResultType]::ParameterName, 'Checkpoint scope entry as key=value; may be repeated')
            [CompletionResult]::new('--scope-json', '--scope-json', [CompletionResultType]::ParameterName, 'Checkpoint scope encoded as JSON')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state;history' {
            [CompletionResult]::new('--pipeline', '--pipeline', [CompletionResultType]::ParameterName, 'Pipeline identifier')
            [CompletionResult]::new('--scope', '--scope', [CompletionResultType]::ParameterName, 'Checkpoint scope entry as key=value; may be repeated')
            [CompletionResult]::new('--scope-json', '--scope-json', [CompletionResultType]::ParameterName, 'Checkpoint scope encoded as JSON')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state;rewind' {
            [CompletionResult]::new('--pipeline', '--pipeline', [CompletionResultType]::ParameterName, 'Pipeline identifier')
            [CompletionResult]::new('--scope', '--scope', [CompletionResultType]::ParameterName, 'Checkpoint scope entry as key=value; may be repeated')
            [CompletionResult]::new('--scope-json', '--scope-json', [CompletionResultType]::ParameterName, 'Checkpoint scope encoded as JSON')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'Destination URI or cursor upper bound, as shown in usage')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state;recover' {
            [CompletionResult]::new('--package', '--package', [CompletionResultType]::ParameterName, 'Package directory')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'Destination URI or cursor upper bound, as shown in usage')
            [CompletionResult]::new('--receipt', '--receipt', [CompletionResultType]::ParameterName, 'Receipt identifier')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'Destination target or table')
            [CompletionResult]::new('--merge-dedup', '--merge-dedup', [CompletionResultType]::ParameterName, 'Merge deduplication policy')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;resume' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;replay' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('package', 'package', [CompletionResultType]::ParameterValue, 'List, verify, archive, and collect packages')
            break
        }
        'cdf;replay;package' {
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'Destination URI or cursor upper bound, as shown in usage')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'Destination target or table')
            [CompletionResult]::new('--merge-dedup', '--merge-dedup', [CompletionResultType]::ParameterName, 'Merge deduplication policy')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;backfill' {
            [CompletionResult]::new('--from', '--from', [CompletionResultType]::ParameterName, 'Inclusive cursor lower bound')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'Destination URI or cursor upper bound, as shown in usage')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'Destination target or table')
            [CompletionResult]::new('--slice-size', '--slice-size', [CompletionResultType]::ParameterName, 'Rows per backfill slice')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--execute', '--execute', [CompletionResultType]::ParameterName, 'Apply the planned operation')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('ls', 'ls', [CompletionResultType]::ParameterValue, 'List durable packages')
            [CompletionResult]::new('gc', 'gc', [CompletionResultType]::ParameterValue, 'Collect packages allowed by retention policy')
            [CompletionResult]::new('verify', 'verify', [CompletionResultType]::ParameterValue, 'Verify package integrity and evidence')
            [CompletionResult]::new('archive', 'archive', [CompletionResultType]::ParameterValue, 'Archive a package in a portable format')
            break
        }
        'cdf;package;ls' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package;gc' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package;verify' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package;archive' {
            [CompletionResult]::new('--format', '--format', [CompletionResultType]::ParameterName, 'Archive output format')
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('--force', '--force', [CompletionResultType]::ParameterName, 'Replace an existing artifact when safe')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;doctor' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;status' {
            [CompletionResult]::new('--color', '--color', [CompletionResultType]::ParameterName, 'Color policy: auto, always, or never')
            [CompletionResult]::new('--progress', '--progress', [CompletionResultType]::ParameterName, 'Progress policy: auto, always, or never')
            [CompletionResult]::new('--unicode', '--unicode', [CompletionResultType]::ParameterName, 'Unicode policy: auto, always, or never')
            [CompletionResult]::new('-q', '-q', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('--quiet', '--quiet', [CompletionResultType]::ParameterName, 'Suppress progress and non-primary success narration')
            [CompletionResult]::new('-v', '-v', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('--verbose', '--verbose', [CompletionResultType]::ParameterName, 'Show evidence detail; repeat for diagnostics')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
    })

    $completions.Where{ $_.CompletionText -like "$wordToComplete*" } |
        Sort-Object -Property ListItemText
}
