
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
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('-V', '-V ', [CompletionResultType]::ParameterName, 'Print version')
            [CompletionResult]::new('--version', '--version', [CompletionResultType]::ParameterName, 'Print version')
            [CompletionResult]::new('help', 'help', [CompletionResultType]::ParameterValue, 'help')
            [CompletionResult]::new('version', 'version', [CompletionResultType]::ParameterValue, 'version')
            [CompletionResult]::new('init', 'init', [CompletionResultType]::ParameterValue, 'init')
            [CompletionResult]::new('add', 'add', [CompletionResultType]::ParameterValue, 'add')
            [CompletionResult]::new('validate', 'validate', [CompletionResultType]::ParameterValue, 'validate')
            [CompletionResult]::new('plan', 'plan', [CompletionResultType]::ParameterValue, 'plan')
            [CompletionResult]::new('explain', 'explain', [CompletionResultType]::ParameterValue, 'explain')
            [CompletionResult]::new('run', 'run', [CompletionResultType]::ParameterValue, 'run')
            [CompletionResult]::new('preview', 'preview', [CompletionResultType]::ParameterValue, 'preview')
            [CompletionResult]::new('sql', 'sql', [CompletionResultType]::ParameterValue, 'sql')
            [CompletionResult]::new('inspect', 'inspect', [CompletionResultType]::ParameterValue, 'inspect')
            [CompletionResult]::new('diff', 'diff', [CompletionResultType]::ParameterValue, 'diff')
            [CompletionResult]::new('schema', 'schema', [CompletionResultType]::ParameterValue, 'schema')
            [CompletionResult]::new('contract', 'contract', [CompletionResultType]::ParameterValue, 'contract')
            [CompletionResult]::new('state', 'state', [CompletionResultType]::ParameterValue, 'state')
            [CompletionResult]::new('resume', 'resume', [CompletionResultType]::ParameterValue, 'resume')
            [CompletionResult]::new('replay', 'replay', [CompletionResultType]::ParameterValue, 'replay')
            [CompletionResult]::new('backfill', 'backfill', [CompletionResultType]::ParameterValue, 'backfill')
            [CompletionResult]::new('package', 'package', [CompletionResultType]::ParameterValue, 'package')
            [CompletionResult]::new('doctor', 'doctor', [CompletionResultType]::ParameterValue, 'doctor')
            [CompletionResult]::new('status', 'status', [CompletionResultType]::ParameterValue, 'status')
            break
        }
        'cdf;help' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;version' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;init' {
            [CompletionResult]::new('--name', '--name', [CompletionResultType]::ParameterName, 'name')
            [CompletionResult]::new('--force', '--force', [CompletionResultType]::ParameterName, 'force')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;add' {
            [CompletionResult]::new('--records', '--records', [CompletionResultType]::ParameterName, 'records')
            [CompletionResult]::new('--cursor', '--cursor', [CompletionResultType]::ParameterName, 'cursor')
            [CompletionResult]::new('--cursor-param', '--cursor-param', [CompletionResultType]::ParameterName, 'cursor-param')
            [CompletionResult]::new('--dry-run', '--dry-run', [CompletionResultType]::ParameterName, 'dry-run')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;validate' {
            [CompletionResult]::new('--deep', '--deep', [CompletionResultType]::ParameterName, 'deep')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;plan' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--select', '--select', [CompletionResultType]::ParameterName, 'select')
            [CompletionResult]::new('--filter', '--filter', [CompletionResultType]::ParameterName, 'filter')
            [CompletionResult]::new('--limit', '--limit', [CompletionResultType]::ParameterName, 'limit')
            [CompletionResult]::new('--order-by', '--order-by', [CompletionResultType]::ParameterName, 'order-by')
            [CompletionResult]::new('--package-id', '--package-id', [CompletionResultType]::ParameterName, 'package-id')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'to')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'target')
            [CompletionResult]::new('--no-pin', '--no-pin', [CompletionResultType]::ParameterName, 'no-pin')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;explain' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--select', '--select', [CompletionResultType]::ParameterName, 'select')
            [CompletionResult]::new('--filter', '--filter', [CompletionResultType]::ParameterName, 'filter')
            [CompletionResult]::new('--limit', '--limit', [CompletionResultType]::ParameterName, 'limit')
            [CompletionResult]::new('--order-by', '--order-by', [CompletionResultType]::ParameterName, 'order-by')
            [CompletionResult]::new('--package-id', '--package-id', [CompletionResultType]::ParameterName, 'package-id')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'to')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'target')
            [CompletionResult]::new('--no-pin', '--no-pin', [CompletionResultType]::ParameterName, 'no-pin')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;run' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--pipeline', '--pipeline', [CompletionResultType]::ParameterName, 'pipeline')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'to')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'target')
            [CompletionResult]::new('--package-id', '--package-id', [CompletionResultType]::ParameterName, 'package-id')
            [CompletionResult]::new('--checkpoint-id', '--checkpoint-id', [CompletionResultType]::ParameterName, 'checkpoint-id')
            [CompletionResult]::new('--loop', '--loop', [CompletionResultType]::ParameterName, 'loop')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;preview' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--select', '--select', [CompletionResultType]::ParameterName, 'select')
            [CompletionResult]::new('--filter', '--filter', [CompletionResultType]::ParameterName, 'filter')
            [CompletionResult]::new('--limit', '--limit', [CompletionResultType]::ParameterName, 'limit')
            [CompletionResult]::new('--order-by', '--order-by', [CompletionResultType]::ParameterName, 'order-by')
            [CompletionResult]::new('--package-id', '--package-id', [CompletionResultType]::ParameterName, 'package-id')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;sql' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('project', 'project', [CompletionResultType]::ParameterValue, 'project')
            [CompletionResult]::new('resources', 'resources', [CompletionResultType]::ParameterValue, 'resources')
            [CompletionResult]::new('resource', 'resource', [CompletionResultType]::ParameterValue, 'resource')
            [CompletionResult]::new('lock', 'lock', [CompletionResultType]::ParameterValue, 'lock')
            [CompletionResult]::new('destinations', 'destinations', [CompletionResultType]::ParameterValue, 'destinations')
            [CompletionResult]::new('destination', 'destination', [CompletionResultType]::ParameterValue, 'destination')
            [CompletionResult]::new('package', 'package', [CompletionResultType]::ParameterValue, 'package')
            [CompletionResult]::new('run', 'run', [CompletionResultType]::ParameterValue, 'run')
            break
        }
        'cdf;inspect;project' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;resources' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;resource' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;lock' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;destinations' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;destination' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;package' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;inspect;run' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;diff' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('schema', 'schema', [CompletionResultType]::ParameterValue, 'schema')
            break
        }
        'cdf;diff;schema' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('discover', 'discover', [CompletionResultType]::ParameterValue, 'discover')
            [CompletionResult]::new('pin', 'pin', [CompletionResultType]::ParameterValue, 'pin')
            [CompletionResult]::new('show', 'show', [CompletionResultType]::ParameterValue, 'show')
            [CompletionResult]::new('diff', 'diff', [CompletionResultType]::ParameterValue, 'diff')
            [CompletionResult]::new('promote', 'promote', [CompletionResultType]::ParameterValue, 'promote')
            break
        }
        'cdf;schema;discover' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;pin' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;show' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;diff' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;schema;promote' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--type', '--type', [CompletionResultType]::ParameterName, 'type')
            [CompletionResult]::new('--execute', '--execute', [CompletionResultType]::ParameterName, 'execute')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;contract' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('freeze', 'freeze', [CompletionResultType]::ParameterValue, 'freeze')
            [CompletionResult]::new('show', 'show', [CompletionResultType]::ParameterValue, 'show')
            [CompletionResult]::new('test', 'test', [CompletionResultType]::ParameterValue, 'test')
            break
        }
        'cdf;contract;freeze' {
            [CompletionResult]::new('--contract', '--contract', [CompletionResultType]::ParameterName, 'contract')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;contract;show' {
            [CompletionResult]::new('--trust', '--trust', [CompletionResultType]::ParameterName, 'trust')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;contract;test' {
            [CompletionResult]::new('--contract', '--contract', [CompletionResultType]::ParameterName, 'contract')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('show', 'show', [CompletionResultType]::ParameterValue, 'show')
            [CompletionResult]::new('history', 'history', [CompletionResultType]::ParameterValue, 'history')
            [CompletionResult]::new('rewind', 'rewind', [CompletionResultType]::ParameterValue, 'rewind')
            [CompletionResult]::new('migrate', 'migrate', [CompletionResultType]::ParameterValue, 'migrate')
            [CompletionResult]::new('recover', 'recover', [CompletionResultType]::ParameterValue, 'recover')
            break
        }
        'cdf;state;show' {
            [CompletionResult]::new('--pipeline', '--pipeline', [CompletionResultType]::ParameterName, 'pipeline')
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--scope', '--scope', [CompletionResultType]::ParameterName, 'scope')
            [CompletionResult]::new('--scope-json', '--scope-json', [CompletionResultType]::ParameterName, 'scope-json')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state;history' {
            [CompletionResult]::new('--pipeline', '--pipeline', [CompletionResultType]::ParameterName, 'pipeline')
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--scope', '--scope', [CompletionResultType]::ParameterName, 'scope')
            [CompletionResult]::new('--scope-json', '--scope-json', [CompletionResultType]::ParameterName, 'scope-json')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state;rewind' {
            [CompletionResult]::new('--pipeline', '--pipeline', [CompletionResultType]::ParameterName, 'pipeline')
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--scope', '--scope', [CompletionResultType]::ParameterName, 'scope')
            [CompletionResult]::new('--scope-json', '--scope-json', [CompletionResultType]::ParameterName, 'scope-json')
            [CompletionResult]::new('--target-checkpoint', '--target-checkpoint', [CompletionResultType]::ParameterName, 'target-checkpoint')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'target-checkpoint')
            [CompletionResult]::new('--marker-checkpoint', '--marker-checkpoint', [CompletionResultType]::ParameterName, 'marker-checkpoint')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state;migrate' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;state;recover' {
            [CompletionResult]::new('--package', '--package', [CompletionResultType]::ParameterName, 'package')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'to')
            [CompletionResult]::new('--receipt', '--receipt', [CompletionResultType]::ParameterName, 'receipt')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'target')
            [CompletionResult]::new('--merge-dedup', '--merge-dedup', [CompletionResultType]::ParameterName, 'merge-dedup')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;resume' {
            [CompletionResult]::new('--run', '--run', [CompletionResultType]::ParameterName, 'run')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;replay' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('package', 'package', [CompletionResultType]::ParameterValue, 'package')
            break
        }
        'cdf;replay;package' {
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'to')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'target')
            [CompletionResult]::new('--merge-dedup', '--merge-dedup', [CompletionResultType]::ParameterName, 'merge-dedup')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;backfill' {
            [CompletionResult]::new('--resource', '--resource', [CompletionResultType]::ParameterName, 'resource')
            [CompletionResult]::new('--from', '--from', [CompletionResultType]::ParameterName, 'from')
            [CompletionResult]::new('--to', '--to', [CompletionResultType]::ParameterName, 'to')
            [CompletionResult]::new('--target', '--target', [CompletionResultType]::ParameterName, 'target')
            [CompletionResult]::new('--slice-size', '--slice-size', [CompletionResultType]::ParameterName, 'slice-size')
            [CompletionResult]::new('--execute', '--execute', [CompletionResultType]::ParameterName, 'execute')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('ls', 'ls', [CompletionResultType]::ParameterValue, 'ls')
            [CompletionResult]::new('gc', 'gc', [CompletionResultType]::ParameterValue, 'gc')
            [CompletionResult]::new('verify', 'verify', [CompletionResultType]::ParameterValue, 'verify')
            [CompletionResult]::new('archive', 'archive', [CompletionResultType]::ParameterValue, 'archive')
            break
        }
        'cdf;package;ls' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package;gc' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package;verify' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;package;archive' {
            [CompletionResult]::new('--format', '--format', [CompletionResultType]::ParameterName, 'format')
            [CompletionResult]::new('--force', '--force', [CompletionResultType]::ParameterName, 'force')
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;doctor' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
        'cdf;status' {
            [CompletionResult]::new('--no-color', '--no-color', [CompletionResultType]::ParameterName, 'no-color')
            [CompletionResult]::new('-h', '-h', [CompletionResultType]::ParameterName, 'Print help')
            [CompletionResult]::new('--help', '--help', [CompletionResultType]::ParameterName, 'Print help')
            break
        }
    })

    $completions.Where{ $_.CompletionText -like "$wordToComplete*" } |
        Sort-Object -Property ListItemText
}
