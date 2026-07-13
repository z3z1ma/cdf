# Print an optspec for argparse to handle cmd's options that are independent of any subcommand.
function __fish_cdf_global_optspecs
    string join \n q/quiet v/verbose color= progress= unicode= h/help V/version
end

function __fish_cdf_needs_command
    # Figure out if the current invocation already has a command.
    set -l cmd (commandline -opc)
    set -e cmd[1]
    argparse -s (__fish_cdf_global_optspecs) -- $cmd 2>/dev/null
    or return
    if set -q argv[1]
        # Also print the command, so this can be used to figure out what it is.
        echo $argv[1]
        return 1
    end
    return 0
end

function __fish_cdf_using_subcommand
    set -l cmd (__fish_cdf_needs_command)
    test -z "$cmd"
    and return 1
    contains -- $cmd[1] $argv
end

complete -c cdf -n "__fish_cdf_needs_command" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_needs_command" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_needs_command" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_needs_command" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_needs_command" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_needs_command" -s h -l help -d 'Print help (see more with \'--help\')'
complete -c cdf -n "__fish_cdf_needs_command" -s V -l version -d 'Print version'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "help" -d 'Show help for a command'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "version" -d 'Print the cdf version'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "init" -d 'Create a new cdf project'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "add" -d 'Add a source resource to the project'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "validate" -d 'Validate project configuration and contracts'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "plan" -d 'Plan a resource run without executing it'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "explain" -d 'Explain resolution, capabilities, and execution choices'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "run" -d 'Execute a governed resource run'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "preview" -d 'Read a bounded preview without committing data'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "sql" -d 'Query cdf system metadata'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "inspect" -d 'Inspect durable project and run evidence'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "diff" -d 'Compare durable schemas'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "schema" -d 'Discover, pin, compare, and promote schemas'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "contract" -d 'Freeze, show, and test contracts'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "state" -d 'Inspect and recover checkpoint state'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "resume" -d 'Resume interrupted work from the run ledger'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "replay" -d 'Replay a verified package'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "backfill" -d 'Plan or execute a bounded cursor backfill'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "package" -d 'List, verify, archive, and collect packages'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "doctor" -d 'Check local runtime and destination health'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "status" -d 'Summarize project freshness and run state'
complete -c cdf -n "__fish_cdf_using_subcommand help" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand help" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand help" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand help" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand help" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand help" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand version" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand version" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand version" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand version" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand version" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand version" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand init" -l name -d 'Project name' -r
complete -c cdf -n "__fish_cdf_using_subcommand init" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand init" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand init" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand init" -l force -d 'Replace an existing artifact when safe'
complete -c cdf -n "__fish_cdf_using_subcommand init" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand init" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand init" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand add" -l records -d 'Record selector within the source' -r
complete -c cdf -n "__fish_cdf_using_subcommand add" -l cursor -d 'Cursor field' -r
complete -c cdf -n "__fish_cdf_using_subcommand add" -l cursor-param -d 'Request parameter carrying the cursor' -r
complete -c cdf -n "__fish_cdf_using_subcommand add" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand add" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand add" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand add" -l dry-run -d 'Show the proposed change without writing it'
complete -c cdf -n "__fish_cdf_using_subcommand add" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand add" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand add" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand validate" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand validate" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand validate" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand validate" -l deep -d 'Run probes that may contact configured systems'
complete -c cdf -n "__fish_cdf_using_subcommand validate" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand validate" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand validate" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l select -d 'Comma-separated projected fields' -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l filter -d 'Filter expression; may be repeated' -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l limit -d 'Maximum rows to read' -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l order-by -d 'Ordering field and optional direction' -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l to -d 'Destination URI or cursor upper bound, as shown in usage' -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l no-pin -d 'Do not pin newly discovered schema'
complete -c cdf -n "__fish_cdf_using_subcommand plan" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand plan" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand plan" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l select -d 'Comma-separated projected fields' -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l filter -d 'Filter expression; may be repeated' -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l limit -d 'Maximum rows to read' -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l order-by -d 'Ordering field and optional direction' -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l to -d 'Destination URI or cursor upper bound, as shown in usage' -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l no-pin -d 'Do not pin newly discovered schema'
complete -c cdf -n "__fish_cdf_using_subcommand explain" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand explain" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand explain" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand run" -l to -d 'Destination URI or cursor upper bound, as shown in usage' -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l jobs -d 'Maximum concurrent jobs' -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand run" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand run" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand run" -l loop -d 'Continue polling for work'
complete -c cdf -n "__fish_cdf_using_subcommand run" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand run" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand run" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l select -d 'Comma-separated projected fields' -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l filter -d 'Filter expression; may be repeated' -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l limit -d 'Maximum rows to read' -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l order-by -d 'Ordering field and optional direction' -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand preview" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand preview" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand preview" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand sql" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand sql" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand sql" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand sql" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand sql" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand sql" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -f -a "project" -d 'Show resolved project information'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -f -a "resources" -d 'List project resources'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -f -a "resource" -d 'Show one resolved resource'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -f -a "lock" -d 'Show the project lock'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -f -a "destinations" -d 'List resolved destinations'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -f -a "package" -d 'List, verify, archive, and collect packages'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations package run" -f -a "run" -d 'Execute a governed resource run'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -f -a "schema" -d 'Discover, pin, compare, and promote schemas'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -f -a "discover" -d 'Discover the current physical source schema'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -f -a "pin" -d 'Pin a discovered schema into the project contract'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -f -a "show" -d 'Show the selected durable record'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -f -a "diff" -d 'Compare durable schemas'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and not __fish_seen_subcommand_from discover pin show diff promote" -f -a "promote" -d 'Plan or execute residual schema promotion'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from discover" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from discover" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from discover" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from discover" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from discover" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from discover" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from pin" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from pin" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from pin" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from pin" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from pin" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from pin" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from show" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from show" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from show" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from show" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from show" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from show" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from diff" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from diff" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from diff" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from diff" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from diff" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from diff" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -l type -r
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -l execute -d 'Apply the planned operation'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand schema; and __fish_seen_subcommand_from promote" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -f -a "freeze" -d 'Freeze a contract snapshot'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -f -a "show" -d 'Show the selected durable record'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -f -a "test" -d 'Test data against a contract'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "show" -d 'Show the selected durable record'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "history" -d 'Show checkpoint history'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "rewind" -d 'Create a marker that rewinds checkpoint state'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "migrate" -d 'Migrate the local state store'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "recover" -d 'Recover state from a committed package receipt'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l pipeline -d 'Pipeline identifier' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l scope -d 'Checkpoint scope entry as key=value; may be repeated' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l scope-json -d 'Checkpoint scope encoded as JSON' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l pipeline -d 'Pipeline identifier' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l scope -d 'Checkpoint scope entry as key=value; may be repeated' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l scope-json -d 'Checkpoint scope encoded as JSON' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l pipeline -d 'Pipeline identifier' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l scope -d 'Checkpoint scope entry as key=value; may be repeated' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l scope-json -d 'Checkpoint scope encoded as JSON' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l to -d 'Destination URI or cursor upper bound, as shown in usage' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l package -d 'Package directory' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l to -d 'Destination URI or cursor upper bound, as shown in usage' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l receipt -d 'Receipt identifier' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l target -d 'Destination target or table' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l merge-dedup -d 'Merge deduplication policy' -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand resume" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand resume" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand resume" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand resume" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand resume" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand resume" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -f -a "package" -d 'List, verify, archive, and collect packages'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l to -d 'Destination URI or cursor upper bound, as shown in usage' -r
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l target -d 'Destination target or table' -r
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l merge-dedup -d 'Merge deduplication policy' -r
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l from -d 'Inclusive cursor lower bound' -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l to -d 'Destination URI or cursor upper bound, as shown in usage' -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l target -d 'Destination target or table' -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l slice-size -d 'Rows per backfill slice' -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l execute -d 'Apply the planned operation'
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "ls" -d 'List durable packages'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "gc" -d 'Collect packages allowed by retention policy'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "verify" -d 'Verify package integrity and evidence'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "archive" -d 'Archive a package in a portable format'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l format -d 'Archive output format' -r
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l force -d 'Replace an existing artifact when safe'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand status" -l color -d 'Color policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand status" -l progress -d 'Progress policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand status" -l unicode -d 'Unicode policy: auto, always, or never' -r -f -a "auto\t''
always\t''
never\t''"
complete -c cdf -n "__fish_cdf_using_subcommand status" -s q -l quiet -d 'Suppress progress and non-primary success narration'
complete -c cdf -n "__fish_cdf_using_subcommand status" -s v -l verbose -d 'Show evidence detail; repeat for diagnostics'
complete -c cdf -n "__fish_cdf_using_subcommand status" -s h -l help -d 'Print help'
