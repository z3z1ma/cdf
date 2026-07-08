# Print an optspec for argparse to handle cmd's options that are independent of any subcommand.
function __fish_cdf_global_optspecs
    string join \n no-color h/help V/version
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

complete -c cdf -n "__fish_cdf_needs_command" -l no-color
complete -c cdf -n "__fish_cdf_needs_command" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_needs_command" -s V -l version -d 'Print version'
complete -c cdf -n "__fish_cdf_needs_command" -f -a "help"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "version"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "init"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "validate"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "plan"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "explain"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "run"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "preview"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "sql"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "inspect"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "diff"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "contract"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "state"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "resume"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "replay"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "backfill"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "package"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "doctor"
complete -c cdf -n "__fish_cdf_needs_command" -f -a "status"
complete -c cdf -n "__fish_cdf_using_subcommand help" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand help" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand version" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand version" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand init" -l name -r
complete -c cdf -n "__fish_cdf_using_subcommand init" -l force
complete -c cdf -n "__fish_cdf_using_subcommand init" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand init" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand validate" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand validate" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l select -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l filter -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l limit -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l order-by -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l package-id -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l to -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l target -r
complete -c cdf -n "__fish_cdf_using_subcommand plan" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand plan" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l select -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l filter -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l limit -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l order-by -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l package-id -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l to -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l target -r
complete -c cdf -n "__fish_cdf_using_subcommand explain" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand explain" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand run" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l pipeline -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l to -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l target -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l package-id -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l checkpoint-id -r
complete -c cdf -n "__fish_cdf_using_subcommand run" -l loop
complete -c cdf -n "__fish_cdf_using_subcommand run" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand run" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l select -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l filter -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l limit -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l order-by -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l package-id -r
complete -c cdf -n "__fish_cdf_using_subcommand preview" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand preview" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand sql" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand sql" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "project"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "resources"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "resource"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "lock"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "destinations"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "destination"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "package"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and not __fish_seen_subcommand_from project resources resource lock destinations destination package run" -f -a "run"
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from project" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resources" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from resource" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from lock" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destinations" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destination" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from destination" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from package" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand inspect; and __fish_seen_subcommand_from run" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand diff; and not __fish_seen_subcommand_from schema" -f -a "schema"
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand diff; and __fish_seen_subcommand_from schema" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -f -a "freeze"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -f -a "show"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and not __fish_seen_subcommand_from freeze show test" -f -a "test"
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -l contract -r
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from freeze" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -l trust -r
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from show" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -l contract -r
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand contract; and __fish_seen_subcommand_from test" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "show"
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "history"
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "rewind"
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "migrate"
complete -c cdf -n "__fish_cdf_using_subcommand state; and not __fish_seen_subcommand_from show history rewind migrate recover" -f -a "recover"
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l pipeline -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l scope -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l scope-json -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from show" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l pipeline -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l scope -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l scope-json -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from history" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l pipeline -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l scope -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l scope-json -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l target-checkpoint -l to -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l marker-checkpoint -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from rewind" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from migrate" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l package -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l to -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l receipt -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l target -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l merge-dedup -r
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand state; and __fish_seen_subcommand_from recover" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand resume" -l run -r
complete -c cdf -n "__fish_cdf_using_subcommand resume" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand resume" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand replay; and not __fish_seen_subcommand_from package" -f -a "package"
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l to -r
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l target -r
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l merge-dedup -r
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand replay; and __fish_seen_subcommand_from package" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l resource -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l from -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l to -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l target -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l slice-size -r
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l execute
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand backfill" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "ls"
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "gc"
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "verify"
complete -c cdf -n "__fish_cdf_using_subcommand package; and not __fish_seen_subcommand_from ls gc verify archive" -f -a "archive"
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from ls" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from gc" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from verify" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l format -r
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l force
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand package; and __fish_seen_subcommand_from archive" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand doctor" -s h -l help -d 'Print help'
complete -c cdf -n "__fish_cdf_using_subcommand status" -l no-color
complete -c cdf -n "__fish_cdf_using_subcommand status" -s h -l help -d 'Print help'
