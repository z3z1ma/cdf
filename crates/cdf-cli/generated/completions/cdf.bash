_cdf() {
    local i cur prev opts cmd
    COMPREPLY=()
    if [[ "${BASH_VERSINFO[0]}" -ge 4 ]]; then
        cur="$2"
    else
        cur="${COMP_WORDS[COMP_CWORD]}"
    fi
    prev="$3"
    cmd=""
    opts=""

    for i in "${COMP_WORDS[@]:0:COMP_CWORD}"
    do
        case "${cmd},${i}" in
            ",$1")
                cmd="cdf"
                ;;
            cdf,add)
                cmd="cdf__subcmd__add"
                ;;
            cdf,backfill)
                cmd="cdf__subcmd__backfill"
                ;;
            cdf,compile)
                cmd="cdf__subcmd__compile"
                ;;
            cdf,contract)
                cmd="cdf__subcmd__contract"
                ;;
            cdf,discover)
                cmd="cdf__subcmd__discover"
                ;;
            cdf,doctor)
                cmd="cdf__subcmd__doctor"
                ;;
            cdf,explain)
                cmd="cdf__subcmd__explain"
                ;;
            cdf,help)
                cmd="cdf__subcmd__help"
                ;;
            cdf,init)
                cmd="cdf__subcmd__init"
                ;;
            cdf,inspect)
                cmd="cdf__subcmd__inspect"
                ;;
            cdf,package)
                cmd="cdf__subcmd__package"
                ;;
            cdf,plan)
                cmd="cdf__subcmd__plan"
                ;;
            cdf,preview)
                cmd="cdf__subcmd__preview"
                ;;
            cdf,run)
                cmd="cdf__subcmd__run"
                ;;
            cdf,schema)
                cmd="cdf__subcmd__schema"
                ;;
            cdf,sql)
                cmd="cdf__subcmd__sql"
                ;;
            cdf,state)
                cmd="cdf__subcmd__state"
                ;;
            cdf,status)
                cmd="cdf__subcmd__status"
                ;;
            cdf,validate)
                cmd="cdf__subcmd__validate"
                ;;
            cdf,version)
                cmd="cdf__subcmd__version"
                ;;
            cdf__subcmd__contract,show)
                cmd="cdf__subcmd__contract__subcmd__show"
                ;;
            cdf__subcmd__discover,resource)
                cmd="cdf__subcmd__discover__subcmd__resource"
                ;;
            cdf__subcmd__discover,source)
                cmd="cdf__subcmd__discover__subcmd__source"
                ;;
            cdf__subcmd__doctor,all)
                cmd="cdf__subcmd__doctor__subcmd__all"
                ;;
            cdf__subcmd__doctor,destination)
                cmd="cdf__subcmd__doctor__subcmd__destination"
                ;;
            cdf__subcmd__doctor,resource)
                cmd="cdf__subcmd__doctor__subcmd__resource"
                ;;
            cdf__subcmd__doctor,runtime)
                cmd="cdf__subcmd__doctor__subcmd__runtime"
                ;;
            cdf__subcmd__doctor,source)
                cmd="cdf__subcmd__doctor__subcmd__source"
                ;;
            cdf__subcmd__inspect,destinations)
                cmd="cdf__subcmd__inspect__subcmd__destinations"
                ;;
            cdf__subcmd__inspect,package)
                cmd="cdf__subcmd__inspect__subcmd__package"
                ;;
            cdf__subcmd__inspect,project)
                cmd="cdf__subcmd__inspect__subcmd__project"
                ;;
            cdf__subcmd__inspect,resource)
                cmd="cdf__subcmd__inspect__subcmd__resource"
                ;;
            cdf__subcmd__inspect,resources)
                cmd="cdf__subcmd__inspect__subcmd__resources"
                ;;
            cdf__subcmd__inspect,run)
                cmd="cdf__subcmd__inspect__subcmd__run"
                ;;
            cdf__subcmd__package,archive)
                cmd="cdf__subcmd__package__subcmd__archive"
                ;;
            cdf__subcmd__package,gc)
                cmd="cdf__subcmd__package__subcmd__gc"
                ;;
            cdf__subcmd__package,ls)
                cmd="cdf__subcmd__package__subcmd__ls"
                ;;
            cdf__subcmd__package,verify)
                cmd="cdf__subcmd__package__subcmd__verify"
                ;;
            cdf__subcmd__schema,diff)
                cmd="cdf__subcmd__schema__subcmd__diff"
                ;;
            cdf__subcmd__schema,promote)
                cmd="cdf__subcmd__schema__subcmd__promote"
                ;;
            cdf__subcmd__schema,show)
                cmd="cdf__subcmd__schema__subcmd__show"
                ;;
            cdf__subcmd__state,history)
                cmd="cdf__subcmd__state__subcmd__history"
                ;;
            cdf__subcmd__state,recover)
                cmd="cdf__subcmd__state__subcmd__recover"
                ;;
            cdf__subcmd__state,rewind)
                cmd="cdf__subcmd__state__subcmd__rewind"
                ;;
            cdf__subcmd__state,show)
                cmd="cdf__subcmd__state__subcmd__show"
                ;;
            *)
                ;;
        esac
    done

    case "${cmd}" in
        cdf)
            opts="-q -v -h -V --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help --version help version init add discover compile validate plan explain run preview sql inspect schema contract state backfill package doctor status"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 1 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__add)
            opts="-q -v -h --dry-run --source --option --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --source)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --option)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__backfill)
            opts="-q -v -h --from --to --target --execute --slice-size --segment-target-rows --segment-target-bytes --segment-max-rows --segment-max-bytes --microbatch-min-rows --microbatch-max-rows --microbatch-min-bytes --microbatch-max-bytes --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --from)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --to)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --target)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --slice-size)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__compile)
            opts="-q -v -h --exclude --locked --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --exclude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__contract)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help show"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__contract__subcmd__show)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__discover)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help source resource"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__discover__subcmd__resource)
            opts="-q -v -h --exclude --out --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --exclude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --out)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__discover__subcmd__source)
            opts="-q -v -h --out --generate --namespace --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --out)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --namespace)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__doctor)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help runtime resource source destination all"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__doctor__subcmd__all)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__doctor__subcmd__destination)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__doctor__subcmd__resource)
            opts="-q -v -h --exclude --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --exclude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__doctor__subcmd__runtime)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__doctor__subcmd__source)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__explain)
            opts="-q -v -h --select --filter --limit --order-by --to --segment-target-rows --segment-target-bytes --segment-max-rows --segment-max-bytes --microbatch-min-rows --microbatch-max-rows --microbatch-min-bytes --microbatch-max-bytes --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --select)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --filter)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --limit)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --order-by)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --to)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__help)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__init)
            opts="-q -v -h --name --force --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --name)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__inspect)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help project resources resource destinations package run"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__inspect__subcmd__destinations)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__inspect__subcmd__package)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__inspect__subcmd__project)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__inspect__subcmd__resource)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__inspect__subcmd__resources)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__inspect__subcmd__run)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__package)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help ls gc verify archive"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__package__subcmd__archive)
            opts="-q -v -h --format --force --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --format)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__package__subcmd__gc)
            opts="-q -v -h --execute --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__package__subcmd__ls)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__package__subcmd__verify)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__plan)
            opts="-q -v -h --exclude --out --select --filter --limit --order-by --to --segment-target-rows --segment-target-bytes --segment-max-rows --segment-max-bytes --microbatch-min-rows --microbatch-max-rows --microbatch-min-bytes --microbatch-max-bytes --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --exclude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --out)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --select)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --filter)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --limit)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --order-by)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --to)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__preview)
            opts="-q -v -h --select --filter --limit --order-by --segment-target-rows --segment-target-bytes --segment-max-rows --segment-max-bytes --microbatch-min-rows --microbatch-max-rows --microbatch-min-bytes --microbatch-max-bytes --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --select)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --filter)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --limit)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --order-by)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__run)
            opts="-q -v -h --exclude --plan --package --resume --locked --to --target --jobs --stats-profile --explain-memory --loop --segment-target-rows --segment-target-bytes --segment-max-rows --segment-max-bytes --microbatch-min-rows --microbatch-max-rows --microbatch-min-bytes --microbatch-max-bytes --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --exclude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --plan)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --package)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --resume)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --to)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --target)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --jobs)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-target-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --segment-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-rows)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-min-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --microbatch-max-bytes)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__schema)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help show diff promote"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__schema__subcmd__diff)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__schema__subcmd__promote)
            opts="-q -v -h --type --execute --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --type)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__schema__subcmd__show)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__sql)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__state)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help show history rewind recover"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__state__subcmd__history)
            opts="-q -v -h --pipeline --scope --scope-json --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --pipeline)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --scope)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --scope-json)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__state__subcmd__recover)
            opts="-q -v -h --package --to --receipt --target --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --package)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --to)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --receipt)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --target)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__state__subcmd__rewind)
            opts="-q -v -h --pipeline --scope --scope-json --to --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --pipeline)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --scope)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --scope-json)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --to)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__state__subcmd__show)
            opts="-q -v -h --pipeline --scope --scope-json --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --pipeline)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --scope)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --scope-json)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__status)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__validate)
            opts="-q -v -h --exclude --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --exclude)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        cdf__subcmd__version)
            opts="-q -v -h --quiet --verbose --color --progress --unicode --memory-budget --spill-budget --help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --color)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --progress)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --unicode)
                    COMPREPLY=($(compgen -W "auto always never" -- "${cur}"))
                    return 0
                    ;;
                --memory-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --spill-budget)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
    esac
}

if [[ "${BASH_VERSINFO[0]}" -eq 4 && "${BASH_VERSINFO[1]}" -ge 4 || "${BASH_VERSINFO[0]}" -gt 4 ]]; then
    complete -F _cdf -o nosort -o bashdefault -o default cdf
else
    complete -F _cdf -o bashdefault -o default cdf
fi
