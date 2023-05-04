#! /bin/env bash -

# usage: bash ./client-ctl2.sh [options] -- subcommand
# options:
# --log-level: set the value of RUST-LOG environment variable
# --base-dir: set base dir that standard and standard error redirect file from the program output
# --boot-config: set the value of option cfg of the program
# --program-file: set the position that the program execute file
# subcommands:
# run seq|start [end]
# rm seq|start [end]
# examples:
# bash ./client-ctl2.sh --log-level=ERROR -- run 10
# bash ./client-ctl2.sh --log-level=ERROR --boot-config=./node.config.toml -- run 11 20

args=$(getopt -l log-level::,program::,base-dir::,program-file::,boot-config:: -n 'client-ctl2.sh' - "$@")
if [ $? != 0 ]
then
    echo "invalied options"
    exit 1
fi

log_level="WARN"
case $(uname) in
    "Darwin")
        progarm="./target/aarch64-apple-darwin/release/luffa-client"
    ;;
    "Linux")
        progarm="./target/release/luffa-client"
    ;;
esac
base_dir="../.local"
boot_config="./luffa-sdk/luffa.client2.toml"

eval set -- "$args"
while true
do
    case $1 in
        --log-level)
            log_level=$2
            shift 2
        ;;
        --program)
            if [ -n "$2" ]
            then
                echo "nothing"
            fi
            shift 2
        ;;
        --base-dir)
            if [ -n "$2" ]
            then
                base_dir="$2"
            fi
            shift 2
        ;;
        --program-file)
            if [ -n "$2" ]
            then
                progarm="$2"
            fi
            shift 2
        ;;
        --boot-config)
            if [ -n "$2" ]
            then
                boot_config="$2"
            fi
            shift 2
        ;;
        --)
            shift
            break
        ;;
    esac
done

op=$1
if [ -z "$op" ]
then
    echo "missing subcommand"
    exit 1
fi

shift
n_start=$1
n_end=$2

mkdir -p "$base_dir"

if [ -n "$log_level" ]
then
    export RUST_LOG=$log_level
fi

for n_seq in $(seq $n_start $n_end)
do
    # env_base="$base_dir/client-$n_seq"
    # env_config="$env_base/configs"
    # env_data="$env_base/data"
    # env_cache="$env_base/cache"
    
    # export LUFFA_CONFIG_DIR="$env_config"
    # export LUFFA_DATA_DIR="$env_data"
    # export LUFFA_CACHE_DIR="$env_cache"

    case $op in
    "run")
        LUFFA_DATA_DIR="$base_dir/client-$n_seq" $progarm --tag client-$n_seq --cfg $boot_config &> $base_dir/client-${n_seq}.log &
        ;;
    "rm")
        ps -ef | grep luffa-client | grep  " client-$n_seq " | awk '{print $2}' | xargs kill
        rm -rf $base_dir/client-$n_seq
        ;;
    esac
done