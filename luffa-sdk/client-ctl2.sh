#! /bin/env bash -
op=$1
if [ -z "$op" ]
then
    echo "操作不能为空"
    exit 1
fi

shift
n_start=$1
n_end=$2

base_dir="../.local"
log_level="WARN"
case $(uname) in
    "Darwin")
        progarm="./target/aarch64-apple-darwin/release/luffa-client"
        ;;
    "Linux")
        progarm="./target/release/luffa-client"
        ;;
esac

mkdir -p "$base_dir"

#if [ -n "$log_level" ]
#then
#    export RUST_LOG=$log_level
#fi
for n_seq in $(seq $n_start $n_end)
do
    env_base="$base_dir/client-$n_seq"
    env_config="$env_base/configs"
    env_data="$env_base/data"
    env_cache="$env_base/cache"
    
    # export LUFFA_CONFIG_DIR="$env_config"
    # export LUFFA_DATA_DIR="$env_data"
    # export LUFFA_CACHE_DIR="$env_cache"

    case $op in
    "run")
        LUFFA_DATA_DIR="$base_dir/client-$n_seq" nohup $progarm --tag client-$n_seq --cfg ./luffa-sdk/luffa.client2.toml &> $base_dir/client-${n_seq}.log &
        ;;
    "rm")
        ps -ef | grep luffa-client | grep  " client-$n_seq " | awk '{print $2}' | xargs kill
        rm -rf $base_dir/client-$n_seq
        ;;
    esac
done