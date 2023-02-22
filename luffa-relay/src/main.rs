use std::str::FromStr;

#[allow(unused_imports)]
use anyhow::{anyhow, Result};
use clap::Parser;
use luffa_relay::{
    cli::Args,
    config::{Config, CONFIG_FILE_NAME, ENV_PREFIX},
};
use luffa_rpc_types::Addr;
use luffa_util::lock::ProgramLock;
use luffa_util::{luffa_config_path, make_config};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut lock = ProgramLock::new("luffa-relay")?;
    lock.acquire_or_exit();

    let args = Args::parse();

    let cfg_path = luffa_config_path(CONFIG_FILE_NAME)?;
    let sources = [Some(cfg_path.as_path()), args.cfg.as_deref()];
    let mut config = make_config(
        // default
        Config::default(),
        // potential config files
        &sources,
        // env var prefix for this config
        ENV_PREFIX,
        // map of present command line arguments
        args.make_overrides_map(),
    )
    .unwrap();

    #[cfg(unix)]
    {
        match luffa_util::increase_fd_limit() {
            Ok(soft) => tracing::debug!("NOFILE limit: soft = {}", soft),
            Err(err) => tracing::error!("Error increasing NOFILE limit: {}", err),
        }
    }

    let (store_rpc, p2p_rpc) = {
        let store_recv = Addr::new_mem();
        let store_sender = store_recv.clone();
        let p2p_recv = Addr::from_str("irpc://0.0.0.0:8887").unwrap();
        let p2p_sender = p2p_recv.clone();
        config.rpc_client.store_addr = Some(store_sender);
        config.rpc_client.p2p_addr = Some(p2p_sender);
        config.synchronize_subconfigs();

        let store_rpc = luffa_relay::mem_store::start(store_recv, config.store.clone()).await?;

        let p2p_rpc = luffa_relay::mem_p2p::start(p2p_recv, config.p2p.clone()).await?;
        (store_rpc, p2p_rpc)
    };

    config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);
    println!("{config:#?}");

    let metrics_config = config.metrics.clone();

    let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
        .await
        .expect("failed to initialize metrics");

    luffa_util::block_until_sigint().await;

    store_rpc.abort();
    p2p_rpc.abort();

    metrics_handle.shutdown();
    Ok(())
}
