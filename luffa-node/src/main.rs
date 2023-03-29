use std::sync::Arc;
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use luffa_node::config::{Config, CONFIG_FILE_NAME, ENV_PREFIX};
use luffa_node::ServerConfig;
use luffa_node::{cli::Args, metrics, DiskStorage, Keychain, Node};
// use luffa_util::lock::ProgramLock;
use luffa_util::{luffa_config_path, make_config};
use tokio::task;
use tracing::error;
use luffa_node::rpc::P2p;

/// Starts daemon process
fn main() -> Result<()> {
    // let mut lock = ProgramLock::new("luffa-node")?;
    // lock.acquire_or_exit();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(2048)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async move {
        let version = option_env!("LUFFA_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"));
        tracing::info!("Starting luffa-node, version {version}");

        let args = Args::parse();

        // TODO: configurable network
        let cfg_path = luffa_config_path(CONFIG_FILE_NAME)?;
        let sources = [Some(cfg_path.as_path()), args.cfg.as_deref()];
        let network_config = make_config(
            // default
            ServerConfig::default(),
            // potential config files
            &sources,
            // env var prefix for this config
            ENV_PREFIX,
            // map of present command line arguments
            args.make_overrides_map(),
        )
        .context("invalid config")?;

        let metrics_config =
            metrics::metrics_config_with_compile_time_info(network_config.metrics.clone());

        let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
            .await
            .map_err(|e| anyhow!("metrics init failed: {:?}", e))?;

        #[cfg(unix)]
        {
            match luffa_util::increase_fd_limit() {
                Ok(soft) => tracing::debug!("NOFILE limit: soft = {}", soft),
                Err(err) => error!("Error increasing NOFILE limit: {}", err),
            }
        } 
        tracing::info!("network_config:{network_config:?}");
        let db_path = &network_config.path.clone();
        let db = luffa_store::Store::open(db_path.clone()).await?;
        let network_config = Config::from(network_config);
        let kc = Keychain::<DiskStorage>::new(network_config.key_store_path.clone()).await?;
        
        
        let (mut p2p,network_sender_in) = Node::new(network_config, kc,Arc::new(db),Some("Relay".to_string()),None).await?;
        let p2p_client = P2p::new(network_sender_in);
        let mut events = p2p.network_events(); 
        task::spawn(async move {
            while let Some(e) = events.recv().await {
                tracing::info!("e:{e:?}");
            }
            tracing::info!("---------event exit-----------");
        });
        // Start services
        let p2p_task = task::spawn(async move {
            if let Err(err) = p2p.run().await {
                error!("{:?}", err);
            }
        });

        luffa_util::block_until_sigint().await;

        // Cancel all async services
        p2p_task.abort();
        p2p_task.await.ok();

        metrics_handle.shutdown();
        Ok(())
    })
}
