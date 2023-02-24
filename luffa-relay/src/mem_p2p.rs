/// A p2p instance listening on a memory rpc channel.
use luffa_node::config::Config;
use luffa_node::{DiskStorage, Keychain, Node};
use luffa_rpc_types::p2p::P2pAddr;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::error;

/// Starts a new p2p node, using the given mem rpc channel.
pub async fn start(rpc_addr: P2pAddr, config: Config) -> anyhow::Result<JoinHandle<()>> {
    let kc = Keychain::<DiskStorage>::new(config.key_store_path.clone()).await?;

    let mut p2p = Node::new(config, rpc_addr, kc).await?;
    let mut events = p2p.network_events();
    task::spawn(async move {
        while let Some(e) = events.recv().await {
            tracing::warn!("e:{e:?}");
        }
        tracing::warn!("---------event exit-----------");
    });
    // Start services
    let p2p_task = task::spawn(async move {
        if let Err(err) = p2p.run().await {
            error!("{:?}", err);
        }
    });

    Ok(p2p_task)
}
