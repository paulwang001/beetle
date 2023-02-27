use libp2p::identity::Keypair;
use libp2p::PeerId;
/// A p2p instance listening on a memory rpc channel.
use luffa_node::config::Config;
use luffa_node::{load_identity, DiskStorage, Keychain, NetworkEvent, Node};
use luffa_rpc_types::p2p::P2pAddr;
use tokio::sync::mpsc::Receiver;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::error;

/// Starts a new p2p node, using the given mem rpc channel.
pub async fn start(
    rpc_addr: P2pAddr,
    config: Config,
) -> anyhow::Result<(Keypair, PeerId, JoinHandle<()>, Receiver<NetworkEvent>)> {
    let mut kc = Keychain::<DiskStorage>::new(config.key_store_path.clone()).await?;

    let key = load_identity(&mut kc).await?;
    let mut p2p = Node::new(config, rpc_addr, kc).await?;
    let events = p2p.network_events();
    let local_id = p2p.local_peer_id().clone();
    // Start services
    let p2p_task = task::spawn(async move {
        if let Err(err) = p2p.run().await {
            error!("{:?}", err);
        }
    });
    Ok((key,local_id,p2p_task,events))
}
