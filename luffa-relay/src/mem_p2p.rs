use std::sync::Arc;

use anyhow::Context;
use libp2p::identity::Keypair;
use libp2p::PeerId;
/// A p2p instance listening on a memory rpc channel.
use luffa_node::config::Config;
use luffa_node::rpc::RpcMessage;
use luffa_node::{load_identity, DiskStorage, Keychain, NetworkEvent, Node};
use luffa_store::Store;
use luffa_store::Config as StoreConfig;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Starts a new p2p node, using the given mem rpc channel.
pub async fn start(
    config: Config,
    store:Arc<luffa_store::Store>,
) -> anyhow::Result<(Keypair, PeerId, JoinHandle<()>, Receiver<NetworkEvent>,Sender<RpcMessage>)> {
    let mut kc = Keychain::<DiskStorage>::new(config.key_store_path.clone()).await?;

    let key = load_identity(&mut kc,None).await?;
    
    let (mut p2p,sender) = Node::new(config, kc,store,Some("Relay".to_string()),None).await?;
    let events = p2p.network_events();
    let local_id = p2p.local_peer_id().clone();
    tracing::info!("peer: {local_id}");
    // Start services
    let p2p_task = task::spawn(async move {
        if let Err(err) = p2p.run().await {
            error!("{:?}", err);
        }
    });
    Ok((key,local_id,p2p_task,events,sender))
}


/// Starts a new store, using the given mem rpc channel.
pub async fn start_store(config: StoreConfig) -> anyhow::Result<luffa_store::Store> {
    // This is the file RocksDB itself is looking for to determine if the database already
    // exists or not.  Just knowing the directory exists does not mean the database is
    // created.
    // let marker = config.path.join("CURRENT");

    tracing::info!("Opening store at {}", config.path.display());
    let store = Store::create(config)
        .await
        .context("failed to open existing store")?;
    Ok(store)
}