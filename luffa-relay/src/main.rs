#[allow(unused_imports)]
use anyhow::{anyhow, Result};
use clap::Parser;
use libp2p::gossipsub::{GossipsubMessage, TopicHash};
use luffa_node::{GossipsubEvent, NetworkEvent};
use luffa_relay::{
    cli::Args,
    config::{Config, CONFIG_FILE_NAME, ENV_PREFIX},
};
use luffa_rpc_types::{
    im::{AppStatus, Event},
    p2p::P2pAddr,
    Addr,
};
// use luffa_util::lock::ProgramLock;
use luffa_rpc_client::P2pClient;
use luffa_util::{luffa_config_path, make_config};
use tracing::debug;

const TOPIC_STATUS: &str = "luffa_status";
const TOPIC_RELAY: &str = "luffa_relay";
const TOPIC_CONTACTS: &str = "luffa_contacts";
const TOPIC_CONTACTS_SCAN: &str = "luffa_contacts_scan_answer";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // let mut lock = ProgramLock::new("luffa-relay")?;
    // lock.acquire_or_exit();

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
    // let (tx, rx) = tokio::sync::mpsc::channel::<NetworkEvent>(4096);
    let (key, peer, store_rpc, p2p_rpc, mut events) = {
        let store_recv = Addr::new_mem();
        let store_sender = store_recv.clone();
        let p2p_recv = match config.rpc_client.p2p_addr.as_ref() {
            Some(addr) => addr.clone(),
            None => Addr::new_mem(),
        };
        // let p2p_recv = Addr::from_str("irpc://0.0.0.0:8887").unwrap();
        // let p2p_recv = Addr::new_mem();
        // let p2p_sender = p2p_recv.clone();
        config.rpc_client.store_addr = Some(store_sender);
        // config.rpc_client.p2p_addr = Some(p2p_sender);
        config.synchronize_subconfigs();

        let store_rpc = luffa_relay::mem_store::start(store_recv, config.store.clone()).await?;

        let (key, peer_id, p2p_rpc, events) =
            luffa_relay::mem_p2p::start(p2p_recv, config.p2p.clone()).await?;
        (key,peer_id,store_rpc, p2p_rpc,events)
    };

    config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);
    debug!("{config:#?}");

    let metrics_config = config.metrics.clone();

    let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
        .await
        .expect("failed to initialize metrics");

    let mut digest = crc64fast::Digest::new();
    digest.write(&peer.to_bytes());
    let my_id = digest.sum64();

    let client = P2pClient::new(P2pAddr::new_mem()).await?;
    client
        .gossipsub_subscribe(TopicHash::from_raw(format!(
            "{}-{}",
            TOPIC_CONTACTS_SCAN, my_id
        )))
        .await?;
    let topics = vec![TOPIC_RELAY, TOPIC_STATUS, TOPIC_CONTACTS];
    for t in topics.into_iter() {
        client.gossipsub_subscribe(TopicHash::from_raw(t)).await?;
    }
    let msg = luffa_rpc_types::im::Message::RelayNode { did: my_id };
    let event = luffa_rpc_types::im::Event::new::<Vec<u8>>(my_id, msg, None);
    let event = event.encode()?;
    client
        .gossipsub_publish(TopicHash::from_raw(TOPIC_RELAY), bytes::Bytes::from(event))
        .await?;
    let process = tokio::spawn(async move {
        while let Some(evt) = events.recv().await {
            match evt {
                NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {}
                NetworkEvent::Gossipsub(GossipsubEvent::Message { message, from, id }) => {
                    let GossipsubMessage { data, .. } = message;
                    if let Ok(im) = Event::decode(data) {
                        let Event {
                            did,
                            event_time,
                            msg,
                            nonce,
                            ..
                        } = im;
                        // TODO check did status
                        if nonce.is_none() {
                            if let Ok(msg) = luffa_rpc_types::im::Message::decrypt(
                                bytes::Bytes::from(msg),
                                None,
                                nonce,
                            ) {
                                todo!()
                            }
                        } else {
                            todo!()
                        }
                    }
                }
                NetworkEvent::Gossipsub(GossipsubEvent::Unsubscribed { peer_id, topic }) => {}
                NetworkEvent::PeerConnected(peer_id) => {
                    tracing::info!("---------PeerConnected-----------{:?}", peer_id);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let u_id = digest.sum64();
                    let msg = luffa_rpc_types::im::Message::StatusSync {
                        did: u_id,
                        status: AppStatus::Connected,
                    };
                    let event = luffa_rpc_types::im::Event::new::<Vec<u8>>(u_id, msg, None);
                    let event = event.encode().unwrap();
                    client
                        .gossipsub_publish(
                            TopicHash::from_raw(TOPIC_STATUS),
                            bytes::Bytes::from(event),
                        )
                        .await
                        .unwrap();
                }
                NetworkEvent::PeerDisconnected(peer_id) => {
                    tracing::info!("---------PeerDisconnected-----------{:?}", peer_id);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let u_id = digest.sum64();
                    let msg = luffa_rpc_types::im::Message::StatusSync {
                        did: u_id,
                        status: AppStatus::Disconnected,
                    };
                    let event = luffa_rpc_types::im::Event::new::<Vec<u8>>(u_id, msg, None);
                    let event = event.encode().unwrap();
                    client
                        .gossipsub_publish(
                            TopicHash::from_raw(TOPIC_STATUS),
                            bytes::Bytes::from(event),
                        )
                        .await
                        .unwrap();
                }
                NetworkEvent::CancelLookupQuery(peer_id) => {
                    tracing::info!("---------CancelLookupQuery-----------{:?}", peer_id);
                }
            }
        }
    });

    luffa_util::block_until_sigint().await;
    process.abort();
    store_rpc.abort();
    p2p_rpc.abort();

    metrics_handle.shutdown();
    Ok(())
}
