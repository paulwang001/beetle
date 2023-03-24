use std::{sync::Arc, time::Duration};

#[allow(unused_imports)]
use anyhow::{anyhow, Result};
use chrono::Utc;
use clap::Parser;
use libp2p::gossipsub::{GossipsubMessage, TopicHash};
use luffa_node::{GossipsubEvent, NetworkEvent, ChatEvent};
use luffa_relay::{
    api::P2pClient,
    cli::Args,
    config::{Config, CONFIG_FILE_NAME, ENV_PREFIX},
    mem_p2p::{self, start_store},
};
use luffa_rpc_types::{AppStatus, Event, FeedbackStatus};
use serde::{Deserialize, Serialize};
// use luffa_util::lock::ProgramLock;
use luffa_rpc_types::Message;
use luffa_util::{luffa_config_path, make_config};

use tokio::sync::RwLock;

const TOPIC_STATUS: &str = "luffa_status";
const TOPIC_RELAY: &str = "luffa_relay";
const TOPIC_CHAT: &str = "luffa_chat";

#[derive(Debug, Serialize, Deserialize)]
struct NoticeBody {
    #[serde(rename = "ID")]
    id:String,
    title:String,
    body:String,
    #[serde(rename = "currentKey")]
    current_key:String,
}

fn get_now() -> u64 {
    Utc::now().timestamp_millis() as u64
}

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

    config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);
    tracing::info!("-------");


    let metrics_config = config.metrics.clone();

    let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
        .await
        .expect("failed to initialize metrics");

    #[cfg(unix)]
    {
        match luffa_util::increase_fd_limit() {
            Ok(soft) => tracing::debug!("NOFILE limit: soft = {}", soft),
            Err(err) => tracing::error!("Error increasing NOFILE limit: {}", err),
        }
    }
    
    tracing::info!("-------");
    let (key, peer, p2p_rpc, mut events, sender) = {
        let store = start_store(config.store.clone()).await.unwrap();
        let (key, peer_id, p2p_rpc, events, sender) =
            mem_p2p::start(config.p2p.clone(), Arc::new(store)).await?;
        (key, peer_id, p2p_rpc, events, sender)
    };

    let mut digest = crc64fast::Digest::new();
    digest.write(&peer.to_bytes());
    let my_id = digest.sum64();
    tracing::info!("started> did:{my_id}");
    
    let client = Arc::new(luffa_node::rpc::P2p::new(sender));
    let client = Arc::new(P2pClient::new(client).unwrap());
    let notice_queue = Arc::new(RwLock::new(std::collections::BTreeMap::<u64, (u64,u64,u8)>::new()));
    let post = reqwest::ClientBuilder::default()
    .connect_timeout(Duration::from_millis(5000))
    .timeout(Duration::from_millis(5000)).build().unwrap();
    let push_api = config.push_api;//.unwrap_or(format!("https://luffa.putdev.com/post/sendMessage")); 
    tracing::info!("mem rpc client open.");
    let client_t = client.clone();
    let notice_queue_t = notice_queue.clone();
    let pub_sub = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let mut has_err = false;
            if let Ok(_) = tokio::time::timeout(Duration::from_secs(5), async {
                tracing::info!("client subscribe.");
                let topics = vec![TOPIC_RELAY, TOPIC_STATUS,TOPIC_CHAT];
                for t in topics.into_iter() {
                    if let Err(e) = client_t.gossipsub_subscribe(TopicHash::from_raw(t)).await {
                        tracing::warn!("{e:?}");
                        has_err = true;
                        break;
                    }
                }
            })
            .await
            {
                if !has_err {
                    tracing::info!("pub sub successfully.");
                    break;
                }
            }
        }
        let mut count = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if count % 6 == 0 {
                if let Ok(_) = tokio::time::timeout(Duration::from_secs(10), async {
                    tracing::info!("client publicsh relay.");
                    let msg = luffa_rpc_types::Message::RelayNode { did: my_id };
                    let event = luffa_rpc_types::Event::new(0, &msg, None, my_id);
                    let event = event.encode().unwrap();
                    if let Err(e) = client_t
                        .gossipsub_publish(TopicHash::from_raw(TOPIC_RELAY), bytes::Bytes::from(event))
                        .await
                    {
                        tracing::warn!("{e:?}");
                    }
                })
                .await
                {
                    tracing::info!("relay successfully.");
                    if let Ok(peers) = client_t.gossipsub_mesh_peers(TopicHash::from_raw(TOPIC_RELAY)).await {
                        tracing::warn!("mesh peers:{:?}",peers);
                    }
                }
            }
            count += 1;
            let tasks = {
                let notice = notice_queue_t.read().await;
                // notice.iter().filter(|(_k,(t,f,c))| *t + 5000 < get_now()).map(|(k,_)| *k).collect::<Vec<_>>()
                notice.iter().map(|(k,_)| *k).collect::<Vec<_>>()
            };
            let mut notice = notice_queue_t.write().await;
            if tasks.is_empty() {
                tracing::warn!("notice task is empty!");
            }

            for task in tasks {
                if let Some((t,f,c)) = notice.remove(&task) {
                    if let Some(api) = push_api.as_ref() {
                        let nb = NoticeBody {


                            id:format!("{task}"),
                            title:format!("luffa://open/chat?id={task}&type={c}"),
                            body:format!("{}",t),
                            current_key:format!("AIzaSyCG7wT4KYvbSf_HYU6xAmn7g5bgKOdGb0s")
                        };

                        tracing::warn!("post:{nb:?}");
                        if let Err(e) = post.post(api).json(&nb).send().await {
                            tracing::warn!("{e:?}");
                        }                        
                    }
                    else{
                        tracing::warn!("post api not found!!!!");
                    }
                }
            }
        }
    });
    let process = tokio::spawn(async move {
        while let Some(evt) = events.recv().await {
            match evt {
                NetworkEvent::RequestResponse(rsp)=>{
                    // tracing::warn!("request>>> {rsp:?}");
                    match rsp {
                        ChatEvent::Request(data)=>{
                            match Event::decode_uncheck(&data) {
                                Ok(im) => {
                                    let Event {
                                        to,
                                        from_id,
                                        ..
                                    } = im;

                                    tracing::warn!("request msg:{from_id} -> {to}");
                                    let notice = notice_queue.clone();
                                    let mut queue = notice.write().await;
                                    let (time,count,_) = queue.entry(to).or_insert((get_now(),from_id,0));
                                    *time = get_now();
                                    *count += 1;
                                    tracing::warn!("TODO: offline notify");

                                }
                                _=>{

                                }
                            }
                        }
                        _=> {

                        }
                    }
                }
                NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {
                    tracing::warn!("Subscribed>>>> peer_id: {peer_id:?} topic: {topic:?}");
                }
                NetworkEvent::Gossipsub(GossipsubEvent::Message { message, from, id }) => {
                    let GossipsubMessage { data, .. } = message;
                    match Event::decode(&data) {
                        Ok(im) => {
                            let Event {
                                to,
                                event_time,
                                msg,
                                nonce,
                                from_id,
                                ..
                            } = im;
                            // TODO check did status
                            if nonce.is_none() {
                                tracing::debug!("------- nonce is None");
                                if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                    bytes::Bytes::from(msg),
                                    None,
                                    nonce,
                                ) {
                                    tracing::info!(
                                        "msg>>>>[{event_time}] from: {from_id} to:{to} msg:{msg:?}"
                                    );
                                    match msg {
                                        Message::Feedback { crc, status } => match status {
                                            FeedbackStatus::Fetch => {}
                                            FeedbackStatus::Notice => {
                                                let mut queue = notice_queue.write().await;
                                                queue.remove(&crc);
                                            }
                                            FeedbackStatus::Reach => {
                                                let mut queue = notice_queue.write().await;
                                                queue.remove(&crc);
                                            }
                                            FeedbackStatus::Read => {}
                                        },
                                        _ => {}
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("{e:?}");
                        }
                    }
                }
                NetworkEvent::Gossipsub(GossipsubEvent::Unsubscribed { peer_id, topic }) => {}
                NetworkEvent::PeerConnected(peer_id) => {
                    tracing::info!("---------PeerConnected-----------{:?}", peer_id);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let u_id = digest.sum64();
                    {
                        let mut queue = notice_queue.write().await;
                        queue.remove(&u_id);
                    }
                    
                    let msg = luffa_rpc_types::Message::StatusSync {
                        to: u_id,
                        from_id: my_id,
                        status: AppStatus::Connected,
                    };
                    let event = luffa_rpc_types::Event::new(0, &msg, None, u_id);
                    let event = event.encode().unwrap();
                    let client = client.clone();
                    tokio::spawn(async move{
                        if let Err(e) = client
                            .gossipsub_publish(
                                TopicHash::from_raw(TOPIC_STATUS),
                                bytes::Bytes::from(event),
                            )
                            .await
                        {
                            tracing::warn!("{e:?}");
                        }
                    });
                    
                }
                NetworkEvent::PeerDisconnected(peer_id) => {
                    tracing::info!("---------PeerDisconnected-----------{:?}", peer_id);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let u_id = digest.sum64();
                    
                    let msg = luffa_rpc_types::Message::StatusSync {
                        to: u_id,
                        from_id: my_id,
                        status: AppStatus::Disconnected,
                    };
                    let event = luffa_rpc_types::Event::new(0, &msg, None, u_id);
                    let event = event.encode().unwrap();
                    let client = client.clone();
                    tokio::spawn(async move{
                        if let Err(e) = client
                            .gossipsub_publish(
                                TopicHash::from_raw(TOPIC_STATUS),
                                bytes::Bytes::from(event),
                            )
                            .await
                        {
                            tracing::warn!("{e:?}");
                        }
                    });
                    
                }
                NetworkEvent::CancelLookupQuery(peer_id) => {
                    tracing::info!("---------CancelLookupQuery-----------{:?}", peer_id);
                }
            }
        }
    });
    tracing::info!("ready...");
    luffa_util::block_until_sigint().await;
    pub_sub.abort();
    process.abort();
    p2p_rpc.abort();

    metrics_handle.shutdown();
    Ok(())
}
