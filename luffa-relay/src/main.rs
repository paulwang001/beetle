use std::{sync::Arc, time::Duration};

#[allow(unused_imports)]
use anyhow::{anyhow, Result};
use cid::Cid;
use clap::Parser;
use libipld::cbor::DagCborCodec;
use libp2p::gossipsub::{GossipsubMessage, TopicHash};
use luffa_node::{GossipsubEvent, NetworkEvent};
use luffa_relay::{
    api::P2pClient,
    cli::Args,
    config::{Config, CONFIG_FILE_NAME, ENV_PREFIX},
    mem_p2p::{self, start_store},
};
use luffa_rpc_types::{AppStatus, ContactsTypes, Event, FeedbackStatus};
use multihash::MultihashDigest;
use serde::{Deserialize, Serialize};
// use luffa_util::lock::ProgramLock;
use luffa_rpc_types::Message;
use luffa_util::{luffa_config_path, make_config};
use petgraph::{
    graph::{NodeIndex, UnGraph},
    prelude::*,
    EdgeType, Graph,
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

const TOPIC_STATUS: &str = "luffa_status";
const TOPIC_RELAY: &str = "luffa_relay";
const TOPIC_CONTACTS: &str = "luffa_contacts";
const TOPIC_CHAT: &str = "luffa_chat";
// const TOPIC_CONTACTS_SCAN: &str = "luffa_contacts_scan_answer";

#[derive(Debug, Serialize, Deserialize)]
struct Node {
    did: u64,
    last_time: u64,
    meta: NodeTypes,
}

#[derive(Debug, Serialize, Deserialize)]
struct Edge {
    last_time: u64,
    meta: EdgeTypes,
}

#[derive(Debug, Serialize, Deserialize)]
enum NodeTypes {
    Client,
    Relay,
    Group,
}
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
enum EdgeTypes {
    Connect,
    Private,
    Group,
}

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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    now.as_millis() as u64
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // let mut lock = ProgramLock::new("luffa-relay")?;
    // lock.acquire_or_exit();

      

    let args = Args::parse();
    
    // if args.cfg.is_none() {
    //     test();
    //     return Ok(());
    // }

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
    let mut net_graph =
        UnGraph::<Node, Edge>::with_capacity(1024, 1024);
    let mut contacts_graph =
        UnGraph::<Node, Edge>::with_capacity(1024, 1024);

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
    let my_idx = net_graph.add_node(Node {
        did: my_id,
        last_time: get_now(),
        meta: NodeTypes::Relay,
    });
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
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            if let Ok(_) = tokio::time::timeout(Duration::from_secs(5), async {
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
                let tasks = {
                    let notice = notice_queue_t.read().await;
                    notice.iter().filter(|(_k,(t,f,c))| *t + 30000 < get_now()).map(|(k,_)| *k).collect::<Vec<_>>()
                };
                let mut notice = notice_queue_t.write().await;
                for task in tasks {
                    if let Some((t,f,c)) = notice.remove(&task) {
                        if let Some(api) = push_api.as_ref() {
                            let nb = NoticeBody {
                                id:format!("{task}"),
                                title:format!("luffa://open/chat?id={f}&type={c}"),
                                body:format!("{}",t),
                                current_key:format!("AIzaSyCG7wT4KYvbSf_HYU6xAmn7g5bgKOdGb0s")
                            };
                            if let Err(e) = post.post(api).json(&nb).send().await {
                                tracing::warn!("{e:?}");
                            }                        
                        }
                    }
                }
                
            }
        }
    });
    let process = tokio::spawn(async move {
        while let Some(evt) = events.recv().await {
            match evt {
                NetworkEvent::RequestResponse(rsp)=>{
                    tracing::warn!("request>>> {rsp:?}");
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
                                crc,
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
                                        Message::ContactsSync { did, contacts } => {
                                            let from = take_node(
                                                &mut contacts_graph,
                                                did,
                                                NodeTypes::Client,
                                            );
                                            let mut is_connected = false;
                                            if let Some(_n) = net_graph.find_edge(my_idx, from) {
                                                is_connected = true;
                                                // TODO closest
                                                // let client_t = client.clone();
                                                // tokio::spawn(async move {
                                                //     tracing::warn!("relay subscribe for did: {}",did);
                                                //     let topic = TopicHash::from_raw(format!(
                                                //         "{}_{}",
                                                //         TOPIC_CHAT, did
                                                //     ));
                                                //     if let Err(e) =
                                                //         client_t.gossipsub_subscribe(topic).await
                                                //     {
                                                //         error!("{e:?}");
                                                //     }
                                                // });
                                            }
                                            for ctt in contacts {
                                                let (meta, tp) =
                                                    if ctt.r#type == ContactsTypes::Group {
                                                        (EdgeTypes::Group, NodeTypes::Group)
                                                    } else {
                                                        (EdgeTypes::Private, NodeTypes::Client)
                                                    };
                                                // if is_connected
                                                //     && ctt.r#type == ContactsTypes::Group
                                                // {
                                                //     let client_t = client.clone();
                                                //     tokio::spawn(async move {
                                                //         let topic = TopicHash::from_raw(format!(
                                                //             "{}_{}",
                                                //             TOPIC_CHAT, ctt.did
                                                //         ));
                                                //         if let Err(e) = client_t
                                                //             .gossipsub_subscribe(topic)
                                                //             .await
                                                //         {
                                                //             error!("{e:?}");
                                                //         }
                                                //     });
                                                // }
                                                let to =
                                                    take_node(&mut contacts_graph, ctt.did, tp);
                                                contacts_graph.update_edge(
                                                    to,
                                                    from.clone(),
                                                    Edge {
                                                        last_time: event_time,
                                                        meta,
                                                    },
                                                );
                                            }
                                        }
                                        Message::RelayNode { did } => {
                                            let from =
                                                take_node(&mut net_graph, did, NodeTypes::Relay);
                                            let w = &mut net_graph[from];
                                            w.last_time = event_time;
                                            w.meta = NodeTypes::Relay;
                                        }
                                        Message::StatusSync {
                                            from_id, status, ..
                                        } => {
                                            let from = take_node(
                                                &mut net_graph,
                                                from_id,
                                                NodeTypes::Client,
                                            );
                                            let to = take_node(
                                                &mut net_graph,
                                                from_id,
                                                NodeTypes::Relay,
                                            );
                                            match net_graph.find_edge(from.clone(), to.clone()) {
                                                Some(i) => match status {
                                                    AppStatus::Active | AppStatus::Connected => {
                                                        let e =
                                                            net_graph.edge_weight_mut(i).unwrap();
                                                        e.last_time = event_time;

                                                        let w = &mut net_graph[from];
                                                        w.last_time = event_time;

                                                        let w = &mut net_graph[to];
                                                        w.last_time = event_time;
                                                    }
                                                    AppStatus::Disconnected
                                                    | AppStatus::Deactive => {
                                                        net_graph.remove_edge(i);
                                                    }
                                                    _ => {}
                                                },
                                                None => match status {
                                                    AppStatus::Active | AppStatus::Connected => {
                                                        net_graph.update_edge(
                                                            from,
                                                            to,
                                                            Edge {
                                                                last_time: event_time,
                                                                meta: EdgeTypes::Connect,
                                                            },
                                                        );
                                                    }
                                                    _ => {}
                                                },
                                            }
                                        }
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
                            } else {
                                // todo!()
                                tracing::warn!("encrypt msg:{from_id} -> {to}");
                                let from =
                                    take_node(&mut contacts_graph, from_id, NodeTypes::Client);
                                if let Some(idx) = contacts_graph.node_indices().find(|n| {
                                    let w = &contacts_graph[*n];
                                    w.did == to
                                }) {
                                    let w = &contacts_graph[idx];
                                    match w.meta {
                                        NodeTypes::Group => {
                                            let mut peers = contacts_graph.neighbors(idx).into_iter();
                                            while let Some(pp) = peers.next() {
                                                let relay_peers = net_graph.neighbors(pp);
                                                let relay_peers =
                                                relay_peers.into_iter().collect::<Vec<_>>();
                                                if relay_peers.is_empty() {
                                                    let data = data.clone();
                                                    let client_t = client.clone();
                                                    let key = Cid::new_v1(
                                                        DagCborCodec.into(),
                                                        multihash::Code::Sha2_256.digest(&data),
                                                    );
                                                    // let mut queue = notice_queue.write().await;
                                                    // queue.insert(crc, key.clone());
                                                    tokio::spawn(async move {
                                                        let expires = Some(event_time + 24 * 60 * 60 * 1000);
                                                        if let Err(e) = client_t
                                                            .put_crc_record(
                                                                crc,
                                                                bytes::Bytes::from(data),
                                                                None,
                                                                expires,
                                                            )
                                                            .await
                                                        {
                                                            error!("{e:?}");
                                                        } else {
                                                            //TODO notice send
                                                        }
                                                    });
                                                }
                                            }
                                        }
                                        NodeTypes::Client => {
                                            // target connect to this relay?

                                            // user is friend? but exchange msg.
                                            match contacts_graph.find_edge(from, idx) {
                                                Some(n) => {}
                                                None => {
                                                    tracing::warn!("")
                                                }
                                            }
                                            let relay_peers = net_graph.neighbors(idx);
                                            let relay_peers =
                                                relay_peers.into_iter().collect::<Vec<_>>();
                                            if relay_peers.is_empty() {
                                                let client_t = client.clone();
                                                // let key = Cid::new_v1(
                                                //     DagCborCodec.into(),
                                                //     multihash::Code::Sha2_256.digest(&data),
                                                // );
                                                let notice = notice_queue.clone();
                                                tokio::spawn(async move {
                                                    let time = event_time / 1000;
                                                    let expires = Some(time + 24 * 60 * 60);
                                                    
                                                    if let Err(e) = client_t
                                                    .put_crc_record(
                                                        crc,
                                                        bytes::Bytes::from(data),
                                                        None,
                                                        expires,
                                                    )
                                                    .await
                                                    {
                                                        error!("notify put record>>> {e:?}");
                                                    } else {
                                                        //TODO notice send
                                                        let mut queue = notice.write().await;
                                                        let (time,count,_) = queue.entry(to).or_insert((get_now(),from_id,0));
                                                        *time = get_now();
                                                        tracing::warn!("TODO: offline notify");
                                                    }
                                                });
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                // let to = take_node(&mut graph, to, NodeTypes::Client);
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
                    
                    match net_graph.node_indices().find(|idx| {
                        let w = &net_graph[*idx];
                        w.did == u_id
                    }) {
                        Some(idx) => {
                            let edge = Edge {
                                last_time: get_now(),
                                meta: EdgeTypes::Connect,
                            };
                            net_graph.update_edge(my_idx.clone(), idx.clone(), edge);
                        }
                        None => {
                            let idx = net_graph.add_node(Node {
                                did: u_id,
                                last_time: get_now(),
                                meta: NodeTypes::Client,
                            });
                            let edge = Edge {
                                last_time: get_now(),
                                meta: EdgeTypes::Connect,
                            };
                            net_graph.update_edge(my_idx.clone(), idx, edge);
                        }
                    }
                    let msg = luffa_rpc_types::Message::StatusSync {
                        to: u_id,
                        from_id: my_id,
                        status: AppStatus::Connected,
                    };
                    let event = luffa_rpc_types::Event::new(0, &msg, None, u_id);
                    let event = event.encode().unwrap();
                    if let Err(e) = client
                        .gossipsub_publish(
                            TopicHash::from_raw(TOPIC_STATUS),
                            bytes::Bytes::from(event),
                        )
                        .await
                    {
                        tracing::warn!("{e:?}");
                    }
                    // let topic = TopicHash::from_raw(format!(
                    //     "{}_{}",
                    //     TOPIC_CHAT, u_id
                    // ));
                    // match client.gossipsub_subscribe(topic).await {
                    //     Ok(ret)=>{
                    //         tracing::warn!("sub result: {} for {}",ret,u_id);
                    //     }
                    //     Err(e)=>{
                    //         tracing::warn!("sub error:{:?}",e);
                    //     }
                    // }
                }
                NetworkEvent::PeerDisconnected(peer_id) => {
                    tracing::info!("---------PeerDisconnected-----------{:?}", peer_id);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let u_id = digest.sum64();
                    match net_graph.node_indices().find(|idx| {
                        let w = &net_graph[*idx];
                        w.did == u_id
                    }) {
                        Some(idx) => {
                            while let Some(i) = net_graph.find_edge(my_idx.clone(), idx.clone()) {
                                let e = net_graph.edge_weight(i).unwrap();
                                if e.meta == EdgeTypes::Connect {
                                    net_graph.remove_edge(i);
                                }
                            }
                        }
                        None => {}
                    }
                    let msg = luffa_rpc_types::Message::StatusSync {
                        to: u_id,
                        from_id: my_id,
                        status: AppStatus::Disconnected,
                    };
                    let event = luffa_rpc_types::Event::new(0, &msg, None, u_id);
                    let event = event.encode().unwrap();
                    if let Err(e) = client
                        .gossipsub_publish(
                            TopicHash::from_raw(TOPIC_STATUS),
                            bytes::Bytes::from(event),
                        )
                        .await
                    {
                        tracing::warn!("{e:?}");
                    }
                    
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

fn take_node(g: &mut UnGraph<Node, Edge>, did: u64, node_type: NodeTypes) -> NodeIndex {
    match g.node_indices().find(|n| {
        let w = &g[*n];
        w.did == did
    }) {
        Some(i) => i,
        None => g.add_node(Node {
            did,
            last_time: get_now(),
            meta: node_type,
        }),
    }
}


fn test() {
    let mut g = DiGraph::<u64,u64>::new();
    
    let a = g.add_node(1);
    let b = g.add_node(2);
    let c = g.add_node(3);
    let d = g.add_node(4);

    let ab = g.add_edge(a, b, 1);
    let ac = g.add_edge(a, c, 2);
    let ad = g.add_edge(a, d, 3);
    
    let ba = g.add_edge(b, a, 4);
    let bc = g.add_edge(b, c, 5);
    let bd = g.add_edge(b, d, 6);
    
    let bc2 = g.add_edge(b, c, 51);
    
   

    if let Some(e) = g.first_edge(c, petgraph::Direction::Incoming) {
        let nn = g.edge_weight(e).unwrap();
        println!("nn>> {nn}");
        assert_eq!(e,bc2,"edge not match.");
        let (ia,ic) = g.edge_endpoints(e).unwrap();
        assert_eq!(ia,b,"failed");
        let mut e = e;
        while let Some(n) = g.next_edge(e, petgraph::Direction::Incoming) {
            let nn = g.edge_weight(n).unwrap();
            println!("nn>> {nn}");
            e = n;
        }
    } 

    let ad = g.add_edge(a, d, 6);
    let ad = g.add_edge(a, d, 7);
    let ad = g.add_edge(a, d, 8);
    let ad = g.add_edge(a, d, 9);
    let ad = g.add_edge(a, d, 10);
    let eg  = g.edges_connecting(a, d);

    let wts = eg.map(|ef| ef.weight()).collect::<Vec<_>>();
     println!("{wts:?}");
     
     g.remove_edge(ad);

     
     let eg  = g.edges_connecting(a, d);
     let mut removes = vec![];
     for n in eg {
         let xx = n.id();
         removes.push(xx);
     }
     for rm in removes {
        g.remove_edge(rm);
     }   
     let eg  = g.edges_connecting(a, d);
     let wts = eg.map(|ef| ef.weight()).collect::<Vec<_>>();
      println!("{wts:?}");
    

}