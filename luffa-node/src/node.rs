use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::AHashMap;
use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use cid::Cid;
use futures_util::stream::StreamExt;
use libipld::{
    cbor::DagCborCodec,
    prelude::{Codec, Decode, Encode},
    Ipld, IpldCodec,
};
use libp2p::core::Multiaddr;
use libp2p::gossipsub::{GossipsubMessage, MessageId, TopicHash};
pub use libp2p::gossipsub::{IdentTopic, Topic};
use libp2p::identify::{Event as IdentifyEvent, Info as IdentifyInfo};
use libp2p::identity::Keypair;
use libp2p::kad::kbucket::{Distance, NodeStatus};
use libp2p::kad::{
    self, BootstrapOk, GetClosestPeersError, GetClosestPeersOk, GetProvidersOk, GetRecordOk,
    KademliaEvent, PeerRecord, QueryId, QueryResult, Quorum, Record,
};
use libp2p::mdns;
use libp2p::metrics::Recorder;
use libp2p::multiaddr::Protocol;
use libp2p::ping::Result as PingResult;
use libp2p::ping;
use libp2p::request_response::{
    InboundFailure, OutboundFailure, RequestId, RequestResponseEvent, RequestResponseMessage,
};
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{ConnectionHandler, IntoConnectionHandler, NetworkBehaviour, SwarmEvent};
use libp2p::{PeerId, Swarm};
use luffa_bitswap::{BitswapEvent, Block};
use luffa_metrics::{core::MRecorder, inc, libp2p_metrics, p2p::P2PMetrics};
use luffa_rpc_types::p2p::{ChatRequest, ChatResponse};
use luffa_rpc_types::{AppStatus, ChatContent, ContactsTypes, Message, FeedbackStatus};
use multihash::MultihashDigest;
use petgraph::algo::astar;
use petgraph::prelude::*;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::{self, Sender as OneShotSender};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use crate::behaviour::{Event, NodeBehaviour};
use crate::config::Config;
use crate::keys::{KeyFilter, Keychain, Storage};
use crate::providers::Providers;
use crate::rpc::{self, ProviderRequestKey, RpcMessage};
use crate::swarm::build_swarm;

const TOPIC_STATUS: &str = "luffa_status";
const TOPIC_CHAT: &str = "luffa_chat";

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    Gossipsub(GossipsubEvent),
    RequestResponse(ChatEvent),
    CancelLookupQuery(PeerId),
    Ping(PingInfo),
}

#[derive(Debug, Clone)]
pub struct PingInfo {
    pub peer: PeerId,
    pub ttl: Duration,
}

#[derive(Debug, Clone)]
enum ConnectionEdge {
    Local(PeerId),
    Remote(u64),
}

/// The events emitted by a [`RequestResponse`] protocol.
#[derive(Debug, Clone)]
pub enum ChatEvent {
    Request(Vec<u8>),
    Response {
        request_id: Option<RequestId>,
        data: Vec<u8>,
    },
    /// An outbound request failed.
    OutboundFailure {
        /// The peer to whom the request was sent.
        peer: PeerId,
        /// The (local) ID of the failed request.
        request_id: RequestId,
        /// The error that occurred.
        error: OutboundFailure,
    },
    /// An inbound request failed.
    InboundFailure {
        /// The peer from whom the request was received.
        peer: PeerId,
        /// The ID of the failed inbound request.
        request_id: RequestId,
        /// The error that occurred.
        error: InboundFailure,
    },
    /// A response to an inbound request has been sent.
    ///
    /// When this event is received, the response has been flushed on
    /// the underlying transport connection.
    ResponseSent {
        /// The peer to whom the response was sent.
        peer: PeerId,
        /// The ID of the inbound request whose response was sent.
        request_id: RequestId,
    },
}

#[derive(Debug, Clone)]
pub enum GossipsubEvent {
    Subscribed {
        peer_id: PeerId,
        topic: TopicHash,
    },
    Unsubscribed {
        peer_id: PeerId,
        topic: TopicHash,
    },
    Message {
        from: PeerId,
        id: MessageId,
        message: GossipsubMessage,
    },
}

pub struct Node<KeyStorage: Storage> {
    swarm: Swarm<NodeBehaviour>,
    net_receiver_in: Receiver<rpc::RpcMessage>,
    chat_receiver: Receiver<(crate::behaviour::chat::Response, PeerId)>,
    chat_sender: Arc<Sender<(crate::behaviour::chat::Response, PeerId)>>,
    routing_tx: Sender<(PeerId, Vec<u64>)>,
    routing_rx: Receiver<(PeerId, Vec<u64>)>,
    dial_queries: AHashMap<PeerId, Vec<OneShotSender<Result<()>>>>,
    pending_routing: AHashMap<u64, Vec<(u64, Instant)>>,
    connected_peers: AHashMap<u64, PeerId>,
    lookup_queries: AHashMap<PeerId, Vec<oneshot::Sender<Result<IdentifyInfo>>>>,
    // TODO(ramfox): use new providers queue instead
    find_on_dht_queries: AHashMap<Vec<u8>, DHTQuery>,
    record_on_dht_queries: AHashMap<Vec<u8>, (QueryId, Vec<OneShotSender<Result<Option<Record>>>>)>,
    provider_on_dht_queries: AHashMap<Vec<u8>, (QueryId, Vec<OneShotSender<Result<QueryId>>>)>,
    pending_request: AHashMap<RequestId, (u64, u64, OneShotSender<Result<Option<ChatResponse>>>)>,
    network_events: Vec<Sender<NetworkEvent>>,
    _keychain: Keychain<KeyStorage>,
    #[allow(dead_code)]
    kad_last_range: Option<(Distance, Distance)>,
    use_dht: bool,
    bitswap_sessions: BitswapSessions,
    providers: Providers,
    listen_addrs: Vec<Multiaddr>,
    store: Arc<luffa_store::Store>,
    cache: DiGraph<u64, (u64, u64)>,
    pub_pending: VecDeque<(Vec<u8>, Instant, u8)>,
    connections: DiGraph<u64, ConnectionEdge>,
    contacts: UnGraph<u64, u8>,
    agent: Option<String>,
}

impl<T: Storage> fmt::Debug for Node<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("swarm", &"Swarm<NodeBehaviour>")
            .field("net_receiver_in", &self.net_receiver_in)
            .field("dial_queries", &self.dial_queries)
            .field("lookup_queries", &self.lookup_queries)
            .field("find_on_dht_queries", &self.find_on_dht_queries)
            .field("record_on_dht_queries", &self.record_on_dht_queries)
            .field("provider_on_dht_queries", &self.provider_on_dht_queries)
            .field("network_events", &self.network_events)
            .field("_keychain", &self._keychain)
            .field("kad_last_range", &self.kad_last_range)
            .field("use_dht", &self.use_dht)
            .field("bitswap_sessions", &self.bitswap_sessions)
            .field("providers", &self.providers)
            .finish()
    }
}

// TODO(ramfox): use new providers queue instead
type DHTQuery = (PeerId, Vec<oneshot::Sender<Result<()>>>);

type BitswapSessions = AHashMap<u64, Vec<(oneshot::Sender<()>, JoinHandle<()>)>>;

pub const DEFAULT_PROVIDER_LIMIT: usize = 10;
const NICE_INTERVAL: Duration = Duration::from_secs(6);
const BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(30);
const EXPIRY_INTERVAL: Duration = Duration::from_secs(12);
const PUB_INTERVAL: Duration = Duration::from_secs(1);

impl<KeyStorage: Storage> Node<KeyStorage> {
    pub async fn new(
        config: Config,
        mut keychain: Keychain<KeyStorage>,
        db: Arc<luffa_store::Store>,
        agent: Option<String>,
        filter: Option<KeyFilter>,
    ) -> Result<(Self, Sender<rpc::RpcMessage>)> {
        let (network_sender_in, net_receiver_in) = channel(1024); // TODO: configurable
        let (chat_sender, chat_receiver) = channel(1024); // TODO: configurable
        let chat_sender = Arc::new(chat_sender);
        let Config {
            libp2p: libp2p_config,
            ..
        } = config;

        let (routing_tx,routing_rx) = channel::<(PeerId,Vec<u64>)>(4096);

        let keypair = load_identity(&mut keychain, filter).await?;
        let mut swarm = build_swarm(&libp2p_config, &keypair, db.clone(), agent.clone()).await?;
        let mut listen_addrs = vec![];
        for addr in &libp2p_config.listening_multiaddrs {
            Swarm::listen_on(&mut swarm, addr.clone())?;
            listen_addrs.push(addr.clone());
        }
        let local_peer_id = swarm.local_peer_id().to_string();
        let is_client = match agent.as_ref() {
            Some(a)=>{
                !a.contains("Relay")
            }
            _=> true
        };
        for multiaddr in &libp2p_config.bootstrap_peers {
            // TODO: move parsing into config
            let mut addr = multiaddr.to_owned();
            let add_addr = addr.clone();
            if let Some(Protocol::P2p(mh)) = addr.pop() {
                let peer_id = PeerId::from_multihash(mh).unwrap();
                tracing::info!("add boot>> {:?}  {:?}",peer_id,addr);
                if !libp2p_config.kademlia {
                    swarm.dial(addr)?;
                }
                if is_client{
                    let l_addr = format!("{}/p2p-circuit/p2p/{}",add_addr.clone().to_string(),local_peer_id);
                    tracing::warn!("{l_addr}");
                    let on_addr = Multiaddr::from_str(&l_addr).unwrap();
                    Swarm::listen_on(&mut swarm, on_addr.clone())?;
                    listen_addrs.push(on_addr.clone());
                }
            } else {
                tracing::info!("Could not parse bootstrap addr {}", multiaddr);
            }
        }
        let cache = DiGraph::<u64, (u64, u64)>::new();
        let connections = DiGraph::<u64, ConnectionEdge>::with_capacity(1024, 1024);
        let contacts = UnGraph::<u64, u8>::with_capacity(1024, 1024);
        Ok((
            Node {
                swarm,
                net_receiver_in,
                chat_receiver,
                chat_sender,
                routing_tx,
                routing_rx,
                dial_queries: Default::default(),
                pending_routing: Default::default(),
                connected_peers: Default::default(),
                lookup_queries: Default::default(),
                // TODO(ramfox): use new providers queue instead
                find_on_dht_queries: Default::default(),
                record_on_dht_queries: Default::default(),
                provider_on_dht_queries: Default::default(),
                pending_request: Default::default(),
                network_events: Vec::new(),
                _keychain: keychain,
                kad_last_range: None,
                use_dht: libp2p_config.kademlia,
                bitswap_sessions: Default::default(),
                providers: Providers::new(4),
                listen_addrs,
                store: db,
                cache,
                connections,
                contacts,
                agent,
                pub_pending: Default::default(),
            },
            network_sender_in,
        ))
    }

    pub fn listen_addrs(&self) -> &Vec<Multiaddr> {
        &self.listen_addrs
    }

    pub fn local_peer_id(&self) -> &PeerId {
        self.swarm.local_peer_id()
    }

    /// Starts the libp2p service networking stack. This Future resolves when shutdown occurs.
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("Listen addrs: {:?}", self.listen_addrs());
        tracing::info!("Local Peer ID: {}", self.local_peer_id());

        let mut nice_interval = self
            .use_dht
            .then(|| tokio::time::interval(NICE_INTERVAL));
        let mut bootstrap_interval = tokio::time::interval(BOOTSTRAP_INTERVAL);
        let mut expiry_interval = tokio::time::interval(EXPIRY_INTERVAL);
        let mut pub_interval = tokio::time::interval(PUB_INTERVAL);

        loop {
            inc!(P2PMetrics::LoopCounter);

            tokio::select! {
                swarm_event = self.swarm.next() => {
                    let swarm_event = swarm_event.expect("the swarm will never die");
                    if let Err(err) = self.handle_swarm_event(swarm_event).await {
                        tracing::info!("swarm error: {:?}", err);
                    }

                    if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                        self.providers.poll(kad);
                    }
                }
                rpc_message = self.net_receiver_in.recv() => {
                    match rpc_message {
                        Some(rpc_message) => {
                            match self.handle_rpc_message(rpc_message) {
                                Ok(true) => {
                                    // shutdown
                                    return Ok(());
                                }
                                Ok(false) => {
                                    // continue;
                                }
                                Err(err) => {
                                    tracing::info!("rpc: {:?}", err);
                                }
                            }
                        }
                        None => {
                            // shutdown
                            return Ok(());
                        }
                    }
                }
                routing = self.routing_rx.recv() => {
                    match routing {
                        Some((peer_id,crc))=>{
                            if let Err(e) = self.handle_chat_routing(peer_id,crc).await {
                                tracing::error!("chat routing to [{peer_id:?}] failed: {:?}", e);
                            }
                        }
                        None=>{

                        }
                    }
                }
                res_channel = self.chat_receiver.recv() => {
                    match res_channel {
                        Some((res,peer_id))=>{
                            if let Err(e) = self.handle_chat_response(res,peer_id).await {
                                tracing::error!("chat response to [{peer_id:?}] failed: {:?}", e);
                            }
                        }
                        None=>{

                        }
                    }
                }
                _ = async {
                    if let Some(ref mut nice_interval) = nice_interval {
                        nice_interval.tick().await
                    } else {
                        unreachable!()
                    }
                }, if nice_interval.is_some() => {
                    // Print peer count on an interval.
                    tracing::warn!("[{}] Peers connected: {:?}",self.local_peer_id(), self.swarm.connected_peers().count());
                    self.dht_nice_tick().await;
                }
                _ = bootstrap_interval.tick() => {
                    if let Err(e) = self.swarm.behaviour_mut().kad_bootstrap() {
                        tracing::warn!("kad bootstrap failed: {:?}", e);
                    }
                    else{
                        tracing::debug!("kad bootstrap successfully");
                    }
                }
                _ = expiry_interval.tick() => {
                    if let Err(err) = self.expiry() {
                        tracing::warn!("expiry error {:?}", err);
                    }
                }
                _ = pub_interval.tick() => {
                    self.pub_pending_tick();
                }
            }
        }
    }

    fn expiry(&mut self) -> Result<()> {
        // Cleanup bitswap sessions
        let mut to_remove = Vec::new();
        for (session_id, workers) in &mut self.bitswap_sessions {
            // Check if the workers are still active
            workers.retain(|(_, worker)| !worker.is_finished());

            if workers.is_empty() {
                to_remove.push(*session_id);
            }

            // Only do a small chunk of cleanup on each iteration
            // TODO(arqu): magic number
            if to_remove.len() >= 10 {
                break;
            }
        }

        for session_id in to_remove {
            let (s, _r) = oneshot::channel();
            self.destroy_session(session_id, s);
        }

        Ok(())
    }

    /// Re put pending routing.
    #[tracing::instrument(skip(self))]
    fn pub_pending_tick(&mut self) {
        while let Some((evt, t, c)) = self.pub_pending.pop_back() {
            if c >= 6 {
                tracing::error!("pub chat event is timeout!!!");
                continue;
            }
            if t.elapsed().as_millis() < c as u128 * 500 {
                self.pub_pending.push_back((evt, t,c));
                continue;
            }
            if let Some(go) = self.swarm.behaviour_mut().gossipsub.as_mut() {
                if let Err(e) = go.publish(TopicHash::from_raw(TOPIC_CHAT), evt.clone()) {
                    tracing::warn!("Message can not pub chat to any relay node [{c}]>>> {e:?}");
                    self.pub_pending.push_back((evt, Instant::now(),c+1));
                }
            }
        }
    }
    /// Check the next node in the DHT.
    #[tracing::instrument(skip(self))]
    async fn dht_nice_tick(&mut self) {
        let mut to_dial = None;
        if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
            for kbucket in kad.kbuckets() {
                if let Some(range) = self.kad_last_range {
                    if kbucket.range() == range {
                        continue;
                    }
                }

                // find the first disconnected node
                for entry in kbucket.iter() {
                    if entry.status == NodeStatus::Disconnected {
                        let peer_id = entry.node.key.preimage();

                        let dial_opts = DialOpts::peer_id(*peer_id)
                            .condition(PeerCondition::Disconnected)
                            .addresses(entry.node.value.clone().into_vec())
                            .extend_addresses_through_behaviour()
                            .build();
                        to_dial = Some((dial_opts, kbucket.range()));
                        break;
                    }
                }
            }
        }

        if let Some((dial_opts, range)) = to_dial {
            info!(
                "checking node {:?} in bucket range ({:?})",
                dial_opts.get_peer_id().unwrap(),
                range
            );

            if let Err(e) = self.swarm.dial(dial_opts) {
                info!("failed to dial: {:?}", e);
            }
            self.kad_last_range = Some(range);
        }
    }

    /// Subscribe to [`NetworkEvent`]s.
    #[tracing::instrument(skip(self))]
    pub fn network_events(&mut self) -> Receiver<NetworkEvent> {
        let (s, r) = channel(512);
        self.network_events.push(s);
        r
    }

    fn destroy_session(&mut self, ctx: u64, response_channel: oneshot::Sender<Result<()>>) {
        if let Some(bs) = self.swarm.behaviour().bitswap.as_ref() {
            let workers = self.bitswap_sessions.remove(&ctx);
            let client = bs.client().clone();
            
            tokio::task::spawn(async move {
                debug!("stopping session {}", ctx);
                if let Some(workers) = workers {
                    info!("stopping workers {} for session {}", workers.len(), ctx);
                    // first shutdown workers
                    for (closer, worker) in workers {
                        if closer.send(()).is_ok() {
                            worker.await.ok();
                        }
                    }
                    info!("all workers stopped for session {}", ctx);
                }
                if let Err(err) = client.stop_session(ctx).await {
                    tracing::info!("failed to stop session {}: {:?}", ctx, err);
                }
                if let Err(err) = response_channel.send(Ok(())) {
                    tracing::error!("session {} failed to send stop response: {:?}", ctx, err);
                }
                warn!("session {} stopped", ctx);
            });
        } else {
            let _ = response_channel.send(Err(anyhow!("no bitswap available")));
        }
    }

    /// Send a request for data over bitswap
    fn want_block(
        &mut self,
        ctx: u64,
        cid: Cid,
        providers: HashSet<PeerId>,
        mut chan: OneShotSender<Result<Block, String>>,
    ) -> Result<()> {
        if let Some(bs) = self.swarm.behaviour().bitswap.as_ref() {
            let client = bs.client().clone();
            let (closer_s, closer_r) = oneshot::channel();

            let entry = self.bitswap_sessions.entry(ctx).or_default();

            let providers: Vec<_> = providers.into_iter().collect();
            let worker = tokio::task::spawn(async move {
                tokio::select! {
                    _ = closer_r => {
                        // Explicit sesssion stop.
                        warn!("session {}: stopped: closed", ctx);
                    }
                    _ = chan.closed() => {
                        // RPC dropped
                        warn!("session {}: stopped: request canceled", ctx);
                    }
                    block = client.get_block_with_session_id(ctx, &cid, &providers) => match block {
                        Ok(block) => {
                            if let Err(e) = chan.send(Ok(block)) {
                                tracing::info!("failed to send block response: {:?}", e);
                            }
                        }
                        Err(err) => {
                            chan.send(Err(err.to_string())).ok();
                        }
                    },
                }
            });
            entry.push((closer_s, worker));

            Ok(())
        } else {
            bail!("no bitswap available");
        }
    }

    #[tracing::instrument(skip(self))]
    async fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<
            <NodeBehaviour as NetworkBehaviour>::OutEvent,
            <<<NodeBehaviour as NetworkBehaviour>::ConnectionHandler as IntoConnectionHandler>::Handler as ConnectionHandler>::Error>,
    ) -> Result<()> {
        // libp2p_metrics().record(&event);
        // tracing::info!("swarm>>>> {event:?}");
        match event {
            // outbound events
            SwarmEvent::Behaviour(event) => self.handle_node_event(event).await,
            SwarmEvent::ConnectionEstablished {
                peer_id,
                num_established,
                ..
            } => {
                if let Some(channels) = self.dial_queries.get_mut(&peer_id) {
                    while let Some(channel) = channels.pop() {
                        channel.send(Ok(())).ok();
                    }
                }
                
                if num_established.get() == 1 {
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let to_id = digest.sum64();
                    self.connected_peers.insert(to_id, peer_id.clone());
                    if let Some(pending) = self.pending_routing.get(&to_id) {
                        let mut pending_crc = vec![];
                        for (crc,t) in pending {
                            if t.elapsed().as_millis() > 2000 {
                                tracing::warn!("pending crc {crc} to {peer_id}");
                                pending_crc.push(*crc);
                                if pending_crc.len() >31 {
                                    break;
                                }
                            }
                        }
                        if !pending_crc.is_empty() {
                            if let Err(e) = self.routing_tx.send((peer_id.clone(),pending_crc)).await {
                                tracing::error!("{e:?}");
                            }
                        }
                    }
                    let local_id = self.local_peer_id();
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&local_id.to_bytes());
                    let my_id = digest.sum64();
                    let f = self.get_peer_index(my_id);
                    let t = self.get_peer_index(to_id);
                    // let time = Utc::now().timestamp_millis() as u64;
                    tracing::info!("local connection >> {my_id} --> {to_id}");
                    self.connections
                        .update_edge(f, t, ConnectionEdge::Local(peer_id.clone()));

                    self.emit_network_event(NetworkEvent::PeerConnected(peer_id));
                }
                debug!("ConnectionEstablished: {:}", peer_id);
                Ok(())
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                if num_established == 0 {
                    let local_id = self.local_peer_id();
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&local_id.to_bytes());
                    let my_id = digest.sum64();

                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let to_id = digest.sum64();
                    self.connected_peers.remove(&to_id);
                    let f = self.get_peer_index(my_id);
                    let t = self.get_peer_index(to_id);

                    if let Some(e) = self.connections.find_edge(f, t) {
                        self.connections.remove_edge(e);
                        tracing::info!("local disconnection >> {my_id} --> {to_id}");
                    }
                    else{
                        tracing::error!("local disconnection >> {my_id} --> {to_id}");
                    }

                    self.emit_network_event(NetworkEvent::PeerDisconnected(peer_id));
                }

                debug!("ConnectionClosed: {:}", peer_id);
                Ok(())
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                debug!("failed to dial: {:?}, {:?}", peer_id, error);

                if let Some(peer_id) = peer_id {
                    if let Some(channels) = self.dial_queries.get_mut(&peer_id) {
                        while let Some(channel) = channels.pop() {
                            channel
                                .send(Err(anyhow!("Error dialing peer {:?}: {}", peer_id, error)))
                                .ok();
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    #[tracing::instrument(skip(self))]
    fn emit_network_event(&mut self, ev: NetworkEvent) {
        debug!("emit>>>{ev:?}");
        for sender in &mut self.network_events {
            let ev = ev.clone();
            let sender = sender.clone();
            tokio::task::spawn(async move {
                if let Err(e) = sender.send(ev.clone()).await {
                    tracing::error!("failed to send network event: {:?}", e);
                }
            });
        }
    }

    #[tracing::instrument(skip(self, event))]
    async fn handle_node_event(&mut self, event: Event) -> Result<()> {
        // tracing::info!("node>>> {event:?}");
        let local_id = self.local_peer_id();
        let mut digest = crc64fast::Digest::new();
        digest.write(&local_id.to_bytes());
        let my_id = digest.sum64();
        match event {
            Event::Bitswap(e) => {
                match e {
                    BitswapEvent::Provide { key } => {
                        debug!("bitswap provide {}", key);
                        if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                            match kad.start_providing(key.hash().to_bytes().into()) {
                                Ok(_query_id) => {
                                    // TODO: track query?
                                }
                                Err(err) => {
                                    tracing::info!("failed to provide {}: {:?}", key, err);
                                }
                            }
                        }
                    }
                    BitswapEvent::FindProviders {
                        key,
                        response,
                        limit,
                    } => {
                        debug!("bitswap find providers {}", key);
                        self.handle_rpc_message(RpcMessage::ProviderRequest {
                            key: ProviderRequestKey::Dht(key.hash().to_bytes().into()),
                            response_channel: response,
                            limit,
                        })?;
                    }
                    BitswapEvent::Ping { peer, response } => {
                        match self.swarm.behaviour().peer_manager.info_for_peer(&peer) {
                            Some(info) => {
                                response.send(info.latency()).ok();
                            }
                            None => {
                                response.send(None).ok();
                            }
                        }
                    }
                }
            }
            Event::Kademlia(e) => {
                libp2p_metrics().record(&e);

                if let KademliaEvent::OutboundQueryProgressed {
                    id, result, step, ..
                } = e
                {
                    match result {
                        QueryResult::GetProviders(Ok(p)) => {
                            match p {
                                GetProvidersOk::FoundProviders { key, providers } => {
                                    let swarm = self.swarm.behaviour_mut();
                                    if let Some(kad) = swarm.kad.as_mut() {
                                        debug!(
                                            "provider results for {:?} last: {}",
                                            key, step.last
                                        );

                                        // Filter out bad providers.
                                        let providers: HashSet<_> = providers
                                            .into_iter()
                                            .filter(|provider| {
                                                let is_bad =
                                                    swarm.peer_manager.is_bad_peer(provider);
                                                if is_bad {
                                                    inc!(P2PMetrics::SkippedPeerKad);
                                                }
                                                !is_bad
                                            })
                                            .collect();

                                        self.providers.handle_get_providers_ok(
                                            id, step.last, key, providers, kad,
                                        );
                                    }
                                }
                                GetProvidersOk::FinishedWithNoAdditionalRecord { .. } => {
                                    let swarm = self.swarm.behaviour_mut();
                                    if let Some(kad) = swarm.kad.as_mut() {
                                        debug!(
                                            "FinishedWithNoAdditionalRecord for query {:#?}",
                                            id
                                        );
                                        self.providers.handle_no_additional_records(id, kad);
                                    }
                                }
                            }
                        }
                        QueryResult::GetProviders(Err(error)) => {
                            if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                                self.providers.handle_get_providers_error(id, error, kad);
                            }
                        }
                        QueryResult::Bootstrap(Ok(BootstrapOk {
                            peer,
                            num_remaining,
                        })) => {
                            debug!(
                                "kad bootstrap done {:?}, remaining: {}",
                                peer, num_remaining
                            );
                        }
                        QueryResult::Bootstrap(Err(e)) => {
                            tracing::info!("kad bootstrap error: {:?}", e);
                        }
                        QueryResult::GetClosestPeers(Ok(GetClosestPeersOk { key, peers })) => {
                            debug!("GetClosestPeers ok {:?}", key);
                            if let Some((peer_id, channels)) = self.find_on_dht_queries.remove(&key)
                            {
                                let have_peer = peers.contains(&peer_id);
                                // if this is not the last step we will have more chances to find
                                // the peer
                                if !have_peer && !step.last {
                                    return Ok(());
                                }
                                let res = move || {
                                    if have_peer {
                                        Ok(())
                                    } else {
                                        Err(anyhow!("Failed to find peer {:?} on the DHT", peer_id))
                                    }
                                };
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(res()).ok();
                                    }
                                });
                            }
                        }
                        QueryResult::GetClosestPeers(Err(GetClosestPeersError::Timeout {
                            key,
                            ..
                        })) => {
                            debug!("GetClosestPeers Timeout: {:?}", key);
                            if let Some((peer_id, channels)) = self.find_on_dht_queries.remove(&key)
                            {
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(Err(anyhow!(
                                            "Failed to find peer {:?} on the DHT: Timeout",
                                            peer_id
                                        )))
                                        .ok();
                                    }
                                });
                            }
                        }
                        QueryResult::GetRecord(Ok(ret)) => match ret {
                            GetRecordOk::FoundRecord(PeerRecord { peer, record }) => {
                                let key = record.key.clone();
                                debug!("FoundRecord: {:?} @{:?}", key, peer);
                                if let Some((_query_id, channels)) =
                                    self.record_on_dht_queries.remove(&key.to_vec())
                                {
                                    tokio::task::spawn(async move {
                                        for chan in channels.into_iter() {
                                            chan.send(Ok(Some(record.clone()))).ok();
                                        }
                                    });
                                }
                                
                            }
                            GetRecordOk::FinishedWithNoAdditionalRecord { .. } => {
                                tracing::debug!("FinishedWithNoAdditionalRecord");
                            }
                        },
                        QueryResult::GetRecord(Err(e)) => {
                            let key = e.into_key();
                            debug!("GetRecord Timeout: {:?}", key);
                            if let Some((query_id, channels)) =
                                self.record_on_dht_queries.remove(&key.to_vec())
                            {
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(Err(anyhow!(
                                            "Failed to get record {:?}[{:?}] on the DHT: Timeout",
                                            key,
                                            query_id
                                        )))
                                        .ok();
                                    }
                                });
                            }
                        }
                        QueryResult::PutRecord(Ok(ret)) => {
                            let key = ret.key;
                            debug!("PutRecord: {:?}", key);
                            if let Some((_query_id, channels)) =
                                self.record_on_dht_queries.remove(&key.to_vec())
                            {
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(Ok(None)).ok();
                                    }
                                });
                            }
                        }
                        QueryResult::PutRecord(Err(e)) => {
                            let key = e.into_key();
                            debug!("GetRecord Timeout: {:?}", key);
                            if let Some((query_id, channels)) =
                                self.record_on_dht_queries.remove(&key.to_vec())
                            {
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(Err(anyhow!(
                                            "Failed to put record {:?}[{:?}] on the DHT: Timeout",
                                            key,
                                            query_id
                                        )))
                                        .ok();
                                    }
                                });
                            }
                        }
                        QueryResult::StartProviding(Ok(providing)) => {
                            let key = providing.key;
                            debug!("StartProviding OK: {:?}", key);
                            if let Some((query_id, channels)) =
                                self.provider_on_dht_queries.remove(&key.to_vec())
                            {
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(Ok(query_id)).ok();
                                    }
                                });
                            }
                        }
                        other => {
                            debug!("Libp2p => Unhandled Kademlia query result: {:?}", other)
                        }
                    }
                }
            }
            Event::Identify(e) => {
                libp2p_metrics().record(&*e);
                // tracing::info!("tick: identify {:?}", e);
                // let local_peer_id = local_id.to_string();
                // let is_client = match self.agent.as_ref() {
                //     Some(ag)=>{
                //         !ag.contains("Relay")
                //     }
                //     None=> false
                // };

                if let IdentifyEvent::Received { peer_id, info } = *e {
                    // let obs_addr = &info.observed_addr;
                    // if obs_addr.to_string().contains("quic-v1/p2p/") /*|| add_addr.clone().to_string().contains("/tcp/")*/ {
                    //     if !is_client {
                    //     }
                    // }
                    for addr in info.listen_addrs.iter() {
                        if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                            chat.add_address(&peer_id, addr.clone());
                        }
                    }
                    if info.agent_version.contains("Relay") {
                        // TODO: only in my relay white list;
                        for protocol in &info.protocols {
                            let p = protocol.as_bytes();
                            if p == kad::protocol::DEFAULT_PROTO_NAME {
                                for addr in &info.listen_addrs {
                                    if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                                        kad.add_address(&peer_id, addr.clone());
                                    }
                                }
                            }
                        }
                          //TODO only in my contacts or my white list of relay
                        if let Some(bitswap) = self.swarm.behaviour().bitswap.as_ref() {
                            bitswap.on_identify(&peer_id, &info.protocols);
                        }
                    } else {
                        //TODO only in my contacts
                        tracing::info!("peer info: {info:?}");
                        // info.observed_addr
                        for protocol in &info.protocols {
                            let p = protocol.as_bytes();
                            if p == b"/libp2p/autonat/1.0.0" {
                                // TODO: expose protocol name on `libp2p::autonat`.
                                // TODO: should we remove them at some point?
                                for addr in &info.listen_addrs {
                                    if let Some(autonat) =
                                        self.swarm.behaviour_mut().autonat.as_mut()
                                    {
                                        autonat.add_server(peer_id, Some(addr.clone()));
                                    }
                                }
                            }
                        }
                    }

                    tracing::info!("identity>>> {peer_id} --> {info:?} ");
                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .inject_identify_info(peer_id, Some(info.clone()));


                    if let Some(channels) = self.lookup_queries.remove(&peer_id) {
                        for chan in channels {
                            chan.send(Ok(info.clone())).ok();
                        }
                    }
                } else if let IdentifyEvent::Error { peer_id, error } = *e {
                    if let Some(channels) = self.lookup_queries.remove(&peer_id) {
                        for chan in channels {
                            chan.send(Err(anyhow!(
                                "error upgrading connection to peer {:?}: {}",
                                peer_id,
                                error
                            )))
                            .ok();
                        }
                    }
                }
                
            }
            Event::Ping(e) => {
                libp2p_metrics().record(&e);
                tracing::info!("ping:{e:?}");
                if let PingResult::Ok(ping) = e.result {
                    let ttl = match &ping {
                        ping::Success::Ping {
                            rtt
                        } => Some(rtt.clone()),
                        _ => None,
                    };

                    if let Some(x) = ttl {
                        self.emit_network_event(NetworkEvent::Ping(PingInfo{
                            peer: e.peer,
                            ttl: x,
                        }))
                    }

                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .inject_ping(e.peer, ping);
                }
                else{
                    tracing::info!("ping>>>>{e:?}");
                }
            }
            Event::Relay(event) => {
                libp2p_metrics().record(&event);
                tracing::warn!("relay:{event:?}");
            }
            Event::Dcutr(e) => {
                libp2p_metrics().record(&e);
            }
            Event::Gossipsub(e) => {
                libp2p_metrics().record(&e);
                match e {
                    libp2p::gossipsub::GossipsubEvent::Message {
                        propagation_source,
                        message_id,
                        message,
                    }=>{
                        if self.agent != Some(format!("Relay")) {
                            self.emit_network_event(NetworkEvent::Gossipsub(GossipsubEvent::Message {
                                from: propagation_source,
                                id: message_id,
                                message,
                            }));
                            return Ok(());
                        }
                        match &message {
                            GossipsubMessage {
                                source,
                                data,
                                sequence_number,
                                topic,
                            } => {
                                let event = luffa_rpc_types::Event::decode(data)?;
                                let luffa_rpc_types::Event {
                                    crc,
                                    from_id,
                                    to,
                                    msg,
                                    nonce,
                                    ..
                                } = event;
                                if let Some(pending) = self.pending_routing.get(&to) {
                                    if pending.iter().find(|(x,_)| *x == crc ).is_some() {
                                        tracing::warn!("CRC is pending routing {crc}");
                                        return Ok(());
                                    }
                                }
                                if nonce.is_none() {
                                    
                                    if let Ok(msg) =
                                        Message::decrypt(bytes::Bytes::from(msg), None, nonce)
                                    {
                                        match msg {
                                            Message::ContactsSync { did, contacts } => {
                                                let f = self.get_contacts_index(did);
                                                for ctt in contacts.iter() {
                                                    let t = self.get_contacts_index(ctt.did);
                                                    let tp = ctt.r#type as u8;
                                                    self.contacts.update_edge(f, t, tp);
                                                }
                                            }
                                            Message::Feedback { crc,status,from_id,.. }=>{
                                                match status {
                                                    FeedbackStatus::Reach | FeedbackStatus::Read =>{
                                                        if let Some(from) = from_id {
                                                            let pending = self.pending_routing.entry(from).or_insert(Vec::new());
                                                            pending.retain(|(c,_t)| !crc.contains(c));
                                                        }
                                                        else{
                                                            tracing::warn!("reach or read feedback which from is None");
                                                        }
                                                    }
                                                    FeedbackStatus::Routing=>{
                                                        if let Some(from) = from_id {
                                                            let pending = self.pending_routing.entry(from).or_insert(Vec::new());
                                                            for c in crc {
                                                                if pending.iter().find(|(x,_)| *x == c ).is_none() {
                                                                    pending.push((c,std::time::Instant::now()));
                                                                }
                                                            }
                                                        }
                                                        else{
                                                            tracing::warn!("reach or read feedback which from is None");
                                                        }
                                                    }
                                                    _=>{
                                                        
                                                    }
                                                }
                                            }
                                            Message::StatusSync {
                                                to,
                                                from_id,
                                                status,
                                            } => {
                                                if to > 0 && from_id > 0 {
                                                    let f = self.get_peer_index(from_id);
                                                    let t = self.get_peer_index(to);
                                                    match status {
                                                        AppStatus::Active
                                                        | AppStatus::Connected => {
                                                            // this node is connected to the target of this message?
                                                            let mut edges = self.connections.edges(t);
                                                            match edges.find(|e| match e.weight() {
                                                                ConnectionEdge::Local(_) => true,
                                                                _ => false,
                                                            }){
                                                                None=>{
                                                                    let time = Utc::now().timestamp_millis()
                                                                        as u64;
                                                                    self.connections.update_edge(
                                                                        f,
                                                                        t,
                                                                        ConnectionEdge::Remote(time),
                                                                    );
                                                                    tracing::info!("remote connection {from_id} -> {to}");
                                                                }
                                                                _=>{

                                                                }
                                                            }
                                                        }
                                                        _ => {
                                                            let mut edges = self.connections.edges(t);
                                                            match edges.find(|e| match e.weight() {
                                                                ConnectionEdge::Local(_) => true,
                                                                _ => false,
                                                            }){
                                                                None=>{
                                                                    if let Some(i) =
                                                                    self.connections.find_edge(f, t)
                                                                    {
                                                                        self.connections.remove_edge(i);
                                                                        tracing::info!("remote disconnection {from_id} -> {to}");
                                                                    }
                                                                    else{
                                                                        tracing::error!("remote disconnection {from_id} -> {to}");
                                                                    }
                                                                }
                                                                _=>{

                                                                }
                                                            }
                                                           
                                                        }
                                                    }
                                                }
                                            }
                                            
                                            _ => {}
                                        }
                                    }
                                } else {
                                    let f = self.get_contacts_index(from_id);
                                    let t = self.get_contacts_index(to);
                                    // check that from and to was in any contacts ?
                                    if let Some(idx) = self.contacts.find_edge(f, t) {
                                        let tp = self.contacts.edge_weight(idx).unwrap();
                                        let tp = *tp;
                                        let mut rx_any = None;
                                        if tp == 0 {
                                            // contact is private
                                            // let f = self.get_peer_index(my_id);
                                            let t = self.get_peer_index(to);
                                            let pending = self.pending_routing.entry(to).or_insert(Vec::new());
                                            if pending.iter().find(|(x,_)| *x == crc ).is_none() {
                                                pending.push((crc,std::time::Instant::now()));
                                            }

                                            if let Ok(Some(rx)) =
                                                self.local_send_if_connected(t,to, data)
                                            {
                                                rx_any = Some(rx);
                                            }
                                            
                                        } else {
                                            let g_idx = self.get_contacts_index(to);
                                            let members = self.contacts.edges(g_idx);
                                            let targets = members
                                                .into_iter()
                                                .map(|m| {
                                                    if m.source() == g_idx {
                                                        m.target()
                                                    } else {
                                                        m.source()
                                                    }
                                                })
                                                .collect::<Vec<_>>();
                                            for t in targets {
                                                if let Some(to_id) = self.contacts.node_weight(t) {
                                                    if *to_id == my_id || *to_id == from_id {
                                                        continue;
                                                    }
                                                    let did = *to_id;
                                                    let pending = self.pending_routing.entry(did).or_insert(Vec::new());
                                                    if pending.iter().find(|(x,_)| *x == crc ).is_none() {
                                                        pending.push((crc,std::time::Instant::now()));
                                                        tracing::warn!("group msg [{crc}] route to {did}");
                                                    }
                                                    else{
                                                        continue;
                                                    }
                                                    if let Ok(Some(rx)) =
                                                        self.local_send_if_connected(t,did, data)
                                                    {
                                                        tokio::spawn(async move {
                                                            if let Ok(res) = rx.await {
                                                                if let Ok(Some(cr)) = res {
                                                                    if let Ok(evt) = luffa_rpc_types::Event::decode(&cr.data) {

                                                                        tracing::warn!("pub group msg {crc} push to {did} with res >>{evt:?}");
                                                                    }
                                                                    else{

                                                                        let res = crate::behaviour::chat::Response(cr.data);
                                                                        tracing::warn!("pub group msg {crc} push to {did} with res >>{res:?}");
                                                                    }
                                                                };
                                                            }
                                                        });
                                                    }
                                                    else{

                                                    }
                                                }
                                                
                                            }
                                        }

                                        if let Some(rx) = rx_any {
                                            tokio::spawn(async move {
                                                if let Ok(res) = rx.await {
                                                    if let Ok(Some(cr)) = res {
                                                        if let Ok(evt) = luffa_rpc_types::Event::decode(&cr.data) {
                                                            let luffa_rpc_types::Event {
                                                                crc,msg,nonce,..
                                                            } = evt;
                                                            if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), None, nonce)
                                                            {
                                                                tracing::info!("[{crc}] res msg>> {msg:?}");
                                                            }
                                                        }
                                                        
                                                    };
                                                }
                                            });
                                        }
                                    } else {
                                        // offer or answer
                                        let t = self.get_peer_index(to);
                                        let pending = self.pending_routing.entry(to).or_insert(Vec::new());
                                        if pending.iter().find(|(x,_)| *x == crc ).is_none() {
                                            pending.push((crc,std::time::Instant::now()));
                                        }

                                        if let Ok(Some(rx)) =
                                            self.local_send_if_connected(t,to, data)
                                        {
                                            tokio::spawn(async move {
                                                if let Ok(res) = rx.await {
                                                    if let Ok(Some(cr)) = res {
                                                        if let Ok(evt) = luffa_rpc_types::Event::decode(&cr.data) {
                                                            let luffa_rpc_types::Event {
                                                                crc,msg,nonce,..
                                                            } = evt;
                                                            if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), None, nonce)
                                                            {
                                                                tracing::info!("[{crc}] res msg>> {msg:?}");
                                                            }
                                                        }
                                                        
                                                    };
                                                }
                                            });
                                        }
                                    }
                                }
                            }
                            _ => {
                                self.emit_network_event(NetworkEvent::Gossipsub(
                                    GossipsubEvent::Message {
                                        from: propagation_source,
                                        id: message_id,
                                        message,
                                    },
                                ));
                            }
                        }
                    }
                    libp2p::gossipsub::GossipsubEvent::Subscribed { peer_id, topic } =>{
                        self.emit_network_event(NetworkEvent::Gossipsub(GossipsubEvent::Subscribed {
                            peer_id,
                            topic,
                        }));
                    }
                    libp2p::gossipsub::GossipsubEvent::Unsubscribed { peer_id, topic }=>{
                        self.emit_network_event(NetworkEvent::Gossipsub(
                            GossipsubEvent::Unsubscribed { peer_id, topic },
                        ));
                    }
                    _=>{

                    }
                }
                
            }
            Event::Mdns(e) => match e {
                mdns::Event::Discovered(peers) => {
                    for (peer_id, addr) in peers {
                        let is_connected = self.swarm.is_connected(&peer_id);
                        debug!(
                            "mdns: discovered {} at {} (connected: {:?})",
                            peer_id, addr, is_connected
                        );
                        if !is_connected {
                            let dial_opts =
                                DialOpts::peer_id(peer_id).addresses(vec![addr]).build();
                            if let Err(e) = Swarm::dial(&mut self.swarm, dial_opts) {
                                tracing::info!("invalid dial options: {:?}", e);
                            }
                        }
                    }
                }
                mdns::Event::Expired(_) => {}
            },
            Event::Bitswap(_e) => {}
            Event::Chat(chat) => {
                match chat {
                    RequestResponseEvent::Message { peer, message } => {
                        tracing::info!("Chat Req>>> {peer:?}");
                        if &peer == self.local_peer_id() {
                            eprintln!("from me");
                            return Ok(());
                        }
                        match message {
                            RequestResponseMessage::Request {
                                request_id,
                                request,
                                channel,
                            } => {
                                let event = match luffa_rpc_types::Event::decode(request.data()) {
                                    Ok(event) => event,
                                    Err(e) => {
                                        tracing::error!("{e}");
                                        panic!("{}", e);
                                        // return Ok(());
                                    }
                                };
                                let luffa_rpc_types::Event {
                                    crc,
                                    from_id,
                                    to,
                                    msg,
                                    nonce,
                                    ..
                                } = event;
                                let mut feed_crc = vec![crc];

                                tracing::info!("chat request from relay {crc}");
                                if let Some(pending) = self.pending_routing.get(&to) {
                                    if pending.iter().find(|(x,_)| *x == crc ).is_some() {
                                        tracing::warn!("CRC is pending routing {crc}");
                                        return Ok(());
                                    }
                                    if let Err(e) = self
                                    .put_to_dht(crc, request.data().to_vec())
                                    {
                                        tracing::error!("put to dht {e:?}");
                                    }
                                }
                                
                                let mut feed_status = luffa_rpc_types::FeedbackStatus::Routing;
                                if nonce.is_none() {
                                    if let Ok(msg) =
                                        Message::decrypt(bytes::Bytes::from(msg), None, nonce.clone())
                                    {
                                        let msg_t = msg.clone();
                                        match msg {
                                            Message::StatusSync { from_id, status ,..}=>{
                                                match status {
                                                    AppStatus::Active=> {
                                                        if let Some(pending) = self.pending_routing.get(&from_id) {
                                                            if let Some(peer_id) = self.connected_peers.get(&from_id) {
                                                                let mut pending_crc = vec![];
                                                                for (crc,t) in pending {
                                                                    if t.elapsed().as_millis() > 2000 {
                                                                        tracing::warn!("active pending crc {crc} to {peer_id}");
                                                                        pending_crc.push(*crc);
                                                                        if pending_crc.len() >= 32 {
                                                                            break;
                                                                        }
                                                                    }
                                                                }
                                                                if !pending_crc.is_empty() {
                                                                    feed_crc.clear();
                                                                    feed_crc.extend_from_slice(&pending_crc);
                                                                    feed_status = luffa_rpc_types::FeedbackStatus::Fetch;
                                                                    if let Err(e) = self.routing_tx.send((peer_id.clone(),pending_crc)).await {
                                                                        tracing::error!("{e:?}");
                                                                    }
                                                                }
                                                            }
                                                            
                                                        }
                                                    }
                                                    AppStatus::Deactive =>{
                                                        self.connected_peers.remove(&from_id);
                                                        let f = self.get_peer_index(my_id);
                                                        let t = self.get_peer_index(from_id);

                                                        if let Some(e) = self.connections.find_edge(f, t) {
                                                            self.connections.remove_edge(e);
                                                            tracing::warn!("local Deactive >> {my_id} --> {from_id}");
                                                        }
                                                        else{
                                                            tracing::error!("local Deactive >> {my_id} --> {from_id}");
                                                        }
                                                    }
                                                    _=>{
                                                        
                                                    }
                                                }
                                                if let Some(go) = self.swarm.behaviour_mut().gossipsub.as_mut() {
                                                    let msg_t = Message::StatusSync {to:from_id,from_id:my_id,status};
                                                    let evt = luffa_rpc_types::Event::new(0,&msg_t,None,from_id);
                                                    let evt = evt.encode()?;
                                                    if let Err(e) = go.publish(
                                                        TopicHash::from_raw(TOPIC_STATUS),
                                                        evt,
                                                    ) {
                                                        tracing::error!("[{from_id}] StatusSync pub>>>:{e:?}");
                                                    }
                                                }
                                                
                                            }
                                            Message::ContactsSync { did, contacts } => {
                                                let f = self.get_contacts_index(did);
                                                for ctt in contacts.iter() {
                                                    let t = self.get_contacts_index(ctt.did);
                                                    let tp = ctt.r#type as u8;
                                                    self.contacts.update_edge(f, t, tp);
                                                }
                                                let evt = luffa_rpc_types::Event::new(0,&msg_t,None,from_id);
                                                let evt = evt.encode()?;
                                                if let Some(go) = self.swarm.behaviour_mut().gossipsub.as_mut() {
                                                    if let Err(e) = go.publish(
                                                        TopicHash::from_raw(TOPIC_STATUS),
                                                        evt,
                                                    ) {
                                                        tracing::error!("[{did}]ContactsSync pub>>>:{e:?}");
                                                    }
                                                }
                                            }
                                            Message::Feedback { crc,status,from_id,.. }=>{
                                                match &status {
                                                    FeedbackStatus::Reach | FeedbackStatus::Read =>{
                                                        if let Some(from) = from_id {
                                                            let pending = self.pending_routing.entry(from).or_insert(Vec::new());
                                                            pending.retain(|(c,_t)| !crc.contains(c));
                                                            // if !pending.is_empty() {
                                                            // }
                                                            tracing::info!("from {from} Reach>>>> {:?}", pending);
                                                            if let Some(go) = self
                                                            .swarm
                                                            .behaviour_mut()
                                                            .gossipsub
                                                            .as_mut()
                                                            {
                                                                let msg_t = Message::Feedback {crc,from_id:Some(from),to_id:None,status};
                                                                let evt = luffa_rpc_types::Event::new(0,&msg_t,None,my_id);
                                                                let evt = evt.encode()?;
                                                                if let Err(e) = go.publish(
                                                                    TopicHash::from_raw(TOPIC_STATUS),
                                                                    evt.clone(),
                                                                ) {
                                                                    tracing::warn!("Feedback>>>{e:?}  msg: {msg_t:?}");
                                                                    self.pub_pending.push_back((evt,Instant::now(),1));
                                                                }
                                                            }
                                                        }
                                                        else{
                                                            tracing::warn!("reach or read feedback which from is None");
                                                        }
                                                    }
                                                    FeedbackStatus::Fetch=>{
                                                        self.emit_network_event(NetworkEvent::RequestResponse(
                                                            ChatEvent::Request(request.data().to_vec()),
                                                        ));
                                                    }
                                                    _=>{
                                                        tracing::warn!("feedback>> nonce msg: {msg_t:?}");

                                                    }
                                                }
                                            }
                                            _ => {
                                                tracing::warn!("nonce msg: {msg_t:?}");
                                            }
                                        }
                                    } 
                                } else if self.agent != Some(format!("Relay")) {
                                    // this node is a client and the message is send to it.
                                    tracing::warn!("private or group msg from relay [{peer:?}], crc >>> {crc}");
                                    self.emit_network_event(NetworkEvent::RequestResponse(
                                        ChatEvent::Request(request.data().to_vec()),
                                    ));
                                    
                                }
                                else {
                                    let f = self.get_contacts_index(from_id);
                                    let t = self.get_contacts_index(to);
                                    
                                    // tracing::info!("{crc}");
                                    // check that from and to was in any contacts ?

                                    if let Some(idx) = self.contacts.find_edge(f, t) {
                                        let tp = self.contacts.edge_weight(idx).unwrap();
                                        let mut rx_any = None;
                                        let tp = *tp;
                                        if tp == 0 {
                                            // contact is private
                                            let notice = Message::Feedback {crc:vec![crc],from_id:None, to_id: Some(to), status: FeedbackStatus::Notice };
                                            let evt = luffa_rpc_types::Event::new(to,&notice,None,from_id);
                                            let data = evt.encode()?;
                                            self.emit_network_event(NetworkEvent::RequestResponse(
                                                ChatEvent::Response { request_id:Some(request_id), data },
                                            ));
                                            let pending = self.pending_routing.entry(to).or_insert(Vec::new());
                                            if pending.iter().find(|(x,_)| *x == crc ).is_none() {
                                                pending.push((crc,std::time::Instant::now()));
                                            }
                                            let t = self.get_peer_index(to);

                                            if let Ok(Some(rx)) =
                                                self.local_send_if_connected(t,to, request.data())
                                            {
                                                rx_any = Some(rx);
                                            }
                                            if rx_any.is_none() {
                                                let f = self.get_peer_index(my_id);
                                                if let Some((cost, mut paths)) = astar(
                                                    &self.connections,
                                                    f,
                                                    |f| f == t,
                                                    |_e| 1,
                                                    |_| 0,
                                                ) {
                                                    paths.reverse();
                                                    tracing::warn!("[{cost}] paths>>>>>> {paths:?}");
                                                    //route this message to shortest node and then break if the node is connected.
                                                    for r in paths {
                                                        if r != f {
                                                            let r_id = self.connections[r];
                                                            match self.local_send_if_connected(
                                                                r,
                                                                r_id,
                                                                request.data(),
                                                            ) {
                                                                Ok(Some(rx)) => {
                                                                    rx_any = Some(rx);
                                                                    break;
                                                                }
                                                                _ => {}
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            match rx_any {
                                                Some(rx) => {
                                                    // let pending = self.pending_routing.entry(to).or_insert(Vec::new());
                                                    // pending.retain(|(c,_t)| crc != *c);
                                                    let sender = self.chat_sender.clone();
                                                    tokio::spawn(async move {
                                                        if let Ok(res) = rx.await {
                                                            if let Ok(Some(cr)) = res {
                                                                let res = crate::behaviour::chat::Response(cr.data);
                                                                if let Err(e) = sender.send((res,peer.clone())).await {
                                                                    tracing::error!("{e:?}");
                                                                }
                                                                
                                                            };
                                                        }
                                                    });
                                                }
                                                None => {
                                                    // do not route to any other relay node
                                                    
                                                    if let Some(go) = self
                                                        .swarm
                                                        .behaviour_mut()
                                                        .gossipsub
                                                        .as_mut()
                                                    {
                                                        if let Err(e) = go.publish(
                                                            TopicHash::from_raw(TOPIC_CHAT),
                                                            request.data().to_vec(),
                                                        ) {
                                                            tracing::error!("[{tp}]:Message can not route to any relay node>>> {e:?}");
                                                            self.pub_pending.push_back((request.data().to_vec(),Instant::now(),1));
                                                        }
                                                        let status = FeedbackStatus::Routing;
                                                        let msg_t = Message::Feedback {crc:vec![crc],from_id:Some(to),to_id:Some(to),status};
                                                        let evt = luffa_rpc_types::Event::new(0,&msg_t,None,my_id);
                                                        let evt = evt.encode().unwrap();
                                                        if let Err(e) = go.publish(
                                                            TopicHash::from_raw(TOPIC_CHAT),
                                                            evt.clone(),
                                                        ) {
                                                            tracing::error!("[{tp}] Routing can not route to any relay node>>> {e:?}");
                                                            self.pub_pending.push_back((evt,Instant::now(),1));
                                                        }

                                                        
                                                    }
                                                    tracing::info!("not connect.");
                                                    self.emit_network_event(NetworkEvent::RequestResponse(
                                                        ChatEvent::Request(request.data().to_vec()),
                                                    ));
                                                }
                                            }
                                        } else {
                                            //Group msg
                                            if let Some(go) =
                                                self.swarm.behaviour_mut().gossipsub.as_mut()
                                            {
                                                if let Err(e) = go.publish(
                                                    TopicHash::from_raw(TOPIC_CHAT),
                                                    request.data().to_vec(),
                                                ) {
                                                    tracing::error!("group msg pub>>>{e:?}");
                                                    self.pub_pending.push_back((request.data().to_vec(),Instant::now(),1));
                                                }
                                            }

                                            let g_idx = self.get_contacts_index(to);
                                            let members = self.contacts.edges(g_idx);
                                            let targets = members
                                                .into_iter()
                                                .map(|m| {
                                                    if m.source() == g_idx {
                                                        m.target()
                                                    } else {
                                                        m.source()
                                                    }
                                                    
                                                })
                                                .collect::<Vec<_>>();
                                            for t in targets {
                                                let mut did = 0;
                                                if let Some(to_id) = self.contacts.node_weight(t) {
                                                    if *to_id == my_id || *to_id == from_id{
                                                        continue;
                                                    }
                                                    did = to_id.clone();
                                                    let pending = self.pending_routing.entry(*to_id).or_insert(Vec::new());
                                                    if pending.iter().find(|(x,_)| *x == crc ).is_none() {
                                                        pending.push((crc,std::time::Instant::now()));
                                                    }
                                                    else{
                                                        continue;
                                                    }
                                                    let notice = Message::Feedback {crc:vec![],from_id:None, to_id: Some(*to_id), status: FeedbackStatus::Notice };
                                                    let evt = luffa_rpc_types::Event::new(*to_id,&notice,None,from_id);
                                                    let data = evt.encode()?;
                                                    self.emit_network_event(NetworkEvent::RequestResponse(
                                                        ChatEvent::Response { request_id:Some(request_id), data },
                                                    ));
                                                }
                                                if let Ok(Some(rx)) =
                                                    self.local_send_if_connected(t,did, request.data())
                                                {
                                                    
                                                    tokio::spawn(async move {
                                                        if let Ok(res) = rx.await {
                                                            if let Ok(Some(cr)) = res {
                                                                let res = crate::behaviour::chat::Response(cr.data);
                                                                tracing::warn!("chat group msg {crc} push to {did} with res >>{res:?}");
                                                            };
                                                        }
                                                    });
                                                }
                                                else{
                                                    tracing::warn!("chat group msg {crc} can not push to {did}");

                                                }
                                                
                                             
                                                
                                            }
                                            
                                        }
                                    } else {
                                        //contacts exchange
                                        if nonce.is_some() {

                                            tracing::warn!("contacts edge not found [{from_id} -> {to}] {crc}");
                                        }
                                        let mut rx_any = None;
                                        let t = self.get_peer_index(to);
                                        let pending = self.pending_routing.entry(to).or_insert(Vec::new());
                                        pending.push((crc,std::time::Instant::now()));
                                        if let Ok(Some(rx)) =
                                            self.local_send_if_connected(t,to, request.data())
                                        {
                                            rx_any = Some(rx);
                                        }
                                        if rx_any.is_none() {
                                            let f = self.get_peer_index(my_id);
                                            if let Some((cost, mut paths)) = astar(
                                                &self.connections,
                                                f,
                                                |f| f == t,
                                                |_e| 1,
                                                |_| 0,
                                            ) {
                                                paths.reverse();
                                                println!("[{cost}] paths 2>>>>>> {paths:?}");
                                                //route this message to shortest node and then break if the node is connected.
                                                for r in paths {
                                                    if r != f {
                                                        let r_id = self.connections[r];
                                                        match self.local_send_if_connected(
                                                            r,
                                                            r_id,
                                                            request.data(),
                                                        ) {
                                                            Ok(Some(rx)) => {
                                                                rx_any = Some(rx);
                                                                break;
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                }
                                            }
                                            else{
                                                tracing::warn!("2 not connect.");
                                                self.emit_network_event(NetworkEvent::RequestResponse(
                                                    ChatEvent::Request(request.data().to_vec()),
                                                ));
                                            }
                                        }
                                        match rx_any {
                                            Some(rx) => {
                                                // let pending = self.pending_routing.entry(to).or_insert(Vec::new());
                                                //     pending.retain(|(c,_t)| crc != *c);
                                                let sender = self.chat_sender.clone();
                                                tokio::spawn(async move {
                                                    if let Ok(res) = rx.await {
                                                        if let Ok(Some(cr)) = res {
                                                            let res = crate::behaviour::chat::Response(cr.data);
                                                            if let Err(e) = sender.send((res,peer.clone())).await {
                                                                tracing::error!("{e:?}");
                                                            }
                                                        };
                                                    }
                                                });
                                            }
                                            None => {
                                                if let Some(go) =
                                                    self.swarm.behaviour_mut().gossipsub.as_mut()
                                                {
                                                    if let Err(e) = go.publish(
                                                        TopicHash::from_raw(TOPIC_CHAT),
                                                        request.data().to_vec(),
                                                    ) {
                                                        tracing::warn!("not route,publish failed, {e:?}");
                                                        self.pub_pending.push_back((request.data().to_vec(),Instant::now(),1));
                                                    }
                                                }
                                               
                                                self.emit_network_event(
                                                    NetworkEvent::RequestResponse(
                                                        ChatEvent::Request(request.data().to_vec()),
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                };
                                let feed = Message::Feedback { crc:feed_crc,from_id:Some(from_id),to_id:Some(to),status:feed_status };
                                {
                                    let evnt = luffa_rpc_types::Event::new(0,&feed,None,my_id);
                                    let res = evnt.encode().unwrap();
                                    let res = crate::behaviour::chat::Response(res);
                                    let chat = self.swarm.behaviour_mut().chat.as_mut().unwrap();

                                    if let Err(e) = chat.send_response(channel, res) {
                                        tracing::error!("chat response failed: >>{e:?}");
                                    }
                                    else if nonce.is_some() {
                                        tracing::warn!("some nonce chat response ok: >>{crc}");
                                    }

                                    tracing::info!("-----{request_id:?}------");
                                }
                            }
                            RequestResponseMessage::Response {
                                request_id,
                                response,
                            } => {
                                let data = response.0;
                                if let Some((crc,to,channel)) = self.pending_request.remove(&request_id) {
                                    if let Err(_e) =
                                        channel.send(Ok(Some(ChatResponse { data: data.clone() })))
                                    {
                                        tracing::error!("channel response failed");
                                    }
                                    let msg = Message::Feedback { crc:vec![crc], from_id:None, to_id: Some(to), status: FeedbackStatus::Send };
                                    self.local_feedback(Some(request_id), msg );
                                    tracing::info!("channel response ok: [{crc}]");
                                }
                                self.emit_network_event(NetworkEvent::RequestResponse(
                                    ChatEvent::Response { request_id:Some(request_id), data },
                                ));
                            }
                        }
                    }
                    RequestResponseEvent::ResponseSent { peer, request_id } => {
                        tracing::info!("Chat Response Sent>>{peer:?}  {:?} ", request_id);
                        self.emit_network_event(NetworkEvent::RequestResponse(
                            ChatEvent::ResponseSent { peer, request_id },
                        ));
                    }
                    RequestResponseEvent::InboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.emit_network_event(NetworkEvent::RequestResponse(
                            ChatEvent::InboundFailure {
                                peer,
                                request_id,
                                error,
                            },
                        ));
                    }
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.emit_network_event(NetworkEvent::RequestResponse(
                            ChatEvent::OutboundFailure {
                                peer,
                                request_id,
                                error,
                            },
                        ));
                    }
                }
            }
            _ => {
                // TODO: check all important events are handled
            }
        }

        Ok(())
    }

    async fn handle_chat_response(
        &mut self,
        res: crate::behaviour::chat::Response,
        peer: PeerId,
    ) -> Result<()> {
        if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
            let req = crate::behaviour::chat::Request(res.0);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let req_id = chat.send_request(&peer, req);
            self.pending_request.insert(req_id, (0,0,tx));
            tokio::spawn(async move {
                match rx.await {
                    Ok(Ok(Some(res))) =>{
                        tracing::info!("request successful:{:?}", res);
                    }
                    _=>{
                        tracing::error!("Chat request failed");
                    }
                }
            });
            Ok(())
        }
        else{
            Err(anyhow!("handle_chat_response> Chat request failed"))
        }
    }
    async fn handle_chat_routing(&mut self, peer_id: PeerId, crc: Vec<u64>) -> Result<()> {
        let local_id = self.local_peer_id();
        let mut digest = crc64fast::Digest::new();
        digest.write(&local_id.to_bytes());
        let my_id = digest.sum64();

        let mut digest = crc64fast::Digest::new();
        digest.write(&peer_id.to_bytes());
        let to_id = digest.sum64();
        if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
            
            tracing::warn!("routing to> {to_id} crc {crc:?}");
            let msg = Message::Feedback { crc, from_id: None, to_id: Some(to_id), status: luffa_rpc_types::FeedbackStatus::Fetch };
            let data = luffa_rpc_types::Event::new(to_id, &msg, None, my_id);
            let data = data.encode().unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let req_id = chat.send_request(&peer_id, crate::behaviour::chat::Request(data));
            self.pending_request.insert(req_id, (0,to_id,tx));
            tokio::spawn(async move {
                match rx.await {
                    Ok(Ok(Some(res))) =>{
                        tracing::info!("request successful:{:?}", res);
                    }
                    _=>{
                        tracing::error!("request successful");
                    }
                }
            });
            Ok(())
        }
        else{
            Err(anyhow!("Chat request failed"))
        }
    }

    fn get_node_index(&mut self, did: u64) -> NodeIndex {
        match self.cache.node_indices().find(|n| self.cache[*n] == did) {
            Some(v) => v,
            None => self.cache.add_node(did),
        }
    }
    fn get_peer_index(&mut self, did: u64) -> NodeIndex {
        match self
            .connections
            .node_indices()
            .find(|n| self.connections[*n] == did)
        {
            Some(v) => v,
            None => self.connections.add_node(did),
        }
    }
    fn get_contacts_index(&mut self, did: u64) -> NodeIndex {
        match self
            .contacts
            .node_indices()
            .find(|n| self.contacts[*n] == did)
        {
            Some(v) => v,
            None => self.contacts.add_node(did),
        }
    }
    fn save_cache_crc(&mut self, crc: u64, from_id: u64, to: u64) -> EdgeIndex {
        tracing::info!("save cache crc:{crc}  {from_id}-> {to}");
        let from = self.get_node_index(from_id);
        let to = self.get_node_index(to);
        let now = Utc::now().timestamp_millis() as u64;
        self.cache.add_edge(from, to, (crc, now))
    }
    fn remove_cache_crc(&mut self, crc: u64, from_id: u64, to: u64) {
        tracing::info!("save cache crc:{crc}  {from_id}-> {to}");
        let from = self.get_node_index(from_id);
        let to = self.get_node_index(to);
        if let Some(edge) = self.cache.edges_connecting(from, to).filter(|c| c.weight().0 == crc).next() {
            self.cache.remove_edge(edge.id());
        }
        
    }
    fn put_to_dht(&mut self, crc: u64, data: Vec<u8>) -> anyhow::Result<QueryId> {
        if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
            let record = Record::new(crc.to_be_bytes().to_vec(), data);
            kad.put_record(record, Quorum::One)
                .map_err(|e| anyhow!("{e:?}"))
        } else {
            Err(anyhow!("put to dht failed"))
        }
    }
    fn load_cache_crc(&mut self, did: u64, have_time: Option<u64>) -> Vec<(u64, u64)> {
        let to = self.get_node_index(did);
        let mut cached_crc = vec![];
        let now = Utc::now().timestamp_millis() as u64;
        let have_time = have_time.unwrap_or(0);
        let mut remove_edges = vec![];
        if let Some(mut e) = self.cache.first_edge(to, Direction::Incoming) {
            let (crc, time) = self.cache.edge_weight(e).unwrap();
            if *time > have_time {
                if let Some((a, _b)) = self.cache.edge_endpoints(e) {
                    let from_id = self.cache.node_weight(a).unwrap();
                    cached_crc.push((*crc, *from_id));

                    while let Some(n) = self.cache.next_edge(e, Direction::Incoming) {
                        let (crc, time) = self.cache.edge_weight(n).unwrap();
                        if now - *time > 24 * 60 * 60 * 1000 {
                            remove_edges.push(n);
                            tracing::info!("remove edge {time}");
                            continue;
                        }
                        if *time < have_time {
                            tracing::info!("skip edge:{time} < {have_time}");
                            if now - *time > 5 * 60 * 1000 {
                                remove_edges.push(n);
                            }
                            e = n;
                            continue;
                        }
                        let (a, _b) = self.cache.edge_endpoints(n).unwrap();
                        let from_id = self.cache.node_weight(a).unwrap();
                        cached_crc.push((*crc, *from_id));
                        e = n;
                    }
                }
            }
        }

        for rm in remove_edges {
            self.cache.remove_edge(rm);
        }

        cached_crc
    }

    fn local_send_if_connected(
        &mut self,
        target: NodeIndex,
        did: u64,
        data: &Vec<u8>,
    ) -> Result<Option<tokio::sync::oneshot::Receiver<Result<Option<ChatResponse>, anyhow::Error>>>>
    {
        // this node is connected to the target of this message?
        let data = data.to_vec();
        let luffa_rpc_types::Event { crc, .. } = luffa_rpc_types::Event::decode_uncheck(&data)?;
        let local_id = self.local_peer_id();
        let mut digest = crc64fast::Digest::new();
        digest.write(&local_id.to_bytes());
        let my_id = digest.sum64();
        let my_idx = self.get_peer_index(my_id);
        if let Some(p) = self.connected_peers.get(&did) {
            if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                let (tx, rx) = tokio::sync::oneshot::channel();

                let req_id = chat.send_request(p, crate::behaviour::chat::Request(data));
                self.pending_request.insert(req_id, (crc, did, tx));
                tracing::warn!(
                    "local chat connected to [{p:?}] send. crc >> {} to {did}",
                    crc,
                );

                return Ok(Some(rx));
            }
        }
        if let Some(e_idx) = self.connections.find_edge(my_idx, target) {
            if let Some(w) = self.connections.edge_weight(e_idx) {
                match w {
                    ConnectionEdge::Local(p) => {
                        let mut digest = crc64fast::Digest::new();
                        digest.write(&p.clone().to_bytes());
                        let p_id = digest.sum64();
                        if p_id != did {
                            tracing::error!("[{crc}] to failed {p:?} [{p_id} != {did}]");
                        }
                        if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                            let (tx, rx) = tokio::sync::oneshot::channel();
                            let mut digest = crc64fast::Digest::new();
                            digest.write(&p.clone().to_bytes());
                            let to_id = digest.sum64();
                            let req_id = chat.send_request(p, crate::behaviour::chat::Request(data));
                            self.pending_request.insert(req_id, (crc,to_id,tx));
                            tracing::warn!("local chat to [{p:?}] send. crc >> {} match [{} {p_id} === {did}]", crc,p_id == did);
                            
                            return Ok(Some(rx));
                        }
                    },
                    _ => {
                        // NA
                    },
                }
            }
            let mut idx = e_idx;

            while let Some(e) = self.connections.next_edge(idx, Direction::Outgoing) {
                let w = self.connections.edge_weight(e).unwrap();
                tracing::warn!("local chat next edge :{w:?}");
                idx = e;
            }
            while let Some(e) = self.connections.next_edge(idx, Direction::Incoming) {
                let w = self.connections.edge_weight(e).unwrap();
                tracing::warn!("local chat next edge :{w:?}");
                idx = e;
            }
        }
        tracing::warn!("local chat msg {crc} can not push to {did}");
        Ok(None)
    }

    #[tracing::instrument(skip(self))]
    fn handle_rpc_message(&mut self, message: RpcMessage) -> Result<bool> {
        // Inbound messages
        match message {
            RpcMessage::BackGossipsub(msg) => match msg {
                crate::rpc::GossipsubMessage::Publish(s, topic, data) => {
                    let from = self.local_peer_id().clone();
                    let id = MessageId::new(&[0]);
                    let data = data.to_vec();
                    let message = libp2p::gossipsub::GossipsubMessage {
                        source: None,
                        sequence_number: None,
                        topic,
                        data,
                    };
                    self.emit_network_event(NetworkEvent::Gossipsub(GossipsubEvent::Message {
                        from,
                        id,
                        message,
                    }))
                }
                _ => {}
            },
            RpcMessage::Chat(response_channel, data) => {
                let peer_manager = &self.swarm.behaviour().peer_manager;
                let manager = peer_manager.all_peers().into_iter().map(|p| p.clone()).collect::<Vec<_>>();

                let mut peers = {
                    manager.iter()
                        .map(
                            |p| match peer_manager.info_for_peer(p) {
                                Some(pp) => match &pp.last_info {
                                    Some(info) => {
                                        Some((p.clone(), Some(info.clone()), pp.last_rtt.clone()))
                                    }
                                    None => Some((p.clone(), None, pp.last_rtt.clone())),
                                },
                                None => Some((p.clone(),None,None)),
                            },
                        )
                        .filter(|item| item.is_some())
                        .map(|item| item.unwrap())
                        // .filter(|(_p, info, _)| info.agent_version.contains("Relay"))
                        .collect::<Vec<_>>()
                };
                
                peers.sort_by(|(_, _e, a), (_, _f, b)| {
                    let a = a.unwrap_or(Duration::from_secs(5)).as_millis();
                    let b = b.unwrap_or(Duration::from_secs(5)).as_millis();
                    a.partial_cmp(&b).unwrap()
                });
                
                
                if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                    let luffa_rpc_types::Event {
                        crc,
                        to,
                        from_id,
                        nonce,
                        ..
                    } = luffa_rpc_types::Event::decode_uncheck(&data.msg).unwrap();
                    if let Some((peer,_,_)) = peers.iter().find(|(p,_,_)| {
                        let mut digest = crc64fast::Digest::new();
                        digest.write(&p.to_bytes());
                        to == digest.sum64()
                    })
                    {
                        let req_id =
                        chat.send_request(peer, crate::behaviour::chat::Request(data.msg));
                        self.pending_request.insert(req_id, (crc,to,response_channel));
                        self.local_feedback(Some(req_id.clone()), Message::Feedback { crc:vec![crc], from_id:Some(from_id), to_id: Some(to), status: FeedbackStatus::Sending });
                        tracing::warn!("found peer for event to >> chat send. {:?}", peer);
                        return Ok(false);
                    }
                    if let Some((peer, _, ttl)) = peers.first() {
                        let req_id =
                        chat.send_request(peer, crate::behaviour::chat::Request(data.msg));
                        self.pending_request.insert(req_id, (crc,to,response_channel));
                        
                        if nonce.is_some() {
                            self.local_feedback(Some(req_id.clone()), Message::Feedback { crc:vec![crc], from_id:Some(from_id), to_id: Some(to), status: FeedbackStatus::Sending });
                            
                            if let Some((_,_,ttl2)) = peers.last() {
                                tracing::info!("chat send fast relay ttl({ttl:?}) < ttl({ttl2:?}). use {peer:?} crc: {} ", crc);
                            }
                            else{
                                tracing::warn!("chat send fast relay ttl({ttl:?}). {:?}", req_id);
                            }
                        }
                        return Ok(false);
                    }

                    if let Some(dft_peer) = manager.first() {
                        let luffa_rpc_types::Event {
                            crc,
                            to,
                            from_id,
                            ..
                        } = luffa_rpc_types::Event::decode_uncheck(&data.msg).unwrap();
                        
                        let req_id =
                        chat.send_request(&dft_peer, crate::behaviour::chat::Request(data.msg));
                        self.pending_request.insert(req_id, (crc,to,response_channel));
                        self.local_feedback(Some(req_id.clone()), Message::Feedback { crc:vec![crc], from_id:Some(from_id), to_id: Some(to), status: FeedbackStatus::Sending });
                        tracing::warn!("2 chat send to default ({dft_peer:?}). {:?}", req_id);
                        return Ok(false);
                    }
                }
                let luffa_rpc_types::Event {
                    crc,
                    to,
                    from_id,
                    ..
                } = luffa_rpc_types::Event::decode_uncheck(&data.msg).unwrap();
                tracing::error!("rpc chat send failed,not peers");
                self.local_feedback(None, Message::Feedback { crc:vec![crc], from_id:Some(from_id), to_id: Some(to), status: FeedbackStatus::Failed });
                if let Err(_e) = response_channel.send(Err(anyhow!("not peers"))) {
                    tracing::error!("channel response failed");
                }
            }
            RpcMessage::ExternalAddrs(response_channel) => {
                response_channel
                    .send(
                        self.swarm
                            .external_addresses()
                            .map(|r| r.addr.clone())
                            .collect(),
                    )
                    .ok();
            }
            RpcMessage::Listeners(response_channel) => {
                response_channel
                    .send(self.swarm.listeners().cloned().collect())
                    .ok();
            }
            RpcMessage::LocalPeerId(response_channel) => {
                response_channel.send(*self.swarm.local_peer_id()).ok();
            }
            RpcMessage::BitswapRequest {
                ctx,
                cids,
                response_channels,
                providers,
            } => {
                tracing::debug!("context:{} bitswap_request", ctx);
                let store = self.store.clone();
                let (tx, rx) = std::sync::mpsc::channel();
                tokio::spawn(async move {
                    for (cid, response_channel) in
                        cids.into_iter().zip(response_channels.into_iter())
                    {
                        match store.get(&cid) {
                            Ok(Some(blob)) => {
                                let blk = Block::new(blob, cid);
                                if let Err(e) = response_channel.send(Ok(blk)) {
                                    tracing::error!("{e:?}");
                                }
                            }
                            _ => {
                                tx.send((cid, response_channel)).unwrap();
                            }
                        }
                    }
                });
                while let Ok((cid, response_channel)) = rx.recv() {
                    if let Err(e) = self
                        .want_block(ctx, cid, providers.clone(), response_channel)
                        .map_err(|err| anyhow!("Failed to send a bitswap want_block: {:?}", err))
                    {
                        tracing::error!("{e:?}");
                    }
                }
            }
            RpcMessage::PushBitswapRequest {
                data,
                response_channels,
            } => {
                tracing::debug!("PushBitswapRequest:--{}---", data.len());
                let cid = Cid::new_v1(
                    DagCborCodec.into(),
                    multihash::Code::Sha2_256.digest(&data[..]),
                );

                let store = self.store.clone();
                let blob = data.clone();
                tokio::spawn(async move {
                    if let Err(e) = store.put(cid, blob, vec![]) {
                        tracing::info!("store put> {:?}", e);
                    }
                });
                let ret = cid.clone();
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    let key = libp2p::kad::record::Key::new(&ret.hash().to_bytes());
                    kad.start_providing(key)
                        .map_err(|_| anyhow!("Failed to push bitswap"))?;
                }
                self.swarm
                    .behaviour()
                    .notify_new_blocks(vec![Block { cid, data }]);
                for channel in response_channels {
                    channel
                        .send(Ok(ret.clone()))
                        .map_err(|_| anyhow!("Failed to push bitswap"))?;
                }
            }
            RpcMessage::BitswapNotifyNewBlocks {
                blocks,
                response_channel,
            } => {
                self.swarm.behaviour().notify_new_blocks(blocks);
                response_channel.send(Ok(())).ok();
            }
            RpcMessage::BitswapStopSession {
                ctx,
                response_channel,
            } => {
                self.destroy_session(ctx, response_channel);
            }
            RpcMessage::ProviderRequest {
                key,
                limit,
                response_channel,
            } => match key {
                ProviderRequestKey::Dht(key) => {
                    debug!("fetching providers for: {:?}", key);
                    if self.swarm.behaviour().kad.is_enabled() {
                        self.providers.push(key, limit, response_channel);
                    } else {
                        tokio::task::spawn(async move {
                            response_channel
                                .send(Err("kademlia is not available".into()))
                                .await
                                .ok();
                        });
                    }
                }
                ProviderRequestKey::Bitswap(_, _) => {
                    debug!(
                        "RpcMessage::ProviderRequest: getting providers for {:?}",
                        key
                    );
                    // TODO
                }
            },
            RpcMessage::StartProviding(response_channel, key) => {
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    // wait for kad to process the query request before returning
                    match kad.start_providing(key.clone()).map_err(|e| e.into()) {
                        Ok(query_id) => match self.provider_on_dht_queries.entry(key.to_vec()) {
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                let (_, channels) = entry.get_mut();
                                channels.push(response_channel);
                            }
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert((query_id, vec![response_channel]));
                            }
                        },
                        Err(e) => {
                            response_channel.send(Err(e)).ok();
                        }
                    }
                } else {
                    response_channel
                        .send(Err(anyhow!("kademlia is not available")))
                        .ok();
                }
            }
            RpcMessage::StopProviding(response_channel, key) => {
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    kad.stop_providing(&key);
                    response_channel.send(Ok(())).ok();
                } else {
                    response_channel
                        .send(Err(anyhow!("kademlia is not availalbe")))
                        .ok();
                }
            }
            RpcMessage::PutRecord(response_channel, record) => {
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    let key = record.key.clone();
                    match kad
                        .put_record(record, Quorum::N(NonZeroUsize::new(1).expect("3 != 1")))
                        .map_err(|e| e.into())
                    {
                        Ok(q) => match self.record_on_dht_queries.entry(key.to_vec()) {
                            std::collections::hash_map::Entry::Occupied(mut entry) => {
                                let (_, channels) = entry.get_mut();
                                channels.push(response_channel);
                            }
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert((q, vec![response_channel]));
                            }
                        },
                        Err(e) => {
                            response_channel.send(Err(e)).ok();
                        }
                    }
                } else {
                    response_channel
                        .send(Err(anyhow!("kademlia is not availalbe")))
                        .ok();
                }
            }
            RpcMessage::PutRecordTo(response_channel, peers, record) => {
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    let key = record.key.clone();
                    let peers = peers.chunks_exact(1);
                    // let peers = peers.next().unwrap();
                    // let q = kad
                    //     .put_record_to(record,peers, Quorum::One);
                    // match self.record_on_dht_queries.entry(key.to_vec()) {
                    //     std::collections::hash_map::Entry::Occupied(mut entry) => {
                    //         let (_, channels) = entry.get_mut();
                    //         channels.push(response_channel);
                    //     }
                    //     std::collections::hash_map::Entry::Vacant(entry) => {
                    //         entry.insert((q, vec![response_channel]));
                    //     }
                    // }
                } else {
                    response_channel
                        .send(Err(anyhow!("kademlia is not availalbe")))
                        .ok();
                }
            }
            RpcMessage::GetRecord(response_channel, key) => {
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    match self.record_on_dht_queries.entry(key.to_vec()) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            let (_, channels) = entry.get_mut();
                            channels.push(response_channel);
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let id = kad.get_record(key);
                            entry.insert((id, vec![response_channel]));
                        }
                    }
                } else {
                    response_channel
                        .send(Err(anyhow!("kademlia is not availalbe")))
                        .ok();
                }
            }
            RpcMessage::NetListeningAddrs(response_channel) => {
                let mut listeners: Vec<_> = Swarm::listeners(&self.swarm).cloned().collect();
                let peer_id = *Swarm::local_peer_id(&self.swarm);
                listeners.extend(Swarm::external_addresses(&self.swarm).map(|r| r.addr.clone()));

                response_channel
                    .send((peer_id, listeners))
                    .map_err(|_| anyhow!("Failed to get Libp2p listeners"))?;
            }
            RpcMessage::NetPeers(response_channel) => {
                #[allow(clippy::needless_collect)]
                let peers = self.swarm.connected_peers().copied().collect::<Vec<_>>();
                let peer_addresses: HashMap<PeerId, Vec<Multiaddr>> = peers
                    .into_iter()
                    .map(|pid| (pid, self.swarm.behaviour_mut().addresses_of_peer(&pid)))
                    .collect();

                response_channel
                    .send(peer_addresses)
                    .map_err(|_| anyhow!("Failed to get Libp2p peers"))?;
            }
            RpcMessage::NetConnect(response_channel, peer_id, addrs) => {
                if self.swarm.is_connected(&peer_id) {
                    response_channel.send(Ok(())).ok();
                } else {
                    let channels = self.dial_queries.entry(peer_id).or_default();
                    channels.push(response_channel);

                    // when using DialOpts::peer_id, having the `P2p` protocol as part of the
                    // added addresses throws an error
                    // we can filter out that protocol before adding the addresses to the dial opts
                    let addrs = addrs
                        .iter()
                        .map(|a| {
                            a.iter()
                                .filter(|p| !matches!(*p, Protocol::P2p(_)))
                                .collect()
                        })
                        .collect();
                    let dial_opts = DialOpts::peer_id(peer_id)
                        .addresses(addrs)
                        .condition(libp2p::swarm::dial_opts::PeerCondition::Always)
                        .build();
                    if let Err(e) = Swarm::dial(&mut self.swarm, dial_opts) {
                        tracing::info!("invalid dial options: {:?}", e);
                        while let Some(channel) = channels.pop() {
                            channel
                                .send(Err(anyhow!("error dialing peer {:?}: {}", peer_id, e)))
                                .ok();
                        }
                    }
                }
            }
            RpcMessage::NetConnectByPeerId(response_channel, peer_id) => {
                if self.swarm.is_connected(&peer_id) {
                    response_channel.send(Ok(())).ok();
                } else {
                    let channels = self.dial_queries.entry(peer_id).or_default();
                    channels.push(response_channel);

                    let dial_opts = DialOpts::peer_id(peer_id)
                        .condition(libp2p::swarm::dial_opts::PeerCondition::Always)
                        .build();
                    if let Err(e) = Swarm::dial(&mut self.swarm, dial_opts) {
                        while let Some(channel) = channels.pop() {
                            channel
                                .send(Err(anyhow!("error dialing peer {:?}: {}", peer_id, e)))
                                .ok();
                        }
                    }
                }
            }
            RpcMessage::AddressesOfPeer(response_channel, peer_id) => {
                let addrs = self.swarm.behaviour_mut().addresses_of_peer(&peer_id);
                response_channel.send(addrs).ok();
            }
            RpcMessage::NetDisconnect(response_channel, _peer_id) => {
                tracing::info!("NetDisconnect API not yet implemented"); // TODO: implement NetDisconnect

                response_channel
                    .send(())
                    .map_err(|_| anyhow!("sender dropped"))?;
            }
            RpcMessage::Gossipsub(g) => {
                let gossipsub = match self.swarm.behaviour_mut().gossipsub.as_mut() {
                    Some(gossipsub) => gossipsub,
                    None => {
                        tracing::info!("Unexpected gossipsub message");
                        return Ok(false);
                    }
                };
                match g {
                    rpc::GossipsubMessage::AddExplicitPeer(response_channel, peer_id) => {
                        gossipsub.add_explicit_peer(&peer_id);
                        response_channel
                            .send(())
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::AllMeshPeers(response_channel) => {
                        let peers = gossipsub.all_mesh_peers().copied().collect();
                        response_channel
                            .send(peers)
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::AllPeers(response_channel) => {
                        let all_peers = gossipsub
                            .all_peers()
                            .map(|(p, t)| (*p, t.into_iter().cloned().collect()))
                            .collect();
                        response_channel
                            .send(all_peers)
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::MeshPeers(response_channel, topic_hash) => {
                        let peers = gossipsub.mesh_peers(&topic_hash).copied().collect();
                        response_channel
                            .send(peers)
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::Publish(response_channel, topic_hash, bytes) => {
                        let res = gossipsub.publish(topic_hash, bytes.to_vec());
                        // tracing::info!("rpc >> publish. {}",res.is_ok());
                        response_channel
                            .send(res)
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::RemoveExplicitPeer(response_channel, peer_id) => {
                        gossipsub.remove_explicit_peer(&peer_id);
                        response_channel
                            .send(())
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::Subscribe(response_channel, topic_hash) => {
                        let res = gossipsub.subscribe(&IdentTopic::new(topic_hash.into_string()));
                        response_channel
                            .send(res)
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::Topics(response_channel) => {
                        let topics = gossipsub.topics().cloned().collect();
                        response_channel
                            .send(topics)
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                    rpc::GossipsubMessage::Unsubscribe(response_channel, topic_hash) => {
                        let res = gossipsub.unsubscribe(&IdentTopic::new(topic_hash.into_string()));
                        response_channel
                            .send(res)
                            .map_err(|_| anyhow!("sender dropped"))?;
                    }
                }
            }
            RpcMessage::ListenForIdentify(response_channel, peer_id) => {
                let channels = self.lookup_queries.entry(peer_id).or_default();
                channels.push(response_channel);
            }
            RpcMessage::LookupPeerInfo(response_channel, peer_id) => {
                if let Some(info) = self.swarm.behaviour().peer_manager.info_for_peer(&peer_id) {
                    let info = info.last_info.clone();
                    response_channel.send(info).ok();
                } else {
                    response_channel.send(None).ok();
                }
            }
            // RpcMessage::LookupLocalPeerInfo(response_channel) => {
            //     let peer_id = self.swarm.local_peer_id();
            //     let listen_addrs = self.swarm.listeners().cloned().collect();
            //     let observed_addrs = self
            //         .swarm
            //         .external_addresses()
            //         .map(|a| a.addr.clone())
            //         .collect();
            //     let protocol_version = String::from(crate::behaviour::PROTOCOL_VERSION);
            //     let agent_version = String::from(crate::behaviour::AGENT_VERSION);
            //     let protocols = self.swarm.behaviour().peer_manager.supported_protocols();

            //     response_channel
            //         .send(Lookup {
            //             peer_id: *peer_id,
            //             listen_addrs,
            //             observed_addrs,
            //             agent_version,
            //             protocol_version,
            //             protocols,
            //         })
            //         .ok();
            // }
            RpcMessage::CancelListenForIdentify(response_channel, peer_id) => {
                self.lookup_queries.remove(&peer_id);
                self.emit_network_event(NetworkEvent::CancelLookupQuery(peer_id));
                response_channel.send(()).ok();
            }
            RpcMessage::FindPeerOnDHT(response_channel, peer_id) => {
                debug!("find closest peers for: {:?}", peer_id);
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    match self.find_on_dht_queries.entry(peer_id.to_bytes()) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            let (_, channels) = entry.get_mut();
                            channels.push(response_channel);
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            kad.get_closest_peers(peer_id);
                            entry.insert((peer_id, vec![response_channel]));
                        }
                    }
                } else {
                    tokio::task::spawn(async move {
                        response_channel
                            .send(Err(anyhow!("kademlia is not available")))
                            .ok();
                    });
                }
            }
            RpcMessage::Shutdown => {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn local_feedback(&mut self, request_id: Option<RequestId>, msg: Message) {
        match &msg {
            Message::Feedback { from_id, to_id, .. }    =>{
                let e = luffa_rpc_types::Event::new(to_id.unwrap_or_default(), &msg, None, from_id.unwrap_or_default());
                let data = e.encode().unwrap();
                self.emit_network_event(NetworkEvent::RequestResponse(ChatEvent::Response { request_id, data }));
            }
            _=>{
                tracing::error!("not is a local feedback {msg:?}");
            }
        }
    }
}

pub async fn load_identity<S: Storage>(
    kc: &mut Keychain<S>,
    filter: Option<KeyFilter>,
) -> Result<Keypair> {
    if kc.is_empty().await? {
        tracing::info!("no identity found, creating",);
        match filter.as_ref() {
            Some(KeyFilter::Phrase(phrase, pwd)) => {
                let k = kc.create_ed25519_key_from_seed(phrase, pwd).await?;
                let keypair: Keypair = k.into();
                return Ok(keypair);
            }
            _ => {
                let (p, k) = kc.create_ed25519_key_bip39("", true).await?;
                let keypair: Keypair = k.into();
                return Ok(keypair);
            }
        }
    }

    match filter.as_ref() {
        Some(KeyFilter::Phrase(phrase, pwd)) => {
            let k = kc.create_ed25519_key_from_seed(phrase, pwd).await?;
            let keypair: Keypair = k.into();
            return Ok(keypair);
        }
        Some(KeyFilter::Name(name)) => {
            let mut keys = kc.keys();
            while let Some(first_key) = keys.next().await {
                let k = first_key.unwrap();
                if &k.name() == name {
                    let keypair: Keypair = k.into();

                    return Ok(keypair);
                }
            }
        }
        _ => {
            // let (p,k) = kc.create_ed25519_key_bip39("",true).await?;
            // let keypair: Keypair = k.into();
            // return Ok(keypair)
        }
    }

    // for now we just use the first key
    let first_key = kc.keys().next().await;
    if let Some(keypair) = first_key {
        let keypair: Keypair = keypair?.into();
        tracing::info!("identity loaded: {}", PeerId::from(keypair.public()));
        return Ok(keypair);
    }

    Err(anyhow!("inconsistent keystate"))
}
