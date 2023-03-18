use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use anyhow::{anyhow, bail, Context, Result};
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
use libp2p::request_response::{RequestResponseEvent, RequestResponseMessage, RequestId, InboundFailure, OutboundFailure};
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{ConnectionHandler, IntoConnectionHandler, NetworkBehaviour, SwarmEvent};
use libp2p::{PeerId, Swarm};
use luffa_bitswap::{BitswapEvent, Block};
use luffa_metrics::{core::MRecorder, inc, libp2p_metrics, p2p::P2PMetrics};
use luffa_rpc_types::p2p::{ChatResponse, ChatRequest};
use luffa_rpc_types::{Message, ContactsTypes, AppStatus, ChatContent};
use multihash::MultihashDigest;
use petgraph::prelude::*;
use petgraph::algo::k_shortest_path;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::{self, Sender as OneShotSender};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use crate::behaviour::{Event, NodeBehaviour};
use crate::config::Config;
use crate::keys::{Keychain, Storage};
use crate::providers::Providers;
use crate::rpc::{self, ProviderRequestKey,RpcMessage};
use crate::swarm::build_swarm;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    Gossipsub(GossipsubEvent),
    RequestResponse(ChatEvent),
    CancelLookupQuery(PeerId),
}

#[derive(Debug,Clone)]
enum ConnectionEdge {
    Local(PeerId),
    Remote(u64),
}

/// The events emitted by a [`RequestResponse`] protocol.
#[derive(Debug,Clone)]
pub enum ChatEvent {
 
    Response {
        request_id: RequestId,
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
    net_receiver_in:Receiver<rpc::RpcMessage>,
    chat_receiver:Receiver<(crate::behaviour::chat::Response,libp2p::request_response::ResponseChannel<crate::behaviour::chat::Response>)>,
    chat_sender:Arc<Sender<(crate::behaviour::chat::Response,libp2p::request_response::ResponseChannel<crate::behaviour::chat::Response>)>>,
    dial_queries: AHashMap<PeerId, Vec<OneShotSender<Result<()>>>>,
    lookup_queries: AHashMap<PeerId, Vec<oneshot::Sender<Result<IdentifyInfo>>>>,
    // TODO(ramfox): use new providers queue instead
    find_on_dht_queries: AHashMap<Vec<u8>, DHTQuery>,
    record_on_dht_queries: AHashMap<Vec<u8>, (QueryId, Vec<OneShotSender<Result<Option<Record>>>>)>,
    provider_on_dht_queries: AHashMap<Vec<u8>, (QueryId, Vec<OneShotSender<Result<QueryId>>>)>,
    pending_request: AHashMap<RequestId,OneShotSender<Result<Option<ChatResponse>>>>,
    network_events: Vec<Sender<NetworkEvent>>,
    _keychain: Keychain<KeyStorage>,
    #[allow(dead_code)]
    kad_last_range: Option<(Distance, Distance)>,
    use_dht: bool,
    bitswap_sessions: BitswapSessions,
    providers: Providers,
    listen_addrs: Vec<Multiaddr>,
    store:Arc<luffa_store::Store>,
    cache:DiGraph<u64,(u64,u64)>,
    connections:UnGraph<u64,ConnectionEdge>,
    contacts:UnGraph<u64,u8>,
    agent:Option<String>,
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
const EXPIRY_INTERVAL: Duration = Duration::from_secs(1);


impl<KeyStorage: Storage> Node<KeyStorage> {
    pub async fn new(
        config: Config,
        mut keychain: Keychain<KeyStorage>,
        db:Arc<luffa_store::Store>,
        agent:Option<String>,
    ) -> Result<(Self,Sender<rpc::RpcMessage>)> {
        let (network_sender_in, net_receiver_in) = channel(1024); // TODO: configurable
        let (chat_sender, chat_receiver) = channel(1024); // TODO: configurable
        let chat_sender = Arc::new(chat_sender);
        let Config {
            libp2p: libp2p_config,
            ..
        } = config;
      
        let keypair = load_identity(&mut keychain).await?;
        let mut swarm = build_swarm(&libp2p_config, &keypair,db.clone(),agent.clone()).await?;
        let mut listen_addrs = vec![];
        for addr in &libp2p_config.listening_multiaddrs {
            Swarm::listen_on(&mut swarm, addr.clone())?;
            listen_addrs.push(addr.clone());
        }
        let cache = DiGraph::<u64,(u64,u64)>::new();
        let connections = UnGraph::<u64,ConnectionEdge>::with_capacity(1024, 1024);
        let contacts = UnGraph::<u64,u8>::with_capacity(1024, 1024);
        Ok((Node {
            swarm,
            net_receiver_in,
            chat_receiver,
            chat_sender,
            dial_queries: Default::default(),
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
            store:db,
            cache,
            connections,
            contacts,
            agent,
        },network_sender_in))
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

        let mut nice_interval = self.use_dht.then(|| tokio::time::interval(NICE_INTERVAL * 10));
        let mut bootstrap_interval = tokio::time::interval(BOOTSTRAP_INTERVAL);
        let mut expiry_interval = tokio::time::interval(EXPIRY_INTERVAL);

        loop {
            inc!(P2PMetrics::LoopCounter);

            tokio::select! {
                swarm_event = self.swarm.next() => {
                    let swarm_event = swarm_event.expect("the swarm will never die");
                    if let Err(err) = self.handle_swarm_event(swarm_event) {
                        tracing::warn!("swarm error: {:?}", err);
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
                                    continue;
                                }
                                Err(err) => {
                                    tracing::warn!("rpc: {:?}", err);
                                }
                            }
                        }
                        None => {
                            // shutdown
                            return Ok(());
                        }
                    }
                }
                res_channel = self.chat_receiver.recv() => {
                    match res_channel {
                        Some((res,channel))=>{
                            self.handle_chat_response(res,channel);
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
                    // self.dht_nice_tick().await;
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
            trace!(
                "checking node {:?} in bucket range ({:?})",
                dial_opts.get_peer_id().unwrap(),
                range
            );

            if let Err(e) = self.swarm.dial(dial_opts) {
                debug!("failed to dial: {:?}", e);
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
                    debug!("stopping workers {} for session {}", workers.len(), ctx);
                    // first shutdown workers
                    for (closer, worker) in workers {
                        if closer.send(()).is_ok() {
                            worker.await.ok();
                        }
                    }
                    debug!("all workers stopped for session {}", ctx);
                }
                if let Err(err) = client.stop_session(ctx).await {
                    tracing::warn!("failed to stop session {}: {:?}", ctx, err);
                }
                if let Err(err) = response_channel.send(Ok(())) {
                    tracing::warn!("session {} failed to send stop response: {:?}", ctx, err);
                }
                debug!("session {} stopped", ctx);
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
                        debug!("session {}: stopped: closed", ctx);
                    }
                    _ = chan.closed() => {
                        // RPC dropped
                        debug!("session {}: stopped: request canceled", ctx);
                    }
                    block = client.get_block_with_session_id(ctx, &cid, &providers) => match block {
                        Ok(block) => {
                            if let Err(e) = chan.send(Ok(block)) {
                                tracing::warn!("failed to send block response: {:?}", e);
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
    fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<
            <NodeBehaviour as NetworkBehaviour>::OutEvent,
            <<<NodeBehaviour as NetworkBehaviour>::ConnectionHandler as IntoConnectionHandler>::Handler as ConnectionHandler>::Error>,
    ) -> Result<()> {
        libp2p_metrics().record(&event);
        // tracing::info!("swarm>>>> {event:?}");
        match event {
            // outbound events
            SwarmEvent::Behaviour(event) => self.handle_node_event(event),
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
                    let local_id = self.local_peer_id();
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&local_id.to_bytes());
                    let my_id = digest.sum64();
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer_id.to_bytes());
                    let to_id = digest.sum64();
                    let f = self.get_peer_index(my_id);
                    let t = self.get_peer_index(to_id);
                    let time = std::time::SystemTime::now();
                    let time = time.duration_since(std::time::UNIX_EPOCH).unwrap();
                    let time = time.as_millis() as u64;
                    self.connections.update_edge(f, t, ConnectionEdge::Local(peer_id.clone()));
                    
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
                    let f = self.get_peer_index(my_id);
                    let t = self.get_peer_index(to_id);

                    if let Some(e) = self.connections.find_edge(f, t) {
                        self.connections.remove_edge(e);
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
            _ => {
                
                Ok(())
            },
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
                    tracing::warn!("failed to send network event: {:?}", e);
                }
            });
        }
    }

    #[tracing::instrument(skip(self))]
    fn handle_node_event(&mut self, event: Event) -> Result<()> {
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
                                    tracing::warn!("failed to provide {}: {:?}", key, err);
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
                            tracing::warn!("kad bootstrap error: {:?}", e);
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
                debug!("tick: identify {:?}", e);
                if let IdentifyEvent::Received { peer_id, info } = *e {
                    if info.agent_version.contains("Relay") {
                        // TODO: only in my relay white list;
                        for protocol in &info.protocols {
                            let p = protocol.as_bytes();
                            if p == kad::protocol::DEFAULT_PROTO_NAME {
                                for addr in &info.listen_addrs {
                                    if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                                        kad.add_address(&peer_id, addr.clone());
                                    }
                                    if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                                        chat.add_address(&peer_id, addr.clone());
                                    }
                                }
                            }
                        }
                        
                    }
                    else{
                        //TODO only in my contacts
                        for protocol in &info.protocols {
                            let p = protocol.as_bytes();
                            if p == b"/libp2p/autonat/1.0.0" {
                                // TODO: expose protocol name on `libp2p::autonat`.
                                // TODO: should we remove them at some point?
                                for addr in &info.listen_addrs {
                                    if let Some(autonat) = self.swarm.behaviour_mut().autonat.as_mut() {
                                        autonat.add_server(peer_id, Some(addr.clone()));
                                    }
                                }
                            }
                        }
                    }
                    
                    //TODO only in my contacts or my white list of relay
                    if let Some(bitswap) = self.swarm.behaviour().bitswap.as_ref() {
                        bitswap.on_identify(&peer_id, &info.protocols);
                    }

                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .inject_identify_info(peer_id, info.clone());

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
                if let PingResult::Ok(ping) = e.result {
                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .inject_ping(e.peer, ping);
                }
            }
            Event::Relay(e) => {
                libp2p_metrics().record(&e);
            }
            Event::Dcutr(e) => {
                libp2p_metrics().record(&e);
            }
            Event::Gossipsub(e) => {
                libp2p_metrics().record(&e);
                if let libp2p::gossipsub::GossipsubEvent::Message {
                    propagation_source,
                    message_id,
                    message,
                } = e
                {
                    if self.agent == Some(format!("Relay")){
                        match &message {
                            GossipsubMessage { source, data, sequence_number, topic }=>{
                                let event =  luffa_rpc_types::Event::decode(data)?;
                                let luffa_rpc_types::Event {
                                    crc,
                                    from_id,
                                    to,
                                    msg,
                                    nonce,
                                    ..
                                } = event;
                                if nonce.is_none() {
                                    if to > 0 {
                                        return Ok(());
                                    }
                                    if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), None, nonce) {
                                        match msg {
                                            Message::ContactsSync { did,contacts }=>{
                                                let f =  self.get_contacts_index(did);
                                                for ctt in contacts.iter() {
                                                    let t =  self.get_contacts_index(ctt.did);
                                                    let tp = ctt.r#type as u8;
                                                    self.contacts.update_edge(f,t,tp);
                                                }
                                                // let p_idx = self.get_node_index(from_id);
                                                // let mut ls_remove = vec![];
                                                // for ctt in contacts.iter_mut() {
                                                //     if ctt.r#type == ContactsTypes::Group {
                                                //         let ls_crc = self.load_cache_crc(ctt.did,Some(ctt.have_time));
                                                //         let ls_crc = ls_crc.into_iter().map(|(x,_f)|x).collect::<Vec<_>>();
                                                //         ctt.wants.extend_from_slice(&ls_crc);
                                                //     }
                                                //     else{
                                                //         let a = self.get_node_index(ctt.did);
                                                //         self.cache.find_edge(a, p_idx);
                                                //         let mut itr = self.cache.edges_connecting(a, p_idx);
                                                //         let mut ls_crc = vec![];
                                                //         while let Some(e_ref) = itr.next() {
                                                //             let (crc,time) = e_ref.weight();
                                                //             if *time > ctt.have_time {
                                                //                 ls_crc.push((crc,ctt.did));
                                                //             }
                                                //             else{
                                                //                 ls_remove.push(e_ref.id());  
                                                //             } 
                                                //         }
                                                        
                                                //         let ls_crc = ls_crc.into_iter().map(|(x,_)|*x).collect::<Vec<_>>();
                                                //         ctt.wants.extend_from_slice(&ls_crc);
                                                //     }

                                                // }
                                                // for rm in ls_remove {
                                                //     self.cache.remove_edge(rm);
                                                // }
                                                // contacts.retain(|c| !c.wants.is_empty());
                                                // if !contacts.is_empty() {

                                                //     let sync_msg = Message::ContactsSync { did, contacts };
                                                //     let local_id = self.local_peer_id();
                                                //     let mut digest = crc64fast::Digest::new();
                                                //     digest.write(&local_id.to_bytes());
                                                //     let my_id = digest.sum64();
                                                //     if let Some(go) = self.swarm.behaviour_mut().gossipsub.as_mut() {
                                                //         let topic = TopicHash::from_raw(format!(
                                                //             "luffa_chat",
                                                //         ));
                                                //         let e = luffa_rpc_types::Event::new(from_id, &sync_msg, None, my_id);
                                                //         if let Err(e) = go.publish(topic, e.encode().unwrap()) {
                                                //             tracing::warn!("{e:?}");
                                                //         }
                                                //     }
                                                // }
                                                
                                            }
                                            Message::StatusSync { to, from_id, status }=>{
                                                if to > 0 && from_id > 0 {

                                                    let f = self.get_peer_index(from_id);
                                                    let t = self.get_peer_index(to);
                                                    match status {
                                                        AppStatus::Active | AppStatus::Connected =>{
                                                            let time = std::time::SystemTime::now();
                                                            let time = time.duration_since(std::time::UNIX_EPOCH).unwrap();
                                                            let time = time.as_millis() as u64;
                                                            self.connections.update_edge(f, t, ConnectionEdge::Remote(time));
                                                        }
                                                        _=>{
                                                            if let Some(i) = self.connections.find_edge(f,t) {
                                                                self.connections.remove_edge(i);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            _=>{
                                            }
                                        }
                                    }
                                }
                                else{
                                    self.save_cache_crc(crc, from_id, to);
                                    // put record(crc,data)
                                    if let Err(e) = self.put_to_dht(crc, data.clone()) {
                                        tracing::error!("{e:?}");
                                    }
                                }
                            },
                            _=>{

                            }
                        }
                    }
                    self.emit_network_event(NetworkEvent::Gossipsub(GossipsubEvent::Message {
                        from: propagation_source,
                        id: message_id,
                        message,
                    }));
                } else if let libp2p::gossipsub::GossipsubEvent::Subscribed { peer_id, topic } = e {
                    self.emit_network_event(NetworkEvent::Gossipsub(GossipsubEvent::Subscribed {
                        peer_id,
                        topic,
                    }));
                } else if let libp2p::gossipsub::GossipsubEvent::Unsubscribed { peer_id, topic } = e
                {
                    self.emit_network_event(NetworkEvent::Gossipsub(
                        GossipsubEvent::Unsubscribed { peer_id, topic },
                    ));
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
                                tracing::warn!("invalid dial options: {:?}", e);
                            }
                        }
                    }
                }
                mdns::Event::Expired(_) => {}
            },
            Event::Bitswap(_e)=>{
                
            }
            Event::Chat(chat)=>{
                match chat {
                    RequestResponseEvent::Message { peer, message }=>{
                        tracing::info!("Chat Req>>> {peer:?}");
                        match message {
                            RequestResponseMessage::Request { request_id, request, channel }=>{
                                let event =  luffa_rpc_types::Event::decode(request.data()).unwrap();
                                let luffa_rpc_types::Event {
                                    crc,
                                    from_id,
                                    to,
                                    msg,
                                    nonce,
                                    ..
                                } = event;
                                if nonce.is_none() {
                                    if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), None, nonce) {
                                        match msg {
                                            Message::ContactsSync { did,mut contacts }=>{
                                                let p_idx = self.get_node_index(from_id);
                                                let mut ls_remove = vec![];
                                                for ctt in contacts.iter_mut() {
                                                    if ctt.r#type == ContactsTypes::Group {
                                                        let ls_crc = self.load_cache_crc(ctt.did,Some(ctt.have_time));
                                                        let ls_crc = ls_crc.into_iter().map(|(x,_f)|x).collect::<Vec<_>>();
                                                        ctt.wants.extend_from_slice(&ls_crc);
                                                    }
                                                    else{
                                                        let a = self.get_node_index(ctt.did);
                                                        self.cache.find_edge(a, p_idx);
                                                        let mut itr = self.cache.edges_connecting(a, p_idx);
                                                        let mut ls_crc = vec![];
                                                        while let Some(e_ref) = itr.next() {
                                                            let (crc,time) = e_ref.weight();
                                                            if *time > ctt.have_time {
                                                                ls_crc.push((crc,ctt.did));
                                                            }
                                                            else{
                                                               ls_remove.push(e_ref.id());  
                                                            } 
                                                        }
                                                        
                                                        let ls_crc = ls_crc.into_iter().map(|(x,_)|*x).collect::<Vec<_>>();
                                                        ctt.wants.extend_from_slice(&ls_crc);
                                                    }

                                                }
                                                for rm in ls_remove {
                                                    self.cache.remove_edge(rm);
                                                }
                                                contacts.retain(|c| !c.wants.is_empty());

                                                let msg = Message::ContactsSync { did, contacts };

                                                let evnt = luffa_rpc_types::Event::new(from_id,&msg,None,my_id);
                                                let res = evnt.encode().unwrap();
                                                let res = crate::behaviour::chat::Response(res);
                                                let chat = self.swarm.behaviour_mut().chat.as_mut().unwrap();
                                                    
                                                if let Err(e) = chat.send_response(channel, res) {
                                                    tracing::warn!("chat response failed: >>{e:?}");
                                                }
                                                
                                            }
                                            _=>{
                                                let msg = Message::Feedback { crc, status: luffa_rpc_types::FeedbackStatus::Fetch };
                                                let evnt = luffa_rpc_types::Event::new(from_id,&msg,None,my_id);
                                                let res = evnt.encode().unwrap();
                                                let res = crate::behaviour::chat::Response(res);
                                                let chat = self.swarm.behaviour_mut().chat.as_mut().unwrap();
                                                    
                                                if let Err(e) = chat.send_response(channel, res) {
                                                    tracing::warn!("chat response failed: >>{e:?}");
                                                }

                                            }
                                        }
                                    }
                                    else{
                                        let msg = Message::Feedback { crc, status: luffa_rpc_types::FeedbackStatus::Fetch };
                                        let evnt = luffa_rpc_types::Event::new(from_id,&msg,None,my_id);
                                        let res = evnt.encode().unwrap();
                                        let res = crate::behaviour::chat::Response(res);
                                        let chat = self.swarm.behaviour_mut().chat.as_mut().unwrap();
                                            
                                        if let Err(e) = chat.send_response(channel, res) {
                                            tracing::warn!("chat response failed: >>{e:?}");
                                        }
                                    }
                                    
                                }
                                else if to == my_id {
                                    let msg = Message::Chat { content : ChatContent::Feedback { crc, status: luffa_rpc_types::FeedbackStatus::Reach } };
                                    
                                    let evnt = luffa_rpc_types::Event::new(from_id,&msg,None,my_id);
                                    let res = evnt.encode().unwrap();
                                    let res = crate::behaviour::chat::Response(res);
                                    let chat = self.swarm.behaviour_mut().chat.as_mut().unwrap();
                                        
                                    if let Err(e) = chat.send_response(channel, res) {
                                        tracing::warn!("chat response failed: >>{e:?}");
                                    }
                                }
                                else{
                                    let f = self.get_contacts_index(from_id);
                                    let t = self.get_contacts_index(to);
                                    if let Some(idx) = self.contacts.find_edge(f, t) {
                                        self.save_cache_crc(crc, from_id, to);
                                        let tp = self.contacts.edge_weight(idx).unwrap();
                                        if *tp == 0 {
                                            let f = self.get_peer_index(my_id);
                                            let t = self.get_peer_index(to);
                                            let mut edges = self.connections.edges(t);
                                            if let Some(local) = edges.find(|e| match e.weight() {
                                                ConnectionEdge::Local(_)=> true,
                                                _=> false
                                            }) {    
                                                match local.weight() {
                                                    ConnectionEdge::Local(p)=> {
                                                        if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                                                            let msg = request.data().to_vec();
                                                            let (tx,rx) = tokio::sync::oneshot::channel();
                                                            let req_id = chat.send_request(p, crate::behaviour::chat::Request(msg));
                                                            self.pending_request.insert(req_id, tx);
                                                            tracing::info!("chat send. {:?}",req_id);     
                                                            
                                                            let sender = self.chat_sender.clone();
                                                            tokio::spawn(async move {
                                                                if let Ok(res) = rx.await {
                                                                    if let Ok(Some(cr)) = res {
                                                                        let res = crate::behaviour::chat::Response(cr.data);
                                                                        if let Err(e) = sender.send((res,channel)).await {
                                                                            tracing::warn!("{e:?}");
                                                                        }
                                                                        
                                                                    };
                                                                }
                                                            });
                                                            return Ok(());
                                                        }
                                                    },
                                                    _=>{
                                                        unreachable!()
                                                    }
                                                }
                                            }
                                            //edges.any(|x|)
                                            let paths = k_shortest_path(&self.connections, f, Some(t), 2, |_e| 1);
                                            let mut has_resend = false;
                                            for (t_idx,_) in paths {
                                                let mut edges = self.connections.edges(t_idx);
                                                if let Some(local) = edges.find(|e| match e.weight() {
                                                    ConnectionEdge::Local(_)=> true,
                                                    _=> false
                                                }) {    
                                                    match local.weight() {
                                                        ConnectionEdge::Local(p)=> {
                                                            if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                                                                let msg = request.data().to_vec();
                                                                let (tx,rx) = tokio::sync::oneshot::channel();
                                                                let req_id = chat.send_request(p, crate::behaviour::chat::Request(msg));
                                                                self.pending_request.insert(req_id, tx);
                                                                tracing::info!("chat send. {:?}",req_id);     
                                                                let sender = self.chat_sender.clone();
                                                                tokio::spawn(async move {
                                                                    if let Ok(res) = rx.await {
                                                                        if let Ok(Some(cr)) = res {
                                                                            let res = crate::behaviour::chat::Response(cr.data);
                                                                            if let Err(e) = sender.send((res,channel)).await {
                                                                                tracing::warn!("{e:?}");
                                                                            }
                                                                            
                                                                        };
                                                                    }
                                                                });
                                                                has_resend = true;
                                                                break;
                                                            }
                                                        },
                                                        _=>{
                                                            unreachable!()
                                                        }
                                                    }
                                                }
                                            }
                                            if has_resend {
                                                return Ok(());
                                            }

                                            // let msg = Message::Chat { content : ChatContent::Feedback { crc, status: luffa_rpc_types::FeedbackStatus::Fetch } };
                                    
                                            // let evnt = luffa_rpc_types::Event::new(from_id,&msg,None,my_id);
                                            // let res = evnt.encode().unwrap();
                                            // let res = crate::behaviour::chat::Response(res);
                                            // let chat = self.swarm.behaviour_mut().chat.as_mut().unwrap();
                                                
                                            // if let Err(e) = chat.send_response(channel, res) {
                                            //     tracing::warn!("chat response failed: >>{e:?}");
                                            // }
                                        }
                                        else{
                                            //TODO Group msg

                                        }
                                    }

                                    // put record(crc,data)
                                    if let Err(e) = self.put_to_dht(crc, request.data().to_vec()) {
                                        tracing::error!("{e:?}");
                                    }
                                    // TODO redirect to other relay node or client if it is connected with this node; 
                                    
                                    // Message::Feedback { crc, status: luffa_rpc_types::FeedbackStatus::Fetch }
                                }; 
                                // let evnt = luffa_rpc_types::Event::new(from_id,&msg,None,0);
                                // let res = evnt.encode().unwrap();
                                // let res = crate::behaviour::chat::Response(res);
                                // let chat = self.swarm.behaviour_mut().chat.as_mut().unwrap();
                                    
                                // if let Err(e) = chat.send_response(channel, res) {
                                //     tracing::warn!("chat response failed: >>{e:?}");
                                // }
                            }
                            RequestResponseMessage::Response { request_id, response }=>{
                                if let Some(channel) = self.pending_request.remove(&request_id) {
                                    if let Err(_e) = channel.send(Ok(Some(ChatResponse {
                                        data: response.0
                                    }))) {
                                        tracing::warn!("channel response failed");
                                    }
                                }
                                // self.emit_network_event(NetworkEvent::RequestResponse(ChatEvent::Response { request_id, data: response.0 }));
                                
                            }
                        }
                    }
                    RequestResponseEvent::ResponseSent { peer, request_id }=>{
                        tracing::warn!("Chat Response Sent>>{peer:?}  {:?} ",request_id);
                        self.emit_network_event(NetworkEvent::RequestResponse(ChatEvent::ResponseSent { peer, request_id }));
                    }
                    RequestResponseEvent::InboundFailure { peer, request_id, error }=>{
                        
                        self.emit_network_event(NetworkEvent::RequestResponse(ChatEvent::InboundFailure { peer, request_id, error }));
                    }
                    RequestResponseEvent::OutboundFailure { peer, request_id, error }=>{
                        self.emit_network_event(NetworkEvent::RequestResponse(ChatEvent::OutboundFailure { peer, request_id, error }));

                    }
                }
            }
            _ => {
                // TODO: check all important events are handled
            }
        }

        Ok(())
    }
    
    fn handle_chat_response(&mut self,res:crate::behaviour::chat::Response,channel:libp2p::request_response::ResponseChannel<crate::behaviour::chat::Response>) {
        if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
            if let Err(e) = chat.send_response(channel, res) {
                tracing::warn!("{e:?}");
            }
        }
    }
    
    fn get_node_index(&mut self,did:u64) -> NodeIndex {
        match self.cache.node_indices().find(|n| self.cache[*n] == did) {
            Some(v)=> v,
            None=>{
                self.cache.add_node(did)
            }
        }
    }
    fn get_peer_index(&mut self,did:u64) -> NodeIndex {
        match self.connections.node_indices().find(|n| self.connections[*n] == did) {
            Some(v)=> v,
            None=>{
                self.connections.add_node(did)
            }
        }
    }
    fn get_contacts_index(&mut self,did:u64) -> NodeIndex {
        match self.contacts.node_indices().find(|n| self.contacts[*n] == did) {
            Some(v)=> v,
            None=>{
                self.contacts.add_node(did)
            }
        }
    }
    fn save_cache_crc(&mut self,crc:u64,from_id:u64,to:u64) -> EdgeIndex {
        let from = self.get_node_index(from_id);
        let to = self.get_node_index(to);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
        self.cache.add_edge(from, to, (crc,now.as_millis() as u64))
    }
    fn put_to_dht(&mut self,crc:u64,data:Vec<u8>) -> anyhow::Result<QueryId>{
        if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
            let record = Record::new(crc.to_be_bytes().to_vec(), data);
            kad.put_record(record, Quorum::One).map_err(|e| anyhow!("{e:?}"))
        }
        else{
            Err(anyhow!("put to dht failed"))
        }
        
    }
    fn load_cache_crc(&mut self,did:u64,have_time:Option<u64>) -> Vec<(u64,u64)>{
        let to = self.get_node_index(did);
        let mut cached_crc = vec![];
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
        let now = now.as_millis() as u64;
        let have_time = have_time.unwrap_or(0);
        let mut remove_edges = vec![];
        if let Some(mut e) = self.cache.first_edge(to, Direction::Incoming) {
            let (crc,time) = self.cache.edge_weight(e).unwrap();
            if now - *time < 24 * 60 * 60 * 1000  && *time > have_time {
                if let Some((a,_b)) = self.cache.edge_endpoints(e) {
                    let from_id = self.cache.node_weight(a).unwrap();
                    cached_crc.push((*crc,*from_id));
                    
                    while let Some(n) = self.cache.next_edge(e, Direction::Incoming) {
                        let (crc,time) = self.cache.edge_weight(n).unwrap();
                        if now - *time > 24 * 60 * 60 * 1000 {
                            remove_edges.push(n);
                            continue;
                        }
                        if *time < have_time {
                            break;
                        }
                        let (a,_b) = self.cache.edge_endpoints(n).unwrap();
                        let from_id = self.cache.node_weight(a).unwrap();
                        cached_crc.push((*crc,*from_id));
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

    #[tracing::instrument(skip(self))]
    fn handle_rpc_message(&mut self, message: RpcMessage) -> Result<bool> {
        // Inbound messages
        match message {
            RpcMessage::BackGossipsub(msg)=>{
                match msg {
                    crate::rpc::GossipsubMessage::Publish(s,topic,data)=>{
                        let from = self.local_peer_id().clone();
                        let id = MessageId::new(&[0]);
                        let data = data.to_vec();
                        let message = libp2p::gossipsub::GossipsubMessage {
                            source:None,
                            sequence_number:None,
                            topic,
                            data,
                        };
                        self.emit_network_event(NetworkEvent::Gossipsub(GossipsubEvent::Message { from, id, message }))
                    }
                    _=>{

                    }
                }
            }
            RpcMessage::Chat(response_channel, data)=>{

                let mut peers = 
                {
                    self.swarm.connected_peers().map(|p| {
                        match self.swarm.behaviour().peer_manager.info_for_peer(p) {
                            Some(pp)=>{
                                
                                match &pp.last_info {
                                    Some(info)=>{
                                        Some((p.clone(),info.clone(),pp.last_rtt.clone()))
                                    }
                                    None=>{
                                        None
                                    }
                                }
                            }
                            None=>{
                                None
                            }
                        }
                        
                    })
                    .filter(|item| item.is_some())
                    .map(|item| item.unwrap())
                    .filter(|(_p,info,_)|{
                        
                        info.agent_version.contains("Relay")                        
                    })
                    .collect::<Vec<_>>()
            };

                peers.sort_by(|(_,_e,a),(_,_f,b)| {
                    let a = a.unwrap_or(Duration::from_secs(5)).as_millis();
                    let b = b.unwrap_or(Duration::from_secs(5)).as_millis();
                    a.partial_cmp(&b).unwrap()
                });
                if let Some(chat) = self.swarm.behaviour_mut().chat.as_mut() {
                    
                    if let Some((peer,_,_)) = peers.first() {
                        let req_id = chat.send_request(peer, crate::behaviour::chat::Request(data.msg));
                        self.pending_request.insert(req_id, response_channel);
                        tracing::info!("chat send. {:?}",req_id);     
                        return Ok(false);
                    }
                    
                }
                
                if let Err(_e) = response_channel.send(Err(anyhow!("not request"))) {
                    tracing::warn!("channel response failed");
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
                let (tx,rx) = std::sync::mpsc::channel();
                tokio::spawn(async move {
                    for (cid, response_channel) in cids.into_iter().zip(response_channels.into_iter()) {
                        match store.get(&cid) {
                            Ok(Some(blob))=>{
                                let blk = Block::new(blob, cid);
                                if let Err(e) = response_channel.send(Ok(blk)) {
                                    tracing::error!("{e:?}");
                                }
                            }
                            _=>{
                                tx.send((cid,response_channel)).unwrap();    
                            }
                        }
                        
                    }
                });
                while let Ok((cid, response_channel)) = rx.recv() {
                    if let Err(e) = self.want_block(ctx, cid, providers.clone(), response_channel)
                    .map_err(|err| anyhow!("Failed to send a bitswap want_block: {:?}", err)) {
                        tracing::error!("{e:?}");
                    }
                }
            }
            RpcMessage::PushBitswapRequest { data, response_channels } => {
                tracing::debug!("PushBitswapRequest:--{}---",data.len());
                let cid = Cid::new_v1(DagCborCodec.into(), multihash::Code::Sha2_256.digest(&data[..]));

                let store = self.store.clone();
                let blob = data.clone();
                tokio::spawn(async move {
                    if let Err(e) = store.put(cid, blob, vec![]) {
                        tracing::warn!("store put> {:?}",e);
                    }
                });
                let ret = cid.clone();
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    let key = libp2p::kad::record::Key::new(&ret.hash().to_bytes());
                    kad.start_providing(key).map_err(|_|anyhow!("Failed to push bitswap") )?;
                }
                self.swarm.behaviour().notify_new_blocks(vec![Block {cid,data}]);
                for channel in response_channels {
                    channel.send(Ok(ret.clone())).map_err(|_| anyhow!("Failed to push bitswap"))?;
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
                        .put_record(record, Quorum::N(NonZeroUsize::new(2).expect("3 != 1")))
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
            RpcMessage::PutRecordTo(response_channel,peers, record) => {
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
                        tracing::warn!("invalid dial options: {:?}", e);
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
                tracing::warn!("NetDisconnect API not yet implemented"); // TODO: implement NetDisconnect

                response_channel
                    .send(())
                    .map_err(|_| anyhow!("sender dropped"))?;
            }
            RpcMessage::Gossipsub(g) => {
                let gossipsub = match self.swarm.behaviour_mut().gossipsub.as_mut() {
                    Some(gossipsub) => gossipsub,
                    None => {
                        tracing::warn!("Unexpected gossipsub message");
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
                        let res = gossipsub
                            .publish(topic_hash, bytes.to_vec());
                        // tracing::warn!("rpc >> publish. {}",res.is_ok());
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
}

pub async fn load_identity<S: Storage>(kc: &mut Keychain<S>) -> Result<Keypair> {
    if kc.is_empty().await? {
        tracing::info!("no identity found, creating",);
        kc.create_ed25519_key().await?;
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
