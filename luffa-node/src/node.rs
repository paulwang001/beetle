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
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{ConnectionHandler, IntoConnectionHandler, NetworkBehaviour, SwarmEvent};
use libp2p::{PeerId, Swarm};
use luffa_bitswap::{BitswapEvent, Block};
use luffa_metrics::{core::MRecorder, inc, libp2p_metrics, p2p::P2PMetrics};
use multihash::MultihashDigest;
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
    CancelLookupQuery(PeerId),
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
    dial_queries: AHashMap<PeerId, Vec<OneShotSender<Result<()>>>>,
    lookup_queries: AHashMap<PeerId, Vec<oneshot::Sender<Result<IdentifyInfo>>>>,
    // TODO(ramfox): use new providers queue instead
    find_on_dht_queries: AHashMap<Vec<u8>, DHTQuery>,
    record_on_dht_queries: AHashMap<Vec<u8>, (QueryId, Vec<OneShotSender<Result<Option<Record>>>>)>,
    provider_on_dht_queries: AHashMap<Vec<u8>, (QueryId, Vec<OneShotSender<Result<QueryId>>>)>,
    network_events: Vec<Sender<NetworkEvent>>,
    _keychain: Keychain<KeyStorage>,
    #[allow(dead_code)]
    kad_last_range: Option<(Distance, Distance)>,
    use_dht: bool,
    bitswap_sessions: BitswapSessions,
    providers: Providers,
    listen_addrs: Vec<Multiaddr>,
    store:Arc<luffa_store::Store>,
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
    ) -> Result<(Self,Sender<rpc::RpcMessage>)> {
        let (network_sender_in, net_receiver_in) = channel(1024); // TODO: configurable
        let Config {
            libp2p: libp2p_config,
            ..
        } = config;
      
        let keypair = load_identity(&mut keychain).await?;
        let mut swarm = build_swarm(&libp2p_config, &keypair,db.clone()).await?;
        let mut listen_addrs = vec![];
        for addr in &libp2p_config.listening_multiaddrs {
            Swarm::listen_on(&mut swarm, addr.clone())?;
            listen_addrs.push(addr.clone());
        }

        Ok((Node {
            swarm,
            net_receiver_in,
            dial_queries: Default::default(),
            lookup_queries: Default::default(),
            // TODO(ramfox): use new providers queue instead
            find_on_dht_queries: Default::default(),
            record_on_dht_queries: Default::default(),
            provider_on_dht_queries: Default::default(),
            network_events: Vec::new(),
            _keychain: keychain,
            kad_last_range: None,
            use_dht: libp2p_config.kademlia,
            bitswap_sessions: Default::default(),
            providers: Providers::new(4),
            listen_addrs,
            store:db,
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
        println!("Listen addrs: {:?}", self.listen_addrs());
        info!("Local Peer ID: {}", self.local_peer_id());

        let mut nice_interval = self.use_dht.then(|| tokio::time::interval(NICE_INTERVAL * 10));
        let mut bootstrap_interval = tokio::time::interval(BOOTSTRAP_INTERVAL);
        let mut expiry_interval = tokio::time::interval(EXPIRY_INTERVAL);

        loop {
            inc!(P2PMetrics::LoopCounter);

            tokio::select! {
                swarm_event = self.swarm.next() => {
                    let swarm_event = swarm_event.expect("the swarm will never die");
                    if let Err(err) = self.handle_swarm_event(swarm_event) {
                        eprintln!("swarm error: {:?}", err);
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
                                    warn!("rpc: {:?}", err);
                                }
                            }
                        }
                        None => {
                            // shutdown
                            return Ok(());
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
                    println!("[{}] Peers connected: {:?}",self.local_peer_id(), self.swarm.connected_peers().count());
                    // self.dht_nice_tick().await;
                }
                _ = bootstrap_interval.tick() => {
                    if let Err(e) = self.swarm.behaviour_mut().kad_bootstrap() {
                        eprintln!("kad bootstrap failed: {:?}", e);
                    }
                    else{
                        println!("kad bootstrap successfully");
                    }
                }
                _ = expiry_interval.tick() => {
                    if let Err(err) = self.expiry() {
                        warn!("expiry error {:?}", err);
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
                eprintln!("failed to dial: {:?}", e);
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
                    warn!("failed to stop session {}: {:?}", ctx, err);
                }
                if let Err(err) = response_channel.send(Ok(())) {
                    warn!("session {} failed to send stop response: {:?}", ctx, err);
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
                                warn!("failed to send block response: {:?}", e);
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
        // println!("swarm>>>> {event:?}");
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
                    self.emit_network_event(NetworkEvent::PeerConnected(peer_id));
                }
                println!("ConnectionEstablished: {:}", peer_id);
                Ok(())
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                if num_established == 0 {
                    self.emit_network_event(NetworkEvent::PeerDisconnected(peer_id));
                }

                println!("ConnectionClosed: {:}", peer_id);
                Ok(())
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error } => {
                eprintln!("failed to dial: {:?}, {:?}", peer_id, error);

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
        println!("emit>>>{ev:?}");
        for sender in &mut self.network_events {
            let ev = ev.clone();
            let sender = sender.clone();
            tokio::task::spawn(async move {
                if let Err(e) = sender.send(ev.clone()).await {
                    eprintln!("failed to send network event: {:?}", e);
                }
            });
        }
    }

    #[tracing::instrument(skip(self))]
    fn handle_node_event(&mut self, event: Event) -> Result<()> {
        // println!("node>>> {event:?}");
        match event {
            Event::Bitswap(e) => {
                match e {
                    BitswapEvent::Provide { key } => {
                        println!("bitswap provide {}", key);
                        if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                            match kad.start_providing(key.hash().to_bytes().into()) {
                                Ok(_query_id) => {
                                    // TODO: track query?
                                }
                                Err(err) => {
                                    eprintln!("failed to provide {}: {:?}", key, err);
                                }
                            }
                        }
                    }
                    BitswapEvent::FindProviders {
                        key,
                        response,
                        limit,
                    } => {
                        info!("bitswap find providers {}", key);
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
                            warn!("kad bootstrap error: {:?}", e);
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
                                warn!("FinishedWithNoAdditionalRecord");
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
                println!("tick: identify {:?}", e);
                if let IdentifyEvent::Received { peer_id, info } = *e {
                    for protocol in &info.protocols {
                        let p = protocol.as_bytes();

                        if p == kad::protocol::DEFAULT_PROTO_NAME {
                            for addr in &info.listen_addrs {
                                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                                    kad.add_address(&peer_id, addr.clone());
                                }
                            }
                        } else if p == b"/libp2p/autonat/1.0.0" {
                            // TODO: expose protocol name on `libp2p::autonat`.
                            // TODO: should we remove them at some point?
                            for addr in &info.listen_addrs {
                                if let Some(autonat) = self.swarm.behaviour_mut().autonat.as_mut() {
                                    autonat.add_server(peer_id, Some(addr.clone()));
                                }
                            }
                        }
                    }
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
                                warn!("invalid dial options: {:?}", e);
                            }
                        }
                    }
                }
                mdns::Event::Expired(_) => {}
            },
            Event::Bitswap(e)=>{
                
            }
            _ => {
                // TODO: check all important events are handled
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn handle_rpc_message(&mut self, message: RpcMessage) -> Result<bool> {
        // Inbound messages
        match message {
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
                tracing::warn!("context:{} bitswap_request", ctx);
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
                tracing::warn!("PushBitswapRequest:--{}---",data.len());
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
                        warn!("invalid dial options: {:?}", e);
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
                warn!("NetDisconnect API not yet implemented"); // TODO: implement NetDisconnect

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
        info!("no identity found, creating",);
        kc.create_ed25519_key().await?;
    }

    // for now we just use the first key
    let first_key = kc.keys().next().await;
    if let Some(keypair) = first_key {
        let keypair: Keypair = keypair?.into();
        info!("identity loaded: {}", PeerId::from(keypair.public()));
        return Ok(keypair);
    }

    Err(anyhow!("inconsistent keystate"))
}
