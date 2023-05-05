use std::{
    num::NonZeroUsize,
    task::{Context, Poll},
    time::Duration,
};

use ahash::AHashMap;
use libp2p::{
    core::{transport::ListenerId, ConnectedPoint},
    identify::Info as IdentifyInfo,
    ping::Success as PingSuccess,
    swarm::{
        dummy, ConnectionHandler, DialError, IntoConnectionHandler, NetworkBehaviour,
        NetworkBehaviourAction, PollParameters,
    },
    Multiaddr, PeerId,
};
use lru::LruCache;
use luffa_metrics::{core::MRecorder, inc, p2p::P2PMetrics};

pub struct PeerManager {
    info: AHashMap<PeerId, Info>,
    bad_peers: LruCache<PeerId, ()>,
    supported_protocols: Vec<String>,
}

#[derive(Default, Debug, Clone)]
pub struct Info {
    pub last_rtt: Option<Duration>,
    pub last_info: Option<IdentifyInfo>,
}

impl Info {
    pub fn latency(&self) -> Option<Duration> {
        // only approximation, this is wrong but the best we have for now
        self.last_rtt.map(|rtt| rtt / 2)
    }
}

const DEFAULT_BAD_PEER_CAP: Option<NonZeroUsize> = NonZeroUsize::new(10 * 4096);

impl Default for PeerManager {
    fn default() -> Self {
        PeerManager {
            info: Default::default(),
            bad_peers: LruCache::new(DEFAULT_BAD_PEER_CAP.unwrap()),
            supported_protocols: Default::default(),
        }
    }
}

#[derive(Debug)]
pub enum PeerManagerEvent {}

impl PeerManager {
    pub fn is_bad_peer(&self, peer_id: &PeerId) -> bool {
        self.bad_peers.contains(peer_id)
    }

    pub fn inject_identify_info(&mut self, peer_id: PeerId, new_info: Option<IdentifyInfo>) {
        self.info.entry(peer_id).or_default().last_info = new_info;
    }

    pub fn inject_ping(&mut self, peer_id: PeerId, new_ping: PingSuccess) {
        if let PingSuccess::Ping { rtt } = new_ping {
            self.info.entry(peer_id).or_default().last_rtt = Some(rtt);
        }
    }

    pub fn info_for_peer(&self, peer_id: &PeerId) -> Option<&Info> {
        self.info.get(peer_id)
    }
    pub fn all_peers(&self) -> Vec<&PeerId> {
        self.info.keys().collect::<Vec<_>>()
    }
    pub fn supported_protocols(&self) -> Vec<String> {
        self.supported_protocols.clone()
    }
}

impl NetworkBehaviour for PeerManager {
    type ConnectionHandler = dummy::ConnectionHandler;
    type OutEvent = PeerManagerEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        dummy::ConnectionHandler
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.info
            .get(peer_id)
            .and_then(|i| i.last_info.as_ref())
            .map(|i| i.listen_addrs.clone())
            .unwrap_or_default()
    }

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {}

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: libp2p::swarm::ConnectionId,
        _event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        params: &mut impl PollParameters,
    ) -> Poll<libp2p::swarm::ToSwarm<Self::OutEvent, libp2p::swarm::THandlerInEvent<Self>>> {
        if self.supported_protocols.is_empty() {
            self.supported_protocols = params
                .supported_protocols()
                .map(|p| String::from_utf8_lossy(&p).to_string())
                .collect();
        }
        Poll::Pending
    }
}
