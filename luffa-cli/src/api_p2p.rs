use anyhow::{Ok, Result};
use futures::StreamExt;
use libp2p::{gossipsub::TopicHash, multiaddr::Protocol, Multiaddr, PeerId};
use luffa_rpc_client::{Lookup, P2pClient};
use std::collections::HashMap;

use crate::error::map_service_error;

#[derive(Debug)]
pub struct P2p {
    client: P2pClient,
}

#[derive(Debug, Clone)]
pub enum PeerIdOrAddr {
    PeerId(PeerId),
    Multiaddr(Multiaddr),
}

impl P2p {
    pub fn new(client: P2pClient) -> Self {
        Self { client }
    }

    pub async fn lookup_local(&self) -> Result<Lookup> {
        self.client.lookup_local().await
    }

    pub async fn lookup(&self, addr: &PeerIdOrAddr) -> Result<Lookup> {
        match addr {
            PeerIdOrAddr::PeerId(peer_id) => self.client.lookup(*peer_id, None).await,
            PeerIdOrAddr::Multiaddr(addr) => {
                let peer_id = peer_id_from_multiaddr(addr)?;
                self.client.lookup(peer_id, Some(addr.clone())).await
            }
        }
        .map_err(|e| map_service_error("relay", e))
    }

    pub async fn connect(&self, addr: &PeerIdOrAddr) -> Result<()> {
        match addr {
            PeerIdOrAddr::PeerId(peer_id) => self.client.connect(*peer_id, vec![]).await,
            PeerIdOrAddr::Multiaddr(addr) => {
                let peer_id = peer_id_from_multiaddr(addr)?;
                self.client.connect(peer_id, vec![addr.clone()]).await
            }
        }
        .map_err(|e| map_service_error("relay", e))
    }

    pub async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>> {
        self.client
            .get_peers()
            .await
            .map_err(|e| map_service_error("relay", e))
    }

    pub async fn addresses(&self) -> Result<Vec<Multiaddr>> {
        self.client
            .external_addresses()
            .await
            .map_err(|e| map_service_error("relay", e))
    }
    pub async fn listening(&self) -> Result<(PeerId, Vec<Multiaddr>)> {
        self.client
            .get_listening_addrs()
            .await
            .map_err(|e| map_service_error("relay", e))
    }

    pub async fn publish(&self, topic: String, data: bytes::Bytes) -> Result<()> {
        let topic_hash = TopicHash::from_raw(topic);
        let msg = self.client.gossipsub_publish(topic_hash, data).await?;
        tracing::info!("gossip publish:{msg:?}");
        Ok(())
    }

    pub async fn subscribe(&self, topic: String) -> Result<bool> {
        let topic_hash = TopicHash::from_raw(topic);
        let msg = self.client.gossipsub_subscribe(topic_hash).await?;
        tracing::info!("gossip subscribe:{msg:?}");
        Ok(msg)
    }
    pub async fn unsubscribe(&self, topic: String) -> Result<bool> {
        let topic_hash = TopicHash::from_raw(topic);
        let msg = self.client.gossipsub_unsubscribe(topic_hash).await?;
        tracing::info!("gossip unsubscribe:{msg:?}");
        Ok(msg)
    }
    pub async fn mesh_peers(&self, topic: String) -> Result<Vec<PeerId>> {
        let topic_hash = TopicHash::from_raw(topic);
        self.client
            .gossipsub_mesh_peers(topic_hash)
            .await
            .map_err(|e| map_service_error("relay", e))
    }
    pub async fn push(&self, data: bytes::Bytes) -> Result<cid::Cid> {
        let rsp = self
            .client
            .push_data(data)
            .await
            .map_err(|e| map_service_error("relay", e))?;
        Ok(rsp.cid)
    }
    pub async fn fetch(&self, ctx: u64, cid: cid::Cid) -> Result<Option<bytes::Bytes>> {
        let providers = self.client.fetch_providers_dht(&cid).await?;
        let providers = providers.collect::<Vec<_>>();
        let mut itr = providers.await.into_iter();
        while let Some(pp) = itr.next() {
            let pp = pp.unwrap_or_default();
            tracing::info!("fetch from: {pp:?}");
            if pp.is_empty() {
                continue;
            }
            let rsp = self
                .client
                .fetch_bitswap(ctx, cid, pp)
                .await
                .map_err(|e| map_service_error("relay", e))?;
            return Ok(Some(rsp));
        }
        Ok(None)
    }
}

fn peer_id_from_multiaddr(addr: &Multiaddr) -> Result<PeerId> {
    match addr.iter().find(|p| matches!(*p, Protocol::P2p(_))) {
        Some(Protocol::P2p(peer_id)) => {
            PeerId::from_multihash(peer_id).map_err(|m| anyhow::anyhow!("Multiaddress contains invalid p2p multihash {:?}. Cannot derive a PeerId from this address.", m ))
        }
        ,
        _ => anyhow::bail!("Mulitaddress must include the peer id"),
    }
}
