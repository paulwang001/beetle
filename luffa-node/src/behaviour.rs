use std::iter;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use libp2p::core::identity::Keypair;
use libp2p::core::PeerId;
use libp2p::gossipsub::{self, MessageAuthenticity};
use libp2p::identify;
use libp2p::kad::store::{MemoryStore, MemoryStoreConfig};
use libp2p::kad::{Kademlia, KademliaConfig};
use libp2p::mdns::tokio::Behaviour as Mdns;
use libp2p::multiaddr::Protocol;
use libp2p::ping::Behaviour as Ping;
use libp2p::relay;
use libp2p::request_response::RequestResponse;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{autonat, dcutr};
use luffa_bitswap::{Bitswap, Block, Config as BitswapConfig, Store};

mod event;
mod peer_manager;
pub(crate) mod chat;
pub(crate) use self::event::Event;
use self::peer_manager::PeerManager;
use crate::config::Libp2pConfig;

pub const PROTOCOL_VERSION: &str = "luffa/0.1.0";
pub const AGENT_VERSION: &str = concat!("/", env!("CARGO_PKG_VERSION"));

/// Libp2p behaviour for the node.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event")]
pub(crate) struct NodeBehaviour {
    ping: Ping,
    pub(crate) keep : libp2p::swarm::keep_alive::Behaviour, 
    pub(crate) identify: identify::Behaviour,
    pub(crate) bitswap: Toggle<Bitswap<BitswapStore>>,
    pub(crate) kad: Toggle<Kademlia<MemoryStore>>,
    mdns: Toggle<Mdns>,
    pub(crate) chat:Toggle<RequestResponse<chat::ChatCodec>>,
    pub(crate) autonat: Toggle<autonat::Behaviour>,
    relay: Toggle<relay::v2::relay::Relay>,
    relay_client: Toggle<relay::v2::client::Client>,
    dcutr: Toggle<dcutr::behaviour::Behaviour>,
    pub(crate) gossipsub: Toggle<gossipsub::Gossipsub>,
    pub(crate) peer_manager: PeerManager,
}

#[derive(Debug, Clone)]
pub(crate) struct BitswapStore(Arc<luffa_store::Store>);

#[async_trait]
impl Store for BitswapStore {
    async fn get(&self, cid: &Cid) -> Result<Block> {
        let store = self.0.clone();
        let data = store
            .get(cid)?
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        Ok(Block::new(data, *cid))
    }

    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        let store = self.0.clone();
        let size = store
            .get_size(cid)?
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        Ok(size as usize)
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        let store = self.0.clone();
        let res = store.has(cid)?;
        Ok(res)
    }
}

impl NodeBehaviour {
    pub async fn new(
        local_key: &Keypair,
        config: &Libp2pConfig,
        relay_client: Option<relay::v2::client::Client>,
        db: Arc<luffa_store::Store>,
        agent_version:Option<String>
    ) -> Result<Self> {
        let mut peer_manager = PeerManager::default();
        let pub_key = local_key.public();
        let peer_id = pub_key.to_peer_id();

        let bitswap = if config.bitswap_client || config.bitswap_server {
            // TODO(dig): server only mode is not implemented yet
            let bs_config = if config.bitswap_server {
                BitswapConfig::default()
            } else {
                BitswapConfig::default_client_mode()
            };
            tracing::info!("init bitswap:{bs_config:?}");
            Some(Bitswap::new(peer_id, BitswapStore(db), bs_config).await)
        } else {
            tracing::info!("disabled bitswap");
            None
        }
        .into();

        let mdns = if config.mdns {
            tracing::info!("init mdns");
            Some(Mdns::new(Default::default())?)
        } else {
            None
        }
        .into();
        let cfg = libp2p::request_response::RequestResponseConfig::default();
        let protocols = iter::once((chat::ChatProtocol(), libp2p::request_response::ProtocolSupport::Full));
        let mut chat = Some(libp2p::request_response::RequestResponse::new(chat::ChatCodec(), protocols, cfg)); 
        chat.as_mut().map(|rq|{
            for multiaddr in &config.bootstrap_peers {
                // TODO: move parsing into config
                let mut addr = multiaddr.to_owned();
                if let Some(Protocol::P2p(mh)) = addr.pop() {
                    let peer_id = PeerId::from_multihash(mh).unwrap();
                    tracing::info!("add boot to chat>> {:?}  {:?}",peer_id,addr);
                    rq.add_address(&peer_id, addr);
                    peer_manager.inject_identify_info(peer_id, None);
                } else {
                    tracing::info!("Could not parse bootstrap addr {}", multiaddr);
                }
            }
        });
        let kad = if config.kademlia {
            tracing::info!("init kademlia");
            // TODO: persist to store
            let mem_store_config = MemoryStoreConfig {
                max_records: 1024 * 64,
                max_provided_keys: 1024 * 1024,
                ..Default::default()
            };
            let store = MemoryStore::with_config(peer_id, mem_store_config);

            // TODO: make user configurable
            let mut kad_config = KademliaConfig::default();
            kad_config.set_parallelism(16usize.try_into().unwrap());
            // TODO: potentially lower (this is per query)
            kad_config.set_query_timeout(Duration::from_secs(60));

            let mut kademlia = Kademlia::with_config(pub_key.to_peer_id(), store, kad_config);
            for multiaddr in &config.bootstrap_peers {
                // TODO: move parsing into config
                let mut addr = multiaddr.to_owned();
                if let Some(Protocol::P2p(mh)) = addr.pop() {
                    let peer_id = PeerId::from_multihash(mh).unwrap();
                    tracing::info!("add boot>> {:?}  {:?}",peer_id,addr);
                    kademlia.add_address(&peer_id, addr);
                } else {
                    tracing::info!("Could not parse bootstrap addr {}", multiaddr);
                }
            }

            // Trigger initial bootstrap
            if let Err(e) = kademlia.bootstrap() {
                tracing::info!("Kademlia bootstrap failed: {}", e);
            }

            Some(kademlia)
        } else {
            None
        }
        .into();

        let autonat = if config.autonat {
            tracing::info!("init autonat");
            let pub_key = local_key.public();
            let config = autonat::Config {
                use_connected: true,
                boot_delay: Duration::from_secs(0),
                refresh_interval: Duration::from_secs(5),
                retry_interval: Duration::from_secs(5),
                ..Default::default()
            }; // TODO: configurable
            let autonat = autonat::Behaviour::new(pub_key.to_peer_id(), config);
            Some(autonat)
        } else {
            None
        }
        .into();

        let relay = if config.relay_server {
            tracing::info!("init relay server");
            let config = relay::v2::relay::Config::default();
            let r = relay::v2::relay::Relay::new(local_key.public().to_peer_id(), config);
            Some(r)
        } else {
            None
        }
        .into();

        let (dcutr, relay_client) = if config.relay_client {
            tracing::info!("init relay client");
            let relay_client =
                relay_client.expect("missing relay client even though it was enabled");
            let dcutr = dcutr::behaviour::Behaviour::new();
            (Some(dcutr), Some(relay_client))
        } else {
            (None, None)
        };

        let identify = {
            let config = identify::Config::new(PROTOCOL_VERSION.into(), local_key.public())
                .with_agent_version(format!("{}{}",agent_version.unwrap_or(String::from("luffa")),AGENT_VERSION))
                .with_cache_size(64 * 1024);
            identify::Behaviour::new(config)
        };

        let gossipsub = if config.gossipsub {
            tracing::info!("init gossipsub");
            let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
            .validation_mode(gossipsub::ValidationMode::Strict).do_px().build().unwrap();
            // let gossipsub_config = gossipsub::GossipsubConfig::default();
            
            let message_authenticity = MessageAuthenticity::Signed(local_key.clone());
            // let message_authenticity = MessageAuthenticity::Anonymous;
            Some(
                gossipsub::Gossipsub::new(message_authenticity, gossipsub_config)
                    .map_err(|e| anyhow::anyhow!("{}", e))?,
            )
        } else {
            None
        }
        .into();
        
        Ok(NodeBehaviour {
            ping: Ping::default(),
            keep: libp2p::swarm::keep_alive::Behaviour::default(),
            identify,
            bitswap,
            mdns,
            chat:chat.into(),
            kad,
            autonat,
            relay,
            dcutr: dcutr.into(),
            relay_client: relay_client.into(),
            gossipsub,
            peer_manager,
        })
    }

    pub fn notify_new_blocks(&self, blocks: Vec<Block>) {
        if let Some(bs) = self.bitswap.as_ref() {
            let client = bs.client().clone();
            tokio::task::spawn(async move {
                if let Err(err) = client.notify_new_blocks(&blocks).await {
                    tracing::info!("failed to notify bitswap about blocks: {:?}", err);
                }
            });
        }
    }

    pub fn kad_bootstrap(&mut self) -> Result<()> {
        if let Some(kad) = self.kad.as_mut() {
            kad.bootstrap()?;
        }
        Ok(())
    }
}
