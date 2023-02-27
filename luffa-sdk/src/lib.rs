#[macro_use]
extern crate lazy_static;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm,
    Nonce, // Or `Aes128Gcm`
};
use anyhow::{Context, Result};
use futures::AsyncWriteExt;
use libp2p::gossipsub::GossipsubMessage;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use libp2p::{gossipsub::TopicHash, identity::Keypair};
use luffa_node::{
    load_identity, DiskStorage, GossipsubEvent, Keychain, NetworkEvent, Node, ENV_PREFIX,
};
use luffa_rpc_client::P2pClient;
use luffa_rpc_types::im::{AppStatus, ContactsEvent, ContactsToken, Event, Message};
use luffa_rpc_types::p2p::P2pAddr;
use luffa_rpc_types::store::StoreAddr;
use luffa_rpc_types::Addr;
use luffa_store::{rpc, Config as StoreConfig, Store};
use luffa_util::{luffa_config_path, make_config};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info};

mod config;

use crate::config::Config;

const TOPIC_STATUS: &str = "luffa_status";
const TOPIC_CONTACTS: &str = "luffa_contacts";
const TOPIC_CHAT_GROUP: &str = "luffa_chat_group";
const TOPIC_CHAT_PRIVATE: &str = "luffa_chat_private";
const TOPIC_CONTACTS_SCAN: &str = "luffa_contacts_scan_answer";
const CONFIG_FILE_NAME: &str = "luffa.config.toml";

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = create_runtime();
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

pub trait Callback: Send + Sync + Debug {
    // fn on_message(&self, msg: Message) -> Result<Option<String>, CallbackError>;
    fn on_message(&self, msg: Message);
}

pub struct Client {
    // queue: VecDeque<Message>,
    sender: RefCell<Option<tokio::sync::mpsc::Sender<(u64, Message)>>>,
    key: Arc<RwLock<Option<Keypair>>>,
}

impl Client {
    pub fn new() -> Self {
        Client {
            key: Arc::new(RwLock::new(None)),
            sender: RefCell::new(None),
        }
    }
    /// show code
    pub fn show_code(&self, comment: Option<String>) -> String {
        self.gen_offer_anwser(comment, None)
    }
    /// gen a offer
    pub fn gen_offer_anwser(&self, comment: Option<String>, secret_key: Option<Vec<u8>>) -> String {
        let (key, is_offer) = match secret_key {
            Some(secret) => (GenericArray::clone_from_slice(&secret[..]), false),
            None => (Aes256Gcm::generate_key(&mut OsRng), true),
        };

        let create_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let secret_key = key.to_vec();
        let mut buf = vec![];

        buf.extend_from_slice(&secret_key);
        // buf.extend_from_slice(&nonce);
        buf.extend_from_slice(&create_at.to_be_bytes());
        let token = RUNTIME.block_on(async {
            let key = self.key.read().await;
            let token = key.as_ref().map(|key| {
                ContactsToken::new(
                    key,
                    comment,
                    secret_key,
                    luffa_rpc_types::im::ContactsTypes::Private,
                )
                .unwrap()
            });
            token.unwrap()
        });

        let msg = if is_offer {
            Message::ContactsExchange {
                exchange: ContactsEvent::Offer { token },
            }
        } else {
            Message::ContactsExchange {
                exchange: ContactsEvent::Answer { token },
            }
        };
        // let msg = msg.encrypt(None).unwrap();
        let msg = bson::to_vec(&msg).unwrap();
        let code = multibase::encode(multibase::Base::Base64, &msg);
        code
        // let nonce = Nonce::from_slice(&nonce); // 96-bits; unique per message
        // let ciphertext = cipher.encrypt(nonce, b"plaintext message".as_ref())?;
        // let plaintext = cipher.decrypt(nonce, ciphertext.as_ref())?;
        // assert_eq!(&plaintext, b"plaintext message");
    }

    pub fn parse_contacts_code(&self, code: String) -> Result<String> {
        match multibase::decode(&code) {
            Ok((_, msg)) => {
                let msg: Message = bson::from_slice(&msg).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                match msg {
                    Message::ContactsExchange { exchange } => {
                        match exchange {
                            ContactsEvent::Offer { token } => {
                                let ContactsToken {
                                    public_key,
                                    create_at,
                                    sign,
                                    secret_key,
                                    comment,
                                    ..
                                } = token;
                                let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
                                let mut msg = vec![];
                                msg.extend_from_slice(&secret_key);
                                // buf.extend_from_slice(&nonce);
                                msg.extend_from_slice(&create_at.to_be_bytes());
                                if pk.verify(&msg, &sign) {
                                    Ok(comment.unwrap_or_default())
                                } else {
                                    Err(anyhow::anyhow!(""))
                                }
                            }
                            _ => Err(anyhow::anyhow!("")),
                        }
                    }
                    _ => Err(anyhow::anyhow!("")),
                }
            }
            Err(e) => Err(anyhow::anyhow!("{e:?}")),
        }
    }
    pub fn answer_contacts_code(&self, code: String, comment: Option<String>) -> Result<()> {
        match multibase::decode(&code) {
            Ok((_, msg)) => {
                let msg: Message = bson::from_slice(&msg).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                match msg {
                    Message::ContactsExchange { exchange } => {
                        match exchange {
                            ContactsEvent::Offer { token } => {
                                let ContactsToken {
                                    public_key,
                                    create_at,
                                    sign,
                                    secret_key,
                                    ..
                                } = token;
                                let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
                                let mut msg = vec![];
                                msg.extend_from_slice(&secret_key);
                                // buf.extend_from_slice(&nonce);
                                msg.extend_from_slice(&create_at.to_be_bytes());
                                if pk.verify(&msg, &sign) {
                                    let code = self.gen_offer_anwser(comment, Some(secret_key));
                                    let (_, data) = multibase::decode(&code).unwrap();
                                    let msg: Message = bson::from_slice(&data)
                                        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                                    let to = PeerId::from_public_key(&pk);
                                    let mut digest = crc64fast::Digest::new();
                                    digest.write(&to.to_bytes());
                                    let to = digest.sum64();
                                    // TODO save answer to DB
                                    self.send_to(to, msg);
                                    Ok(())
                                } else {
                                    Err(anyhow::anyhow!(""))
                                }
                            }
                            _ => Err(anyhow::anyhow!("")),
                        }
                    }
                    _ => Err(anyhow::anyhow!("")),
                }
            }
            Err(e) => Err(anyhow::anyhow!("{e:?}")),
        }
    }

    pub fn send_to(&self, to: u64, msg: Message) {
        let tx = self.sender.borrow();
        let tx = tx.clone();
        let tx = tx.unwrap();
        RUNTIME.block_on(async move {
            tx.send((to, msg)).await.unwrap();
        });
    }
    pub fn stop(&self) {
        self.send_to(
            u64::MAX,
            Message::StatusSync {
                did: 0,
                status: AppStatus::Bye,
            },
        );
    }

    pub fn start(&self, cfg_path: Option<String>, cb: Box<dyn Callback>) {
        #[cfg(unix)]
        {
            match luffa_util::increase_fd_limit() {
                Ok(soft) => tracing::debug!("NOFILE limit: soft = {}", soft),
                Err(err) => tracing::error!("Error increasing NOFILE limit: {}", err),
            }
        }

        let args: HashMap<String, String> = HashMap::new();
        let dft_path = luffa_config_path(CONFIG_FILE_NAME).unwrap();
        let cfg_path = cfg_path.map(|p| PathBuf::from(p));
        let cfg_path = cfg_path.unwrap_or(dft_path);
        let sources = [Some(cfg_path.as_path())];
        let config = make_config(
            // default
            Config::default(),
            // potential config files
            &sources,
            // env var prefix for this config
            ENV_PREFIX,
            // map of present command line arguments
            args,
        )
        .unwrap();

        let kc = RUNTIME.block_on(async {
            let mut kc = Keychain::<DiskStorage>::new(config.p2p.clone().key_store_path.clone())
                .await
                .unwrap();
            let key = load_identity(&mut kc).await.unwrap();

            let mut m_key = self.key.write().await;
            *m_key = Some(key);
            kc
        });
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let mut sender = self.sender.borrow_mut();
        *sender = Some(tx);

        RUNTIME.block_on(async {
            tokio::spawn(async move {
                Self::run(config, kc, cb, rx).await;
            });
        });
    }
    async fn run(
        mut config: Config,
        kc: Keychain<DiskStorage>,
        cb: Box<dyn Callback>,
        mut receiver: tokio::sync::mpsc::Receiver<(u64, Message)>,
    ) {
        // let (tx, rx) = tokio::sync::mpsc::channel::<NetworkEvent>(4096);
        let (peer, store_rpc, p2p_rpc, mut events) = {
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
            let store_rpc = start_store(store_recv, config.store.clone()).await.unwrap();

            let (peer_id, p2p_rpc, events) =
                start_node(p2p_recv, config.p2p.clone(), kc).await.unwrap();
            (peer_id, store_rpc, p2p_rpc, events)
        };

        config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);
        info!("{config:#?}");

        let metrics_config = config.metrics.clone();

        let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
            .await
            .expect("failed to initialize metrics");

        let mut digest = crc64fast::Digest::new();
        digest.write(&peer.to_bytes());
        let my_id = digest.sum64();

        let client = P2pClient::new(P2pAddr::new_mem()).await.unwrap();
        client
            .gossipsub_subscribe(TopicHash::from_raw(format!(
                "{}-{}",
                TOPIC_CONTACTS_SCAN, my_id
            )))
            .await
            .unwrap();
        let topics = vec![TOPIC_STATUS];
        for t in topics.into_iter() {
            client
                .gossipsub_subscribe(TopicHash::from_raw(t))
                .await
                .unwrap();
        }
        let msg = luffa_rpc_types::im::Message::StatusSync {
            did: my_id,
            status: AppStatus::Active,
        };
        let event = luffa_rpc_types::im::Event::new::<Vec<u8>>(my_id, msg, None);
        let event = event.encode().unwrap();
        client
            .gossipsub_publish(TopicHash::from_raw(TOPIC_STATUS), bytes::Bytes::from(event))
            .await
            .unwrap();
        let client_t = client.clone();
        let process = tokio::spawn(async move {
            while let Some(evt) = events.recv().await {
                match evt {
                    NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {
                        // TODO: a group member or my friend online?
                    }
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
                                    // TODO: did is me or I'm a member any local group
                                    cb.on_message(msg);
                                    todo!()
                                }
                            } else {
                                todo!()
                            }
                        }
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Unsubscribed { peer_id, topic }) => {

                        // TODO: a group member or my friend offline?
                    }
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
                        client_t
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
                        client_t
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

        while let Some((to, msg)) = receiver.recv().await {
            if to == u64::MAX {
                break;
            }
            let evt = if msg.need_encrypt() {
                match Self::get_aes_key_from_contacts(to) {
                    Ok(Some(key)) => Some(Event::new::<Vec<u8>>(to, msg, Some(key))),
                    Ok(None) => Some(Event::new::<Vec<u8>>(to, msg, None)),
                    _ => None,
                }
            } else {
                Some(Event::new::<Vec<u8>>(to, msg, None))
            };
            match evt {
                Some(e) => {
                    let data = e.encode().unwrap();
                    let topic_hash = if to == 0 {
                        TopicHash::from_raw(TOPIC_STATUS)
                    } else {
                        TopicHash::from_raw(format!("{}-{}", TOPIC_CONTACTS_SCAN, to))
                    };
                    client
                        .gossipsub_publish(topic_hash, bytes::Bytes::from(data))
                        .await
                        .unwrap();
                }
                None => {}
            }
        }
        // luffa_util::block_until_sigint().await;
        // ctl.await.unwrap();
        process.abort();
        store_rpc.abort();
        p2p_rpc.abort();

        metrics_handle.shutdown();
    }

    fn get_aes_key_from_contacts(did: u64) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}
/// Starts a new p2p node, using the given mem rpc channel.
pub async fn start_node(
    rpc_addr: P2pAddr,
    config: luffa_node::config::Config,
    keychain: Keychain<DiskStorage>,
) -> anyhow::Result<(PeerId, JoinHandle<()>, Receiver<NetworkEvent>)> {
    let mut p2p = Node::new(config, rpc_addr, keychain).await?;
    let events = p2p.network_events();
    let local_id = p2p.local_peer_id().clone();
    // Start services
    let p2p_task = task::spawn(async move {
        if let Err(err) = p2p.run().await {
            error!("{:?}", err);
        }
    });
    Ok((local_id, p2p_task, events))
}

/// Starts a new store, using the given mem rpc channel.
pub async fn start_store(
    rpc_addr: StoreAddr,
    config: StoreConfig,
) -> anyhow::Result<JoinHandle<()>> {
    // This is the file RocksDB itself is looking for to determine if the database already
    // exists or not.  Just knowing the directory exists does not mean the database is
    // created.
    let marker = config.path.join("CURRENT");

    let store = if marker.exists() {
        info!("Opening store at {}", config.path.display());
        Store::open(config)
            .await
            .context("failed to open existing store")?
    } else {
        info!("Creating store at {}", config.path.display());
        Store::create(config)
            .await
            .context("failed to create new store")?
    };

    let rpc_task = tokio::spawn(async move { rpc::new(rpc_addr, store).await.unwrap() });

    Ok(rpc_task)
}
