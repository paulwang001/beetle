#[macro_use]
extern crate lazy_static;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::{
    aead::{KeyInit, OsRng},
    Aes256Gcm, // Or `Aes128Gcm`
};
use anyhow::Context;
use libp2p::gossipsub::GossipsubMessage;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use libp2p::{gossipsub::TopicHash, identity::Keypair};
use luffa_node::{
    load_identity, DiskStorage, GossipsubEvent, Keychain, NetworkEvent, Node, ENV_PREFIX,
};
use luffa_rpc_client::P2pClient;
use luffa_rpc_types::p2p::P2pAddr;
use luffa_rpc_types::store::StoreAddr;
use luffa_rpc_types::{message_from, Addr};
use luffa_rpc_types::{AppStatus, ContactsEvent, ContactsToken, Event, Message};
use luffa_store::{rpc, Config as StoreConfig, Store};
use luffa_util::{luffa_config_path, make_config};
use sled::Db;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

mod config;

use crate::config::Config;

const TOPIC_STATUS: &str = "luffa_status";
// const TOPIC_CONTACTS: &str = "luffa_contacts";
const TOPIC_CHAT: &str = "luffa_chat";
// const TOPIC_CHAT_PRIVATE: &str = "luffa_chat_private";
// const TOPIC_CONTACTS_SCAN: &str = "luffa_contacts_scan_answer";
const CONFIG_FILE_NAME: &str = "luffa.config.toml";
const KVDB_CONTACTS_FILE: &str = "luffa.contacts.db";
const KVDB_CONTACTS_TREE: &str = "luffa_contacts";
const TOPIC_RELAY: &str = "luffa_relay";

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = create_runtime();
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Contancts parse error")]
    CodeParser,
    #[error("Send message failed")]
    SendFailed,
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

pub trait Callback: Send + Sync + Debug {
    // fn on_message(&self, msg: Message) -> Result<Option<String>, CallbackError>;
    fn on_message(&self, msg: Vec<u8>);
}

#[derive(Debug, Clone)]
pub struct Client {
    sender: Arc<RwLock<Option<tokio::sync::mpsc::Sender<(u64, Vec<u8>, u64)>>>>,
    key: Arc<RwLock<Option<Keypair>>>,
    db: Arc<Db>,
    client: Arc<RwLock<Option<Arc<P2pClient>>>>,
}

impl Client {
    pub fn new() -> Self {
        let path = luffa_util::luffa_data_path(KVDB_CONTACTS_FILE).unwrap();
        let db = Arc::new(sled::open(path).unwrap());
        Client {
            key: Arc::new(RwLock::new(None)),
            sender: Arc::new(RwLock::new(None)),
            db,
            client:Arc::new(RwLock::new(None)),
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

        let secret_key = key.to_vec();

        let token = RUNTIME.block_on(async {
            let key = self.key.read().await;
            let token = key.as_ref().map(|key| {
                ContactsToken::new(
                    key,
                    comment,
                    secret_key,
                    luffa_rpc_types::ContactsTypes::Private,
                )
                .unwrap()
            });
            token.unwrap()
        });

        let secret_key = token.secret_key.clone();
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
        let msg = serde_cbor::to_vec(&msg).unwrap();
        if is_offer {
            let mut digest = crc64fast::Digest::new();
            digest.write(&msg);
            let offer_id = digest.sum64();
            self.save_contacts_offer(offer_id, secret_key);
        }
        let code = multibase::encode(multibase::Base::Base64, &msg);
        code
        // let nonce = Nonce::from_slice(&nonce); // 96-bits; unique per message
        // let ciphertext = cipher.encrypt(nonce, b"plaintext message".as_ref())?;
        // let plaintext = cipher.decrypt(nonce, ciphertext.as_ref())?;
        // assert_eq!(&plaintext, b"plaintext message");
    }

    pub fn parse_contacts_code(&self, code: String) -> std::result::Result<String, ClientError> {
        match multibase::decode(&code) {
            Ok((_, msg)) => {
                let msg: Message = serde_cbor::from_slice(&msg).map_err(|_e| ClientError::CodeParser)?;
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
                                    Err(ClientError::CodeParser)
                                }
                            }
                            _ => Err(ClientError::CodeParser),
                        }
                    }
                    _ => Err(ClientError::CodeParser),
                }
            }
            Err(e) => Err(ClientError::CodeParser),
        }
    }

    /// scan qr code to get offer
    /// save to constacts,
    /// and then answer to did that who owner the QR
    pub fn answer_contacts_code(
        &self,
        code: String,
        comment: Option<String>,
    ) -> std::result::Result<(), ClientError> {
        match multibase::decode(&code) {
            Ok((_, msg)) => {
                let msg: Message = serde_cbor::from_slice(&msg).map_err(|e| ClientError::CodeParser)?;
                match msg {
                    Message::ContactsExchange { exchange } => {
                        match exchange {
                            ContactsEvent::Offer { token } => {
                                let ContactsToken {
                                    public_key,
                                    create_at,
                                    sign,
                                    secret_key,
                                    contacts_type,
                                    ..
                                } = token;
                                let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
                                let mut msg = vec![];
                                msg.extend_from_slice(&secret_key);
                                // buf.extend_from_slice(&nonce);
                                msg.extend_from_slice(&create_at.to_be_bytes());
                                if pk.verify(&msg, &sign) {
                                    let code =
                                        self.gen_offer_anwser(comment, Some(secret_key.clone()));
                                    let (_, data) = multibase::decode(&code).unwrap();
                                    let msg: Message = serde_cbor::from_slice(&data)
                                        .map_err(|e| ClientError::CodeParser)?;
                                    let to = PeerId::from_public_key(&pk);
                                    let mut digest = crc64fast::Digest::new();
                                    digest.write(&to.to_bytes());
                                    let to = digest.sum64();
                                    // Save answer to DB
                                    self.save_contacts(
                                        to,
                                        secret_key,
                                        public_key,
                                        sign,
                                        contacts_type as u8,
                                    );
                                    let from_id = self.get_local_id().unwrap_or_default();
                                    self.send_to(to, msg, from_id);
                                    Ok(())
                                } else {
                                    Err(ClientError::CodeParser)
                                }
                            }
                            _ => Err(ClientError::CodeParser),
                        }
                    }
                    _ => Err(ClientError::CodeParser),
                }
            }
            Err(_e) => Err(ClientError::CodeParser),
        }
    }
    fn save_contacts(
        &self,
        to: u64,
        secret_key: Vec<u8>,
        public_key: Vec<u8>,
        sign: Vec<u8>,
        c_type: u8,
    ) {
        let tree = self.db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let s_key = format!("S-KEY-{c_type}-{}", to);
        let p_key = format!("P-KEY-{c_type}-{}", to);
        let sig_key = format!("SIG-{c_type}-{}", to);
        tree.insert(s_key, secret_key).unwrap();
        tree.insert(p_key, public_key).unwrap();
        tree.insert(sig_key, sign).unwrap();
        tree.flush().unwrap();
    }

    fn save_contacts_offer(&self, offer_id: u64, secret_key: Vec<u8>) {
        let tree = self.db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let offer_key = format!("OFFER-{}", offer_id);
        tree.insert(offer_key, secret_key).unwrap();
        tree.flush().unwrap();
    }
    fn get_offer_by_offer_id(db: Arc<Db>, offer_id: u64) -> Option<Vec<u8>> {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let offer_key = format!("OFFER-{}", offer_id);
        match tree.get(offer_key) {
            Ok(Some(data)) => Some(data.to_vec()),
            _ => None,
        }
    }
    fn send_to(&self, to: u64, msg: Message, from_id: u64) {
        RUNTIME.block_on(async move {
            let tx = self.sender.read().await;
            let tx = tx.as_ref().unwrap();
            let msg = serde_cbor::to_vec(&msg).unwrap();
            tx.send((to, msg, from_id)).await.unwrap();
        });
    }
    pub fn send_msg(&self, to: u64, msg: Vec<u8>) -> std::result::Result<(), ClientError> {
        let from_id = self.get_local_id().unwrap_or_default();
        match message_from(msg) {
            Some(msg) => {
                self.send_to(to, msg, from_id);
                Ok(())
            }
            None => Err(ClientError::SendFailed),
        }
    }

    pub fn get_local_id(&self) -> Option<u64> {
        RUNTIME.block_on(async {
            let key = self.key.read().await;
            let key = key
                .as_ref()
                .map(|p| PeerId::from_public_key(&p.public()).to_bytes());
            key.map(|m| {
                let mut digest = crc64fast::Digest::new();
                digest.write(&m);
                digest.sum64()
            })
        })
    }

    pub fn get_peer_id(&self) -> Option<String> {
        RUNTIME.block_on(async {
            let key = self.key.read().await;
            let key = key
                .as_ref()
                .map(|p| PeerId::from_public_key(&p.public()).to_bytes());
            key.map(|m| bs58::encode(m).into_string())
        })
    }

    pub fn relay_list(&self) -> Vec<String> {
        let client = self.client.clone();
        
        RUNTIME.block_on(async {
            let c = client.read().await;
            if let Some(cc) = c.as_ref() {
                match cc
                    .gossipsub_mesh_peers(TopicHash::from_raw(TOPIC_RELAY))
                    .await
                {
                    Ok(peers) => {
                        info!("peers:{peers:?}"); 
                        return peers
                            .into_iter()
                            .map(|p| bs58::encode(p.to_bytes()).into_string())
                            .collect::<Vec<_>>();
                    
                    }
                    Err(e)=>{
                       eprintln!("{e:?}");
                       tracing::warn!("{e:?}"); 
                    }
                }
            }
            warn!("client is None");
            vec![]
        })
    }

    pub fn connect(&self, peer_id: String) -> bool {
        let (_, data) = multibase::decode(&peer_id).unwrap();
        let peer_id = PeerId::from_bytes(&data).unwrap();
        let client = self.client.clone();
        RUNTIME.block_on(async {
            let c = client.read().await;
            if let Some(cc) = c.as_ref() {
                match cc.connect(peer_id, vec![]).await {
                    Ok(_)=> true,
                    _=>false
                }
            }
            else{
                false
            }
        })
    }
    
    pub fn stop(&self) {
        self.send_to(
            u64::MAX,
            Message::StatusSync {
                did: 0,
                relay_id: 0,
                status: AppStatus::Bye,
            },
            u64::MAX,
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
        // let client_addr = config.rpc_client.p2p_addr.clone();
        let client = self.client.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let db = self.db.clone();
        RUNTIME.block_on(async {
            let mut sender = self.sender.write().await;
            *sender = Some(tx);
            // if let Some(addr) = client_addr {
            //     let client = P2pClient::new(addr).await.unwrap();
            //     let mut clt = self.client.write().await;
            //     *clt = Some(client);
            // }
            tokio::spawn(async move {
                Self::run(db, config, kc, cb, client,rx,).await;
                eprintln!("run exit!....");
            });
        });
    }
    async fn run(
        db: Arc<Db>,
        mut config: Config,
        kc: Keychain<DiskStorage>,
        cb: Box<dyn Callback>,
        client_lock: Arc<RwLock<Option<Arc<P2pClient>>>>,
        mut receiver: tokio::sync::mpsc::Receiver<(u64, Vec<u8>, u64)>,
    ) {
        // let (tx, rx) = tokio::sync::mpsc::channel::<NetworkEvent>(4096);
        config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);

        let metrics_config = config.metrics.clone();

        let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
            .await
            .expect("failed to initialize metrics");

        let (peer, store_rpc, p2p_rpc, mut events) = {
            let store_recv = Addr::new_mem();
            let store_sender = store_recv.clone();
            let p2p_recv = match config.p2p.rpc_client.p2p_addr.as_ref() {
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


        let mut digest = crc64fast::Digest::new();
        digest.write(&peer.to_bytes());
        let my_id = digest.sum64();
        let addr = match config.rpc_client.p2p_addr.as_ref() {
            Some(a) => a.clone(),
            None => Addr::new_mem(),
        };
        let client = Arc::new(P2pClient::new(addr).await.unwrap());

        let client_t = client.clone();
        tokio::spawn(async move {
            let mut has_err = false;
            loop {
                if let Err(e) = client_t
                    .gossipsub_subscribe(TopicHash::from_raw(format!("{}_{}", TOPIC_CHAT, my_id)))
                    .await
                {
                    error!("{e:?}");
                    has_err = true;
                }
                let topics = vec![TOPIC_STATUS];
                for t in topics.into_iter() {
                    if let Err(e) = client_t
                        .gossipsub_subscribe(TopicHash::from_raw(t))
                        .await
                    {
                        error!("{e:?}");
                        has_err = true;
                        break;;
                    }
                }

                if !has_err {
                    info!("subscribed all as client,status sync");
                    let msg = luffa_rpc_types::Message::StatusSync {
                        did: 0,
                        status: AppStatus::Active,
                        relay_id:my_id
                    };
                    let key:Option<Vec<u8>> = None;
                    let event = Event::new(0, msg, key, my_id);
                    let data = event.encode().unwrap();
                    if let Err(e) = client_t.gossipsub_publish(TopicHash::from_raw(format!("{}", TOPIC_STATUS)), bytes::Bytes::from(data)).await {
                        warn!("{e:?}");
                    }
                    // break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
           
        });

        let client_t = client.clone();
        {
            let mut lock = client_lock.write().await;
            *lock = Some(client_t);
        }
        let client_t = client.clone();
        let db_t = db.clone();
        let process = tokio::spawn(async move {
            while let Some(evt) = events.recv().await {
                match evt {
                    NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {
                        // TODO: a group member or my friend online?
                        info!("Subscribed> peer_id: {peer_id:?} topic:{topic}");
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Message { message, from, id }) => {
                        let GossipsubMessage { data, .. } = message;
                        if let Ok(im) = Event::decode(data) {
                            let Event {
                                msg,
                                nonce,
                                from_id,
                                ..
                            } = im;
                            info!("Gossipsub> peer_id: {from:?} msg:{}",msg.len());
                            // TODO check did status
                            if nonce.is_none() {
                                if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                    bytes::Bytes::from(msg),
                                    None,
                                    nonce,
                                ) {
                                    // TODO: did is me or I'm a member any local group
                                    info!("Gossipsub> on_message peer_id: {from:?} msg:{:?}",msg);
                                    let data = serde_cbor::to_vec(&msg).unwrap();
                                    cb.on_message(data);
                                    // todo!()
                                }
                            } else {
                                match Self::get_aes_key_from_contacts(db_t.clone(), from_id) {
                                    Some(key) => {
                                        if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                            bytes::Bytes::from(msg),
                                            Some(key),
                                            nonce,
                                        ) {
                                            // TODO: did is me or I'm a member any local group
                                            let data = serde_cbor::to_vec(&msg).unwrap();
                                            cb.on_message(data);
                                        }
                                    }
                                    None => {
                                        let mut digest = crc64fast::Digest::new();
                                        digest.write(&msg);
                                        let offer_id = digest.sum64();
                                        if let Some(key) =
                                            Self::get_offer_by_offer_id(db_t.clone(), offer_id)
                                        {
                                            if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                                bytes::Bytes::from(msg),
                                                Some(key),
                                                nonce,
                                            ) {
                                                // TODO: did is me or I'm a member any local group
                                                let data = serde_cbor::to_vec(&msg).unwrap();
                                                cb.on_message(data);
                                            }
                                        } else {
                                            tracing::warn!("invalid msg");
                                        }
                                    }
                                }
                                // todo!()
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
                        let msg = luffa_rpc_types::Message::StatusSync {
                            did: u_id,
                            relay_id: my_id,
                            status: AppStatus::Connected,
                        };
                        let event = luffa_rpc_types::Event::new(0, msg, None, u_id);
                        let event = event.encode().unwrap();
                        if let Err(e) = 
                        client_t
                            .gossipsub_publish(
                                TopicHash::from_raw(TOPIC_STATUS),
                                bytes::Bytes::from(event),
                            )
                            .await
                        {
                            error!("{e:?}");
                        }
                    }
                    NetworkEvent::PeerDisconnected(peer_id) => {
                        tracing::info!("---------PeerDisconnected-----------{:?}", peer_id);
                        let mut digest = crc64fast::Digest::new();
                        digest.write(&peer_id.to_bytes());
                        let u_id = digest.sum64();
                        let msg = luffa_rpc_types::Message::StatusSync {
                            did: u_id,
                            relay_id: my_id,
                            status: AppStatus::Disconnected,
                        };
                        let event = luffa_rpc_types::Event::new(0, msg, None, u_id);
                        let event = event.encode().unwrap();
                        if let Err(e) = client_t
                            .gossipsub_publish(
                                TopicHash::from_raw(TOPIC_STATUS),
                                bytes::Bytes::from(event),
                            )
                            .await
                        {
                            error!("{e:?}");
                        }
                    }
                    NetworkEvent::CancelLookupQuery(peer_id) => {
                        tracing::info!("---------CancelLookupQuery-----------{:?}", peer_id);
                    }
                }
            }
        });

        while let Some((to, msg, msg_id)) = receiver.recv().await {
            if to == u64::MAX {
                break;
            }
            let msg = serde_cbor::from_slice::<Message>(&msg).unwrap();
            let evt = if msg.need_encrypt() {
                match Self::get_aes_key_from_contacts(db.clone(), to) {
                    Some(key) => Some(Event::new(to, msg, Some(key), msg_id)),
                    None => Some(Event::new(to, msg, None, msg_id)),
                }
            } else {
                Some(Event::new(to, msg, None, msg_id))
            };
            match evt {
                Some(e) => {
                    let data = e.encode().unwrap();
                    let topic_hash = if to == 0 {
                        TopicHash::from_raw(TOPIC_STATUS)
                    } else {
                        TopicHash::from_raw(format!("{}_{}", TOPIC_CHAT, to))
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

    fn get_aes_key_from_contacts(db: Arc<Db>, did: u64) -> Option<Vec<u8>> {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let s_key = format!("S-KEY-{}", did);
        match tree.get(&s_key) {
            Ok(Some(d)) => Some(d.to_vec()),
            _ => None,
        }
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
    // let marker = config.path.join("CURRENT");

    info!("Opening store at {}", config.path.display());
    let store =
    Store::open(config)
        .await
        .context("failed to open existing store")?;
    // let store = if marker.exists() {
    //     info!("Opening store at {}", config.path.display());
    //     Store::open(config)
    //         .await
    //         .context("failed to open existing store")?
    // } else {
    //     info!("Creating store at {}", config.path.display());
    //     Store::create(config)
    //         .await
    //         .context("failed to create new store")?
    // };

    let rpc_task = tokio::spawn(async move { rpc::new(rpc_addr, store).await.unwrap() });

    Ok(rpc_task)
}

include!(concat!(env!("OUT_DIR"), "/luffa_sdk.uniffi.rs"));
