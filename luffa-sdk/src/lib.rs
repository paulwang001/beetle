#[macro_use]
extern crate lazy_static;

use aes_gcm::{
    aead::{KeyInit, OsRng},
    Aes256Gcm, // Or `Aes128Gcm`
};
use anyhow::Context;
use api::P2pClient;
use futures::StreamExt;
use libp2p::identity::PublicKey;
use libp2p::{gossipsub::TopicHash, identity::Keypair};
use libp2p::{multihash, PeerId};
use luffa_node::{
    DiskStorage, GossipsubEvent, KeyFilter, Keychain, NetworkEvent, Node, ENV_PREFIX,
};
use luffa_rpc_types::{
    message_from, message_to, ChatContent, Contacts, ContactsTypes, ContentData, FeedbackStatus,
};
use luffa_rpc_types::{AppStatus, ContactsEvent, ContactsToken, Event, Message};
use luffa_store::{Config as StoreConfig, Store};
use luffa_util::{luffa_config_path, make_config};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use simsearch::{SearchOptions, SimSearch};
use sled::Db;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, SimpleTokenizer, Stemmer, TextAnalyzer};

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::avatar_nickname::avatar::generate_avatar;
use crate::avatar_nickname::nickname::generate_nickname;
use anyhow::Result;
use chrono::Utc;
use image::EncodableLayout;
use tantivy::collector::TopDocs;
use tantivy::directory::{ManagedDirectory, MmapDirectory};
use tantivy::query::QueryParser;
use tantivy::{Index, TantivyError};
use tantivy::{doc, DocAddress, Score};
use tantivy::{schema::*, IndexWriter};
use tokio::sync::oneshot::{Sender as ShotSender};

mod api;
mod config;
pub mod avatar_nickname;
mod sled_db;

use crate::config::Config;
use crate::sled_db::contacts::{ContactsDb, KVDB_CONTACTS_TREE};
use crate::sled_db::group_members::GroupMembersDb;
use crate::sled_db::local_config::{KVDB_CONTACTS_FILE, LUFFA_CONTENT};
use crate::sled_db::mnemonic::Mnemonic;
use crate::sled_db::session::SessionDb;
use crate::sled_db::SledDb;

// const TOPIC_CONTACTS: &str = "luffa_contacts";
// const TOPIC_CHAT: &str = "luffa_chat";
// const TOPIC_CHAT_PRIVATE: &str = "luffa_chat_private";
// const TOPIC_CONTACTS_SCAN: &str = "luffa_contacts_scan_answer";
const CONFIG_FILE_NAME: &str = "luffa.config.toml";
// const KVDB_CONTACTS_FILE: &str = "contacts";
// const KVDB_CONTACTS_TREE: &str = "luffa_contacts";
// const KVDB_CHAT_SESSION_TREE: &str = "luffa_sessions";
// const LUFFA_CONTENT: &str = "index_data";

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = create_runtime();
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatSession {
    pub did: u64,
    pub session_type: u8,
    pub last_time: u64,
    pub tag: String,
    pub read_crc: u64,
    pub reach_crc: Vec<u64>,
    pub last_msg: String,
    pub enabled_silent: bool,
    pub last_msg_status: u8,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct ContactsView {
    pub did: u64,
    pub tag: String,
    pub c_type: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventMeta {
    pub from_id: u64,
    pub to_id: u64,
    pub from_tag: String,
    pub to_tag: String,
    pub event_time: u64,
    pub status: u32,
    pub msg: Vec<u8>,
}

pub type ClientResult<T> = anyhow::Result<T, ClientError>;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OfferStatus {
    Offer,
    Answer,
    Reject,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError{
    #[error("Contancts parse error")]
    CodeParser,
    #[error("Send message failed")]
    SendFailed,
    #[error("Client start failed")]
    StartFailed,
    #[error("Search error")]
    SearchError,
    #[error(transparent)]
    SledError(#[from] sled::Error),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    TantivyError(#[from] tantivy::TantivyError),
    #[error(transparent)]
    SerdeCborError(#[from] serde_cbor::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
    #[error(transparent)]
    MultibaseError(#[from] multibase::Error),
    #[error(transparent)]
    MultihashError(#[from] multihash::Error),
    #[error(transparent)]
    Bs58DecodeError(#[from] bs58::decode::Error),
    #[error(transparent)]
    DecodingError(#[from] libp2p::identity::error::DecodingError),
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::error::Error),
    #[error("{0}")]
    CustomError(String),
}

#[derive(Default)]
struct CloneableAtomicBool(AtomicBool);

impl Clone for CloneableAtomicBool {
    fn clone(&self) -> Self {
        Self(AtomicBool::new(self.0.load(Ordering::SeqCst)))
    }
}

impl Deref for CloneableAtomicBool {
    type Target = AtomicBool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

pub trait Callback: Send + Sync + Debug {
    fn on_message(&self, crc: u64, from_id: u64, to: u64,event_time:u64, msg: Vec<u8>);
}

pub fn public_key_to_id(public_key: Vec<u8>) -> u64 {
    let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
    let peer = PeerId::from_public_key(&pk);
    let mut digest = crc64fast::Digest::new();
    digest.write(&peer.to_bytes());
    let to = digest.sum64();
    to
}

pub fn bs58_decode(data: &str) -> ClientResult<u64> {
    let mut to = [0u8; 8];
    to.clone_from_slice(&bs58::decode(data).into_vec()?);
    Ok(u64::from_be_bytes(to))
}

fn content_index(idx_path: &Path) -> (Index, Schema) {
    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("ngram")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
    let mut schema_builder = Schema::builder();

    schema_builder.add_u64_field("crc", INDEXED | STORED);
    schema_builder.add_u64_field("from_id", INDEXED | STORED);
    schema_builder.add_u64_field("to_id", INDEXED | STORED);
    schema_builder.add_u64_field("event_time", INDEXED | STORED);
    schema_builder.add_text_field("msg_type", STRING | STORED);
    schema_builder.add_text_field("title", text_options.clone());
    schema_builder.add_text_field("body", text_options);
    let schema = schema_builder.build();
    std::fs::create_dir_all(idx_path).unwrap();
    // Indexing documents
    let dir = ManagedDirectory::wrap(Box::new(MmapDirectory::open(idx_path).unwrap())).unwrap();
    let ta = TextAnalyzer::from(SimpleTokenizer)
        .filter(LowerCaser)
        .filter(Stemmer::new(tantivy::tokenizer::Language::English));
    let index = Index::open_or_create(dir, schema.clone()).unwrap();
    index.tokenizers().register("term", ta);
    index.tokenizers()
    .register("ngram", NgramTokenizer::new(1, 3, false));
    (index, schema)
}

#[derive(Clone)]
pub struct Client {
    sender: Option<
        Arc<
            RwLock<
                tokio::sync::mpsc::Sender<(
                    u64,
                    Vec<u8>,
                    u64,
                    ShotSender<Result<u64>>,
                    Option<Vec<u8>>,
                )>,
            >,
        >,
    >,
    key: Option<Arc<RwLock<Keychain<DiskStorage>>>>,
    db: Option<Arc<RwLock<Arc<Db>>>>,
    key_db: Option<Arc<RwLock<Arc<Db>>>>,
    client: Option<Arc<RwLock<P2pClient>>>,
    idx: Option<Arc<RwLock<Index>>>,
    filter: Option<Arc<RwLock<Option<KeyFilter>>>>,
    config: Option<Arc<RwLock<Config>>>,
    store: Option<Arc<RwLock<Arc<luffa_store::Store>>>>,
    is_init: CloneableAtomicBool,
    is_started: CloneableAtomicBool,
}

impl ContactsDb for Client {}

impl SessionDb for Client {}

impl GroupMembersDb for Client {}

impl Mnemonic for Client {}

impl SledDb for Client{}

impl Client {
    pub fn new() -> Self {
        RUNTIME.block_on(async {
            if let Err(e) = luffa_metrics::MetricsHandle::new(luffa_node::metrics::metrics_config_with_compile_time_info(Default::default()))
                .await
            {
                println!("failed to initialize metrics");
            }
        });

        Client {
            key: None,
            filter: None,
            config: None,
            store: None,
            sender: None,
            db:None,
            key_db:None,
            client: None,
            idx: None,
            is_init: Default::default(),
            is_started: Default::default(),
        }
    }

    async fn get_keypair(&self, filter: Option<KeyFilter>) -> Option<Keypair> {
        match filter {
            Some(KeyFilter::Phrase(phrase, password)) => {
                if let Some(chain) = self.key.as_ref() {
                    let mut chain = chain.write();
                    if let Ok(key) = chain.create_ed25519_key_from_seed(&phrase, &password).await {
                        let key: Keypair = key.into();
                        return Some(key);
                    }
                }
            }
            Some(KeyFilter::Name(name)) => {
                if let Some(chain) = self.key.as_ref() {
                    let chain = chain.read();
                    let mut keys = chain.keys();
                    while let Some(key) = keys.next().await {
                        if let Ok(key) = key {
                            if key.name() == name {
                                let key: Keypair = key.into();
                                return Some(key);
                            }
                        }
                    }
                }
            }
            _ => {
                if let Some(chain) = self.key.as_ref() {
                    let chain = chain.read();
                    let mut keys = chain.keys();
                    while let Some(key) = keys.next().await {
                        if let Ok(key) = key {
                            let key: Keypair = key.into();
                            return Some(key);
                        }
                    }
                }
            }
        }
        if let Some(chain) = self.key.as_ref() {
            let mut chain = chain.write();
            match chain.create_ed25519_key_bip39("", true).await {
                Ok((_p, key)) => {
                    let key: Keypair = key.into();
                    Some(key)
                }
                Err(_e) => None,
            }
        }
        else{
            None
        }
    }
    /// show code
    pub fn show_code(&self, domain_name: &str, link_type: &str) -> ClientResult<Option<String>> {
        let res = if let Some(uid) = self.get_did()? {
            let s_key = Aes256Gcm::generate_key(&mut OsRng);
            let secret_key = s_key.to_vec();
            let mut digest = crc64fast::Digest::new();
            digest.write(&secret_key);
            let offer_id = digest.sum64();
            tracing::warn!("gen offer id:{}", offer_id);
            self.save_contacts_offer(offer_id, secret_key.clone());
            if let Some(my_id) = self.get_local_id()? {
                let tag = self.find_contacts_tag(my_id)?.unwrap_or_default();
                Some(format!("{}/{}/{}/{}/{}", domain_name, link_type, uid, bs58::encode(secret_key).into_string(), tag))
            } else {
                None
            }
        } else {
            None
        };
        Ok(res)
    }

    /// gen a new offer code for one did
    pub fn gen_offer_code(&self, did: u64) -> ClientResult<String> {
        let s_key = Aes256Gcm::generate_key(&mut OsRng);
        let secret_key = s_key.to_vec();
        let mut digest = crc64fast::Digest::new();
        digest.write(&secret_key);
        let offer_id = digest.sum64();
        tracing::info!("gen offer id:{}", offer_id);
        self.save_contacts_offer(offer_id, secret_key.clone());
        let tag = self.find_contacts_tag(did)?.unwrap_or_default();
        let c_type = if let Some(c_type) = Self::get_contacts_type(self.db(), did) {
            if c_type == ContactsTypes::Private {
                format!("p")
            } else {
                format!("g")
            }
        } else {
            format!("p")
        };
        let g_id = bs58::encode(did.to_be_bytes()).into_string();
        Ok(format!("luffa://{}/{}/{}/{}",c_type, g_id, bs58::encode(secret_key).into_string(), tag))
    }

    ///Offer contacts
    pub fn contacts_offer(&self, code: &String) -> ClientResult<u64> {
        let mut tmp = code.split('/');
        let _from_tag = tmp.next_back();
        let key = tmp.next_back();
        let uid = tmp.next_back();
        let c_type = match tmp.next_back() {
            Some(tp) => {
                if tp == "g" {
                    ContactsTypes::Group
                } else {
                    ContactsTypes::Private
                }
            }
            _ => ContactsTypes::Private,
        };
        let mut to = [0u8; 8];
        to.clone_from_slice(&bs58::decode(uid.unwrap()).into_vec().unwrap());
        let to = u64::from_be_bytes(to);

        let my_id = self.get_local_id()?.unwrap();
        let tag = self.find_contacts_tag(my_id)?;

        let secret_key = key.unwrap();
        let key = bs58::decode(secret_key).into_vec().unwrap();

        let mut digest = crc64fast::Digest::new();
        digest.write(&key);
        let from_id = digest.sum64();

        let secret_key = key.to_vec();
        let mut digest = crc64fast::Digest::new();
        digest.write(&secret_key);
        let offer_id = digest.sum64();
        let msg = {
            let offer_key = Aes256Gcm::generate_key(&mut OsRng);
            let offer_key = offer_key.to_vec();
            tracing::warn!("secret_key::: {}", secret_key.len());
            let filter = self.filter.as_ref().map(|f| f.read().clone()).unwrap();
            let token = RUNTIME.block_on(async {
                let key = self.get_keypair(filter).await;
                let token = key.as_ref().map(|key| {
                    ContactsToken::new(
                        key,
                        tag,
                        offer_key.clone(),
                        c_type,
                    ).unwrap()
                });
                token.unwrap()
            });

            tracing::warn!("gen offer id:{}", offer_id);
            self.save_contacts_offer(offer_id, secret_key);

            Message::ContactsExchange {
                exchange: ContactsEvent::Offer { token },
            }
        };

        let res = match self.send_to(to, msg, from_id, Some(key)).map_err(|e| {
            tracing::warn!("send_to failed:{e:?}");
            ClientError::SendFailed
        }) {
            Ok(crc) => {
                Self::update_offer_status(self.db(),to, crc, OfferStatus::Offer);

                crc
            }
            Err(_) => {
                0
            }
        };
        Ok(res)
    }
    ///Offer group contacts
    pub fn contacts_group_create(
        &self,
        invitee: Vec<u64>,
        tag: Option<String>,
    ) -> ClientResult<u64> {
        let key_id = self.gen_key("", false)?.unwrap();
        let g_key = self.read_keypair(&key_id).unwrap();
        let g_id = bs58::decode(key_id).into_vec().unwrap();
        let mut buf = [0u8; 8];
        buf.clone_from_slice(&g_id[..8]);
        let g_id = u64::from_be_bytes(buf);

        let my_id = self.get_local_id()?.unwrap();
        // let tag = self.find_contacts_tag(my_id);
        let offer_key = Aes256Gcm::generate_key(&mut OsRng);
        let offer_key = offer_key.to_vec();
        let msg = {
            let mut digest = crc64fast::Digest::new();
            digest.write(&offer_key);
            let offer_id = digest.sum64();

            let token = ContactsToken::new(
                &g_key,
                tag,
                offer_key.clone(),
                luffa_rpc_types::ContactsTypes::Group,
            )
                .unwrap();

            tracing::warn!("gen offer id:{}", offer_id);
            self.save_contacts_offer(offer_id, offer_key.clone());
            let ContactsToken {
                public_key,
                sign,
                secret_key,
                contacts_type,
                comment,
                ..
            } = token.clone();
            Self::save_contacts(
                self.db(),
                g_id,
                secret_key,
                public_key,
                contacts_type,
                sign,
                comment,
                g_key.to_protobuf_encoding().ok()
            );

            Message::ContactsExchange {
                exchange: ContactsEvent::Offer { token },
            }
        };
        let members = invitee.clone();
        RUNTIME.block_on(async {
            let contacts = vec![Contacts {
                did: g_id,
                r#type: ContactsTypes::Group,
                have_time: 0,
                wants: vec![],
            }];

            let sync = Message::ContactsSync {
                did: my_id,
                contacts,
            };

            let event = Event::new(0, &sync, None, my_id);
            let data = event.encode().unwrap();
            let client_t = self.client.as_ref().unwrap().read().clone();
            let sender = self.sender.clone();
            let tx = self.sender.as_ref().map(|x| x.read().clone()).unwrap();
            tokio::spawn(async move {
                if let Err(e) = client_t
                    .chat_request(
                        bytes::Bytes::from(data),
                    )
                    .await
                {
                    tracing::warn!("send contacts sync status >>> {e:?}");
                }
                for i_id in invitee {
                    let (req, res) = tokio::sync::oneshot::channel();
                    let msg = serde_cbor::to_vec(&msg).unwrap();
                    tx.send((i_id, msg, my_id, req, None)).await.unwrap();
                    match res.await {
                        Ok(r) => {
                            tracing::info!("send group offer ok :{r:?}");
                        }
                        Err(e) => {
                            tracing::warn!("{e:?}");
                        }
                    }
                }
            });
        });
        Self::group_member_insert(self.db(), g_id, members)?;
        Ok(g_id)
    }

    ///Offer group contacts invite member
    pub fn contacts_group_invite_member(
        &self,
        g_id: u64,
        invitee: u64,
        tag: Option<String>,
    ) -> ClientResult<bool> {
        let group_keypair = format!("GROUPKEYPAIR-{}", g_id);
        let group_keypair = Self::get_key(self.db(), &group_keypair);
        let g_key = Keypair::from_protobuf_encoding(group_keypair.as_bytes())?;

        let my_id = self.get_local_id()?.unwrap();
        let offer_key = Aes256Gcm::generate_key(&mut OsRng);
        let offer_key = offer_key.to_vec();

        let msg = {
            let mut digest = crc64fast::Digest::new();
            digest.write(&offer_key);
            let offer_id = digest.sum64();

            let token = ContactsToken::new(
                &g_key,
                tag,
                offer_key.clone(),
                luffa_rpc_types::ContactsTypes::Group,
            )
                .unwrap();

            tracing::warn!("gen offer id:{}", offer_id);
            self.save_contacts_offer(offer_id, offer_key.clone());

            Message::ContactsExchange {
                exchange: ContactsEvent::Offer { token },
            }
        };
        RUNTIME.block_on(async {
            let contacts = vec![Contacts {
                did: g_id,
                r#type: ContactsTypes::Group,
                have_time: 0,
                wants: vec![],
            }];

            let sync = Message::ContactsSync {
                did: my_id,
                contacts,
            };

            let event = Event::new(0, &sync, None, my_id);
            let data = event.encode().unwrap();
            let client_t = self.client.as_ref().unwrap().read().clone();
            let sender = self.sender.as_ref().clone();
            let tx = sender.unwrap().read().clone();
            tokio::spawn(async move {
                if let Err(e) = client_t
                    .chat_request(
                        bytes::Bytes::from(data),
                    )
                    .await
                {
                    tracing::warn!("pub contacts sync status >>> {e:?}");
                }
                let (req, res) = tokio::sync::oneshot::channel();
                let msg = serde_cbor::to_vec(&msg).unwrap();
                tx.send((invitee, msg, my_id, req, None)).await.unwrap();
                match res.await {
                    Ok(r) => {
                        tracing::info!("send group offer ok :{r:?}");
                    }
                    Err(e) => {
                        tracing::warn!("{e:?}");
                    }
                }
            });

        });
        Self::group_member_insert(self.db(), g_id, vec![invitee])?;
        Ok(true)
    }
    pub fn contacts_anwser_from_crc(&self, to: u64, crc: u64) -> ClientResult<u64> {
        if let Ok(Some(meta)) = self.read_msg_with_meta(to, crc) {
            let EventMeta {
                msg,

                ..
            } = meta;
            let msg = message_from(msg).unwrap();
            match msg {
                Message::ContactsExchange { exchange }=>{
                    match exchange {
                        ContactsEvent::Offer { token }=>{
                            if let Some((offer_id, secret_key, ..)) =
                            Self::get_answer_from_tree(self.db(), crc)
                            {
                                let ContactsToken { group_key, contacts_type, .. } = token;
                                match contacts_type {
                                    ContactsTypes::Private=>{
                                        Self::update_offer_status(self.db(), to,crc, OfferStatus::Answer);
                                        return self.contacts_anwser(to, offer_id, secret_key);
                                    }
                                    ContactsTypes::Group=>{
                                        if let Some(g_key) = group_key {
                                            let mut data = [0u8;64];
                                            data.copy_from_slice(&g_key);
                                            if let Ok(keypair) = ssh_key::private::Ed25519Keypair::from_bytes(&data) {
                                                let keypair = luffa_node::Keypair::Ed25519(keypair);
                                                let g_key: Keypair = keypair.into();
                                                Self::update_offer_status(self.db(),to, crc, OfferStatus::Answer);
                                                return self.contacts_anwser_group(to, offer_id, secret_key, g_key);
                                            }    
                                        }
                                    }
                                }
                            }
                        }
                        _=>{

                        }
                    }
                }
                _=>{

                }
            }
        }
        Ok(0)
    }
    /// answer an offer and send it to from
    pub fn contacts_anwser(
        &self,
        to: u64,
        offer_id: u64,
        secret_key: Vec<u8>,
    ) -> ClientResult<u64> {
        let offer_key = Self::get_offer_by_offer_id(self.db(), offer_id);

        let my_id = self.get_local_id()?.unwrap();
        let comment = self.find_contacts_tag(my_id)?;
        tracing::warn!("secret_key::: {}",secret_key.len());
        let new_key =
            match self.get_secret_key(to) {
                Some(key) => {
                    tracing::warn!("contacts old s key");
                    key
                }
                None => secret_key.clone(),
            };
        let filter = self.filter.as_ref().map(|f| f.read().clone()).unwrap();
        let token = RUNTIME.block_on(async {
            let key = self.get_keypair(filter.clone()).await;
            let token = key.as_ref().map(|key| {
                ContactsToken::new(
                    key,
                    comment,
                    new_key,
                    luffa_rpc_types::ContactsTypes::Private,
                )
                    .unwrap()
            });
            token.unwrap()
        });

        let msg = {
            Message::ContactsExchange {
                exchange: ContactsEvent::Answer { token },
            }
        };

        let res = match self.send_to(to, msg, offer_id, offer_key).map_err(|e| {
            tracing::warn!("send_to failed:{e:?}");
            ClientError::SendFailed
        }) {
            Ok(crc) => {
                let now = Utc::now().timestamp_millis() as u64;
                Self::update_session(self.db(), to, None, None, None, None, None,now);
                crc
            }
            Err(_) => {
                0
            }
        };
        Ok(res)
    }
    /// answer an group offer and send it to
    pub fn contacts_anwser_group(
        &self,
        to: u64,
        offer_id: u64,
        secret_key: Vec<u8>,
        group_key: Keypair,
    ) -> ClientResult<u64> {
        let offer_key = Self::get_offer_by_offer_id(self.db(), offer_id);

        let comment = self.find_contacts_tag(to)?;
        tracing::warn!("secret_key::: {}", secret_key.len());
        let new_key = match self.get_secret_key(to) {
            Some(key) => {
                tracing::warn!("contacts old s key");
                key
            }
            None => secret_key.clone(),
        };
        let token = ContactsToken::new(
            &group_key,
            comment,
            new_key,
            luffa_rpc_types::ContactsTypes::Group,
        )
        .unwrap();

        let msg = {
            Message::ContactsExchange {
                exchange: ContactsEvent::Answer { token },
            }
        };

        let res = match self.send_to(to, msg, offer_id, offer_key).map_err(|e| {
            tracing::warn!("send_to failed:{e:?}");
            ClientError::SendFailed
        }) {
            Ok(crc) => {
                let now = Utc::now().timestamp_millis() as u64;
                Self::update_session(self.db(), to, None, None, None, None, None,now);
                crc
            }
            Err(_) => {
                0
            }
        };
        Ok(res)
    }

    fn save_contacts_offer(&self, offer_id: u64, secret_key: Vec<u8>) {
        let tree = Self::open_contact_tree(self.db()).unwrap();
        let offer_key = format!("OFFER-{}", offer_id);
        tree.insert(offer_key, secret_key).unwrap();
        tree.flush().unwrap();
    }

    fn get_secret_key(&self, did: u64) -> Option<Vec<u8>> {
        Self::get_contacts_skey(self.db(), did)
    }

    fn send_to(&self, to: u64, msg: Message, from_id: u64, key: Option<Vec<u8>>) -> Result<u64> {
        RUNTIME.block_on(async move {
            let sender = self.sender.as_ref().clone();
            let msg = serde_cbor::to_vec(&msg).unwrap();
            let from_id = if from_id > 0 {
                from_id
            } else {
                self.local_id().await.unwrap_or_default()
            };
            let tx = sender.unwrap().read().clone();
            match tokio::time::timeout(Duration::from_secs(15), async move {
                let (req, res) = tokio::sync::oneshot::channel();
                tx.send((to, msg, from_id, req, key)).await.unwrap();
                match res.await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("{e:?}");
                        Err(anyhow::anyhow!("{e:?}"))
                    }
                }
            })
                .await
            {
                Ok(Ok(crc)) => Ok(crc),
                Ok(Err(e)) => Err(anyhow::anyhow!("{e:?}")),
                Err(t) => Err(anyhow::anyhow!("{t:?}")),
            }
        })
    }

    /// Send msg to peer
    pub fn send_msg(&self, to: u64, msg: Vec<u8>) -> ClientResult<u64> {
        let res = match message_from(msg) {
            Some(msg) => {
                match self.send_to(to, msg, 0, None).map_err(|e| {
                    tracing::warn!("{e:?}");
                    ClientError::SendFailed
                }) {
                    Ok(crc) => crc,
                    Err(_e) => 0,
                }
            }
            None => 0,
        };
        Ok(res)
    }

    pub fn recent_messages(&self, did: u64, offset: u32, limit: u32) -> ClientResult<Vec<u64>> {
        let mut msgs = vec![];
        let table = format!("message_{did}_time");
        let tree = self.db().open_tree(&table)?;
        let mut itr = tree.into_iter();
        let mut skiped = 0;
        while let Some(val) = itr.next_back() {
            let (_k, v) = val?;
            if skiped < offset {
                skiped += 1;
                continue;
            }
            let mut key = [0u8; 8];
            key.clone_from_slice(&v[..8]);
            let crc = u64::from_be_bytes(key);
            msgs.push(crc);
            if msgs.len() >= limit as usize {
                break;
            }
        }
        Ok(msgs)
    }
    pub fn recent_offser(&self, top: u32) -> ClientResult<Vec<u64>> {
        let mut msgs = vec![];
        let table = format!("offer_time");
        let tree = self.db().open_tree(&table)?;
        let mut itr = tree.into_iter();
        while let Some(val) = itr.next_back() {
            let (_k, v) = val?;
            let mut key = [0u8; 8];
            key.clone_from_slice(&v[..8]);
            let crc = u64::from_be_bytes(key);
            msgs.push(crc);
            if msgs.len() >= top as usize {
                break;
            }
        }
        Ok(msgs)
    }
    pub fn meta_msg(&self, data: &[u8]) -> ClientResult<EventMeta> {
        let evt: Event = serde_cbor::from_slice(data)?;
        let Event {
            to,
            event_time,
            from_id,
            msg,
            ..
        } = evt;
        let (to_tag, _) = Self::get_contacts_tag(self.db(), to).unwrap_or_default();
        let (from_tag, _) = Self::get_contacts_tag(self.db(), from_id).unwrap_or_default();

        Ok(EventMeta {
            from_id,
            to_id: to,
            from_tag,
            to_tag,
            event_time,
            status:0,
            msg,
        })
    }

    pub fn read_msg_with_meta(&self, did: u64, crc: u64) -> ClientResult<Option<EventMeta>> {
        let table = format!("message_{did}");

        let tree = self.db().open_tree(&table)?;
        let db_t = self.db();
        let ok = match tree.get(crc.to_be_bytes()) {
            Ok(v) => {
                let vv = v.map(|v| {
                    let data = v.to_vec();
                    let evt: Event = serde_cbor::from_slice(&data[..]).unwrap();
                    let Event {
                        to,
                        event_time,
                        crc,
                        from_id,
                        nonce,
                        msg,
                    } = evt;
                    let mut key = Self::get_aes_key_from_contacts(db_t.clone(), did);
                    if key.is_none() {
                        key = Self::get_offer_by_offer_id(db_t.clone(), from_id);
                    }
                    let now = Utc::now().timestamp_millis() as u64;
                    if let Ok(msg) =
                        Message::decrypt(bytes::Bytes::from(msg.clone()), key, nonce.clone())
                    {
                        let status = Self::get_crc_tree_status(db_t.clone(),crc,&table) as u32;
                        match &msg {
                            Message::Chat { content } => match content {
                                ChatContent::Send { data } => {
                                    // let (_title, body) = Self::extra_content(data);
                                    if Self::update_session(
                                        db_t.clone(),
                                        did,
                                        None,
                                        Some(crc),
                                        None,
                                        None,
                                        Some(status as u8),
                                        now,
                                    ) {
                                        let msg = Message::Chat {
                                            content: ChatContent::Feedback {
                                                crc,
                                                status: luffa_rpc_types::FeedbackStatus::Read,
                                            },
                                        };
                                        if let Some(msg) = message_to(msg) {
                                            if self.send_msg(did, msg).unwrap() == 0 {
                                                tracing::error!("send read feedback failed");
                                            }
                                        }
                                        // tracing::error!("send read feedback to {did}");
                                    }
                                }
                                _ => {
                                    tracing::warn!("read No send message crc> {} did:{did} ,content:{:?}",crc,content);
                                    return None;
                                }
                            },
                            _ => {
                                tracing::warn!("read No chat message crc> {} did:{did} ,msg :{:?}",crc,msg);
                                
                            }
                        }

                        let (to_tag, _) =
                            Self::get_contacts_tag(db_t.clone(), to).unwrap_or_default();
                        let (from_tag, _) =
                            Self::get_contacts_tag(db_t.clone(), from_id).unwrap_or_default();
                        match message_to(msg) {
                            Some(msg) => Some(EventMeta {
                                from_id,
                                to_id: to,
                                from_tag,
                                to_tag,
                                event_time,
                                status,
                                msg,
                            }),
                            None => None,
                        }
                    } else {
                        if let Some(key) = Self::get_offer_by_offer_id(db_t.clone(), from_id) {
                            if let Ok(msg) =
                                Message::decrypt(bytes::Bytes::from(msg), Some(key), nonce)
                            {
                                let (to_tag, _) =
                                    Self::get_contacts_tag(db_t.clone(), to).unwrap_or_default();
                                let (from_tag, _) = Self::get_contacts_tag(db_t.clone(), from_id)
                                    .unwrap_or_default();
                                match message_to(msg) {
                                    Some(msg) => Some(EventMeta {
                                        from_id,
                                        to_id: to,
                                        from_tag,
                                        to_tag,
                                        event_time,
                                        status:0,
                                        msg,
                                    }),
                                    None => {
                                        error!("read msg 0: decrypt failed>>>");
                                        None
                                    }
                                }
                            } else {
                                error!("read msg 1: decrypt failed>>>");
                                None
                            }
                        } else {
                            error!("read msg 2: decrypt failed>>>");
                            None
                        }
                    }
                });

                let vv = vv.unwrap_or_default();

                vv
            }
            Err(e) => {
                error!("{e:?}");
                None
            }
        };
        Ok(ok)
    }

    pub fn remove_local_msg(&self, did: u64, crc: u64) -> ClientResult<()> {
        let table = format!("message_{did}");

        if !Self::have_in_tree(self.db(), crc, &table) {
            return Ok(());
        }

        let event_at = self
            .read_msg_with_meta(did, crc)
            .ok()
            .flatten()
            .map(|x| x.event_time);

        // 删除消息数据
        Self::burn_from_tree(self.db(), crc, table.clone());

        let event_table = format!("{}_time", table);
        if let Some(event_at) = event_at {
            Self::burn_from_tree(self.db(), event_at, event_table);
        } else {
            // // {
            // //     let x = 232323344u64;
            // //     let bs = x.to_be_bytes();
            // //
            // //     let x2 = bs.into_iter().fold(0, |acc, x| acc << 8 | (x as u64));
            // //     assert_eq!(x, x2)
            // // }

            // // 查找消息，使用消息id
            // let tree = self.db.open_tree(event_table.clone())?;
            // let id = tree.iter().find(|item| {
            //     item
            //         .as_ref()
            //         .ok()
            //         .and_then(|(_, v)| {
            //             let id = v.to_vec().into_iter().fold(0, |acc, x| acc << 8 | (x as u64));

            //             if id == crc {
            //                 Some(())
            //             } else {
            //                 None
            //             }
            //         })
            //         .is_some()
            // })
            //     .map(|x| x.ok())
            //     .flatten()
            //     .map(|(key, _)| {
            //         key.to_vec().into_iter().fold(0, |acc, x| acc << 8 | (x as u64))
            //     });


            // if let Some(id) = id {
            //     Self::burn_from_tree(self.db(), id, event_table)
            // } else {
            //     warn!("not found event_time id with crc id: {crc}");
            // }
        }

        Ok(())
    }


    pub fn get_local_id(&self) -> ClientResult<Option<u64>> {
        let res = RUNTIME.block_on(async {
            self.local_id().await
        });
        Ok(res)
    }

    pub fn get_did(&self) -> ClientResult<Option<String>> {
        Ok(self.get_local_id().unwrap().map(|d| {
            // hex::encode(d.to_be_bytes())
            bs58::encode(d.to_be_bytes()).into_string()
        }))
    }

    async fn local_id(&self) -> Option<u64> {
        let filter = self.filter.as_ref().map(|x|x.read().clone()).unwrap_or_default();
        let key = self.get_keypair(filter.clone()).await;
        key.map(|k| {
            let data = PeerId::from_public_key(&k.public()).to_bytes();
            let mut digest = crc64fast::Digest::new();
            digest.write(&data);
            digest.sum64()
        })
    }

    pub fn session_list(&self, top: u32) -> ClientResult<Vec<ChatSession>> {
        let tree = Self::open_session_tree(self.db())?;
        let my_id = self.get_local_id().unwrap().unwrap_or_default();
        let mut chats = tree
            .into_iter()
            .map(|item| {
                let (_key, val) = item.unwrap();
                let chat: ChatSession = serde_cbor::from_slice(&val[..]).unwrap();
                chat
            })
            .filter(|c| c.did != my_id)
            .collect::<Vec<_>>();

        chats.sort_by(|a, b| a.last_time.partial_cmp(&b.last_time).unwrap());
        chats.reverse();

        chats.truncate(top as usize);
        Ok(chats)
    }

    /// pagination session
    pub fn session_page(&self, page: u32, size: u32) -> ClientResult<Vec<ChatSession>> {
        let my_id = self.get_local_id()?.unwrap_or_default();
        Ok(Self::db_session_list(self.db(), page, size, my_id).unwrap_or_default())
    }

    pub fn keys(&self) -> ClientResult<Vec<String>> {
        // luffa_node::Keychain::keys(&self)
        RUNTIME.block_on(async {
            if let Some(chain) = self.key.as_ref() {
                let chain = chain.read();
                let keys = chain.keys().map(|m| match m {
                    Ok(k) => Some(k.name()),
                    Err(_e) => None
                }).collect::<Vec<_>>().await;

                Ok(keys.into_iter().filter(|x| x.is_some()).map(|x| x.unwrap()).collect::<Vec<_>>())
            } else {
                Err(ClientError::CustomError("get keychain fail".to_string()))
            }
        })
    }

    pub fn gen_key(&self, password: &str, store: bool) -> ClientResult<Option<String>> {
        RUNTIME.block_on(async {
            if let Some(chain) = self.key.as_ref() {
                let mut chain = chain.write();
                match chain.create_ed25519_key_bip39(password, store).await {
                    Ok((phrase, key)) => {
                        let name = key.name();
                        Self::save_mnemonic_keypair(self.key_db(), &phrase, key)?;
                        Ok(Some(name))
                    }
                    Err(e) => {
                        Err(ClientError::CustomError(e.to_string()))
                    }
                }
            }
            else{
                Err(ClientError::CustomError(format!("key is none")))
            }
        })
    }
    pub fn import_key(&self, phrase: &str, password: &str) -> ClientResult<Option<String>> {
        RUNTIME.block_on(async {
            if let Some(chain) = self.key.as_ref() {
                let mut chain = chain.write();
                match chain.create_ed25519_key_from_seed(phrase, password).await {
                    Ok(key) => {
                        let name = key.name();
                        Self::save_mnemonic_keypair(self.key_db(), phrase, key)?;
                        Ok(Some(name))
                    }
                    Err(e) => {
                        Err(ClientError::CustomError(e.to_string()))
                    }
                }
            } else {
                Err(ClientError::CustomError("get keychain fail".to_string()))
            }
        })
    }

    pub fn save_key(&self, name: &str) -> ClientResult<bool> {
        RUNTIME.block_on(async {
            // let tree = self.db.open_tree("bip39_keys")?;
            // let k_pair = format!("pair-{}", name);
            if let Ok(Some(k_val)) = Self::get_mnemonic_keypair(self.key_db(), name) {
                if let Some(chain) = self.key.as_ref() {
                    let mut chain = chain.write();
                    let mut data = [0u8; 64];
                    data.clone_from_slice(&k_val);
                    if let Ok(k) = chain.create_ed25519_key_from_bytes(&data).await {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    Err(ClientError::CustomError("get keychain fail".to_string()))
                }
            } else {
                Ok(true)
            }
        })
    }

    pub fn remove_key(&self, name: &str) -> ClientResult<bool> {
        if let Err(_) = self.stop() {}

        RUNTIME.block_on(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Ok(Some(_)) = Self::remove_mnemonic_keypair(self.key_db(), name) {
                if let Some(chain) = self.key.as_ref() {
                    let mut chain = chain.write();
                    match chain.remove(name).await {
                        Ok(_) => {
                            let mut u_id = [0u8;8];
                            let tmp = bs58::decode(name).into_vec()?;
                            u_id.copy_from_slice(&tmp);
                            let u_id = u64::from_be_bytes(u_id);
                            let path = luffa_util::luffa_data_path(&KVDB_CONTACTS_FILE.read()).unwrap().join(format!("{u_id}"));
                            let idx_path = luffa_util::luffa_data_path(&LUFFA_CONTENT.read()).unwrap().join(format!("{u_id}"));
                            std::fs::remove_dir_all(path)?;
                            std::fs::remove_dir_all(idx_path)?;
                            Ok(true)
                        }
                        Err(e) => {
                            Err(ClientError::CustomError(e.to_string()))
                        }
                    }
                } else {
                    Err(ClientError::CustomError("get keychain fail".to_string()))
                }
            } else {
                Ok(false)
            }
        })
    }
    pub fn read_key_phrase(&self, name: &str) -> ClientResult<Option<String>> {
        Self::get_mnemonic(self.key_db(), name)
    }
    pub fn read_keypair(&self, id: &str) -> Option<Keypair> {
        if let Ok(Some(k_val)) = Self::get_mnemonic_keypair(self.key_db(), id) {
            let mut data = [0u8; 64];
            data.clone_from_slice(&k_val);
            if let Ok(keypair) = ssh_key::private::Ed25519Keypair::from_bytes(&data) {
                let keypair = luffa_node::Keypair::Ed25519(keypair);
                let k: Keypair = keypair.into();
                return Some(k);
            }
        }

        None
    }

    pub fn save_session(
        &self,
        did: u64,
        tag: String,
        read: Option<u64>,
        reach: Option<u64>,
        msg: Option<String>,
    ) -> ClientResult<()> {
        let now = Utc::now().timestamp_millis() as u64;
        Self::update_session(self.db(), did, Some(tag), read, reach, msg,None, now);
        Ok(())
    }

    pub fn find_contacts_tag(&self, did: u64) -> ClientResult<Option<String>> {
        Ok(Self::get_contacts_tag(self.db(), did).map(|(v, _)| v))
    }

    pub fn update_contacts_tag(&self, did: u64, tag: String) {
        Self::set_contacts_tag(self.db(), did, tag)
    }

    pub fn contacts_list(&self, c_type: u8) -> ClientResult<Vec<ContactsView>> {
        let tree = Self::open_contact_tree(self.db())?;
        let tag_prefix = format!("TAG-");
        let itr = tree.scan_prefix(tag_prefix);
        let my_id = self.get_local_id()?;
        let mut res = vec![];
        for item in itr {
            let (k, v) = item.unwrap();
            let tag = String::from_utf8(v.to_vec())?;
            let key = String::from_utf8(k.to_vec())?;
            let to = key.split('-').last().unwrap();
            let to: u64 = to.parse()?;
            let (flag,c_type) = 
            if let Some(t) = Self::get_contacts_type(self.db(), to) {
                (c_type == t as u8 && Some(to) != my_id,t as u8)
            } else if c_type == 0 {
                (Some(to) != my_id,0)
            }
            else{
                (false,3)
            };
            if flag {
                res.push(ContactsView { did: to, tag,c_type });
            }
        }
        Ok(res)
    }
    pub fn contacts_search(&self, c_type: u8, pattern: &str) -> ClientResult<Vec<ContactsView>> {
        let tree = Self::open_contact_tree(self.db())?;
        let options = SearchOptions::new()
            .levenshtein(true)
            .case_sensitive(false)
            .threshold(0.2)
            .stop_whitespace(true);
        let mut engine: SimSearch<ContactsView> = SimSearch::new_with(options);
        let tag_prefix = format!("TAG-");
        let itr = tree.scan_prefix(tag_prefix);
        let my_id = self.get_local_id()?;
        for item in itr {
            let (k, v) = item.unwrap();
            let tag = String::from_utf8(v.to_vec())?;
            let key = String::from_utf8(k.to_vec())?;
            let to = key.split('-').last().unwrap();
            let to: u64 = to.parse()?;
            let to_id = bs58::encode(to.to_be_bytes().to_vec()).into_string();
            // if to_id.to_lowercase().contains(keyword)
            let mut flag = false;
            if c_type > 1 {
                flag = true;
            } else {
                if let Some(t) = Self::get_contacts_type(self.db(), to) {
                    flag = c_type == t as u8 && Some(to) != my_id;
                } else if c_type == 0 {
                    flag = true && Some(to) != my_id;
                };
            }
            if flag {
                if let Some(t) = Self::get_contacts_type(self.db(), to) {
                    let c_type = t as u8;
                    let c_to = ContactsView {
                        did: to,
                        tag:tag.clone(),
                        c_type,
                    };
                    engine.insert(c_to, &format!("{to_id} {tag}"));
                }
            }
        }
        let res = engine.search(pattern);
        Ok(res)
    }

    pub fn search(&self, query: String, offset: u32, limit: u32) -> ClientResult<Vec<String>> {
        let idx = self.idx.as_ref().unwrap().read();
        let reader = idx.reader()?;
        let schema = idx.schema();
        let searcher = reader.searcher();

        let title = schema
            .get_field("title")
            .ok_or(ClientError::CustomError("get filed title fail".to_string()))?;
        let body = schema
            .get_field("body")
            .ok_or(ClientError::CustomError("get filed body fail".to_string()))?;

        let query_parser = QueryParser::for_index(&idx, vec![title, body]);

        // QueryParser may fail if the query is not in the right
        // format. For user facing applications, this can be a problem.
        // A ticket has been opened regarding this problem.
        let query = query_parser
            .parse_query(&query)
            .map_err(|_e| ClientError::SearchError)?;

        // Perform search.
        // `topdocs` contains the 10 most relevant doc ids, sorted by decreasing scores...
        let top_docs: Vec<(Score, DocAddress)> = searcher
            .search(
                &query,
                &TopDocs::with_limit(limit as usize).and_offset(offset as usize),
            )
            .map_err(|_e| ClientError::SearchError)?;
        let mut docs = vec![];
        for (_score, doc_address) in top_docs {
            // Retrieve the actual content of documents given its `doc_address`.
            let retrieved_doc = searcher
                .doc(doc_address)
                .map_err(|_e| ClientError::SearchError)?;
            // tracing::info!("{}", schema.to_json(&retrieved_doc));
            docs.push(schema.to_json(&retrieved_doc));
        }
        Ok(docs)
    }

    pub fn get_peer_id(&self) -> ClientResult<Option<String>> {
        let res = RUNTIME.block_on(async {
            let filter = self.filter.as_ref().map(|x|x.read().clone()).unwrap_or(None);
            let key = self.get_keypair(filter.clone()).await;
            key.map(|k| {
                let data = PeerId::from_public_key(&k.public()).to_bytes();
                bs58::encode(data).into_string()
            })
        });
        Ok(res)
    }

    pub fn relay_list(&self) -> ClientResult<Vec<String>> {
        let client = self.client.as_ref().map(|x| x.read().clone());

        let list = RUNTIME.block_on(async {

            if let Some(cc) = client.as_ref() {
                match cc.get_peers().await {
                    Ok(peers) => {
                        tracing::debug!("peers:{peers:?}");
                        return peers
                            .into_iter()
                            .map(|(p, _)| bs58::encode(p.to_bytes()).into_string())
                            .collect::<Vec<_>>();
                    }
                    Err(e) => {
                        tracing::info!("{e:?}");
                    }
                }
            }
            tracing::info!("client is None");
            vec![]
        });
        Ok(list)
    }

    pub fn connect(&self, peer_id: String) -> ClientResult<bool> {
        let (_, data) = multibase::decode(&peer_id)?;
        let peer_id = PeerId::from_bytes(&data)?;
        let client = self.client.as_ref().map(|x| x.read().clone());
        let ok = RUNTIME.block_on(async {
            if let Some(cc) = client.as_ref() {
                match cc.connect(peer_id, vec![]).await {
                    Ok(_) => true,
                    _ => false,
                }
            } else {
                false
            }
        });
        Ok(ok)
    }

    pub fn disconnect(&self) -> ClientResult<bool> {
        let peer_id = self.get_peer_id()?.unwrap();
        let (_, data) = multibase::decode(&peer_id)?;
        let peer_id = PeerId::from_bytes(&data)?;
        let client = self.client.as_ref().map(|x| x.read().clone());
        let ok = RUNTIME.block_on(async {
            if let Some(cc) = client.as_ref() {
                match cc.disconnect(peer_id).await {
                    Ok(_) => true,
                    _ => false,
                }
            } else {
                false
            }
        });
        Ok(ok)
    }

    pub fn stop(&self) -> ClientResult<()> {
        if let Err(e) =
            self.send_to(
                u64::MAX,
                Message::StatusSync {
                    to: 0,
                    from_id: 0,
                    status: AppStatus::Bye,
                },
                u64::MAX,
                None,
            ) {
            tracing::warn!("{e:?}");
            return Err(ClientError::CustomError(e.to_string()));
        }
        Ok(())
    }
    pub fn init(&self, cfg_path: Option<String>) -> ClientResult<()> {
        info!("is_init {}", self.is_init.load(Ordering::SeqCst));

        if self.is_init.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!("init: at => {}", chrono::Local::now());
        info!("init: path => {cfg_path:?}");

        #[cfg(unix)]
        {
            match luffa_util::increase_fd_limit() {
                Ok(soft) => tracing::debug!("NOFILE limit: soft = {}", soft),
                Err(err) => tracing::error!("Error increasing NOFILE limit: {}", err),
            }
        }
        let args: HashMap<String, String> = HashMap::new();
        let dft_path = luffa_config_path(CONFIG_FILE_NAME)?;
        let cfg_path = cfg_path.map(|p| PathBuf::from(p));
        let cfg_path = cfg_path.unwrap_or(dft_path);
        println!("cfg_path:{cfg_path:?}");

        let sources = [Some(cfg_path.as_path())];
        let mut config = make_config(
            // default
            Config::default(),
            // potential config files
            &sources,
            // env var prefix for this config
            ENV_PREFIX,
            // map of present command line arguments
            args,
        )?;

        println!("config--->{config:?}");
        config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);

        // let metrics_config = config.metrics.clone();
        let store_config = config.store.clone();
        RUNTIME.block_on(async {
            // let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
            //     .await
            //     .expect("failed to initialize metrics");

            let kc = Keychain::<DiskStorage>::new(config.p2p.clone().key_store_path.clone())
                .await.unwrap();

            self.key.as_ref().map(|x| {
               let mut x = x.write();
               *x = kc; 
            });
            self.config.as_ref().map(|x| {
                let mut x = x.write();
                *x = config;
            });
            let store = start_store(store_config).await.unwrap();
            self.store.as_ref().map(|x|{
                let mut x = x.write();
                *x = Arc::new(store);
            });

            let path = luffa_util::luffa_data_path(&KVDB_CONTACTS_FILE.read())
            .unwrap()
            .join(format!("keys"));
        
            info!("path >>>> {:?}", &path);

            let db = Arc::new(sled::open(path).expect("open db failed"));
            self.key_db.as_ref().map(|x| {
                let mut x = x.write();
                *x = db.clone();
            });

        });

        self.is_init.store(true, Ordering::SeqCst);
        Ok(())

    }

    pub fn start(
        &self,
        key: Option<String>,
        tag: Option<String>,
        cb: Box<dyn Callback>,
    ) -> ClientResult<u64> {
        info!("is_start {}", self.is_started.load(Ordering::SeqCst));
        if self.is_started.load(Ordering::SeqCst) {
            return Ok(self
                .get_local_id()
                .ok()
                .flatten()
                .expect("client get local id"));
        }

        // let keychain = Keychain::<DiskStorage>::new(config.p2p.clone().key_store_path.clone());
        let filter = key.map(|k| KeyFilter::Name(format!("{}", k)));
        self.filter.as_ref().map(|x| {
            let mut x = x.write();
            *x = filter.clone();
        });

        let my_id = self.get_local_id()?;
        if my_id.is_none() {
            return Ok(0);
        }
        let my_id = my_id.unwrap();

        let path = luffa_util::luffa_data_path(&KVDB_CONTACTS_FILE.read())
            .unwrap()
            .join(format!("{my_id}"));
        let idx_path = luffa_util::luffa_data_path(&LUFFA_CONTENT.read())
            .unwrap()
            .join(format!("{my_id}"));
        info!("path >>>> {:?}", &path);
        info!("idx_path >>>> {:?}", &idx_path);

        let db = Arc::new(sled::open(path).expect("open db failed"));
        self.db.as_ref().map(|x| {
            let mut x = x.write();
            *x = db.clone();
        });
        let (idx, schema) = content_index(idx_path.as_path());
        let writer = idx
            .writer_with_num_threads(1, 12 * 1024 * 1024)
            .or_else(|e| {
                match e {
                    TantivyError::LockFailure(_, _) => {
                        // 清除文件锁，再次尝试打开 indexWriter
                        warn!("first open indexWriter failed because of acquire file lock failed");
                        fs::remove_file(&idx_path.join(".tantivy-writer.lock"))
                            .expect("release indexWriter lock failed");

                        idx.writer_with_num_threads(1, 12 * 1024 * 1024)
                    }
                    _ => panic!("acquire indexWriter failed: {:?}", e),
                }
            })
            .expect("acquire indexWriter failed");

        self.update_contacts_tag(my_id, tag.unwrap_or(format!("{}", my_id)).to_string());

        let (tx, rx) = tokio::sync::mpsc::channel(4096);

        self.sender.as_ref().map(|x| {
            let mut x = x.write();
            *x = tx;
        });
        let idx_writer = Arc::new(RwLock::new(writer));
        // self.schema.map(|x| x.replace(schema));

        let store = self.store.as_ref().unwrap().read().clone();
        let config = self.config.as_ref().unwrap().read().clone();
        let kc = self.key.as_ref().unwrap().read().clone();
        RUNTIME.block_on(async {
            let (peer, p2p_rpc, events, sender) = {
                let (peer_id, p2p_rpc, events, sender) =
                    start_node(config.p2p.clone(), kc, store, filter)
                        .await
                        .unwrap();
                (peer_id, p2p_rpc, events, sender)
            };
            let client = Arc::new(luffa_node::rpc::P2p::new(sender));
            let client = P2pClient::new(client).unwrap();
            self.client.as_ref().map(|x| {
                let mut x = x.write();
                *x = client.clone();
            });
            tokio::spawn(async move {
                debug!("runing...");
                Self::run(
                    db, cb, rx, idx_writer,schema, &peer, client,
                    events, p2p_rpc,
                )
                .await;
                debug!("run exit!....");
            });
        });

        self.is_started.store(true, Ordering::SeqCst);
        Ok(my_id)
    }

    pub fn generate_nickname(&self, peer_id: &str) -> ClientResult<String> {
        Ok(generate_nickname(peer_id))
    }

    pub fn generate_avatar(&self, peer_id: &str) -> ClientResult<String> {
        Ok(generate_avatar(peer_id))
    }

    /// run
    async fn run(
        db: Arc<Db>,
        cb: Box<dyn Callback>,
        mut receiver: tokio::sync::mpsc::Receiver<(
            u64,
            Vec<u8>,
            u64,
            ShotSender<anyhow::Result<u64>>,
            Option<Vec<u8>>,
        )>,
        idx_writer: Arc<RwLock<IndexWriter>>,
        schema: Schema,
        peer: &PeerId,
        client: P2pClient,
        mut events: tokio::sync::mpsc::Receiver<NetworkEvent>,
        p2p_rpc: JoinHandle<()>,
    ) {
        // let (tx, rx) = tokio::sync::mpsc::channel::<NetworkEvent>(4096);
        let cb = Arc::new(cb);

        let mut digest = crc64fast::Digest::new();
        digest.write(&peer.to_bytes());
        let my_id = digest.sum64();
        let db_t = db.clone();
        let client_t = client.clone();
        let sync_task = tokio::spawn(async move {
            let mut count = 0_u64;
            loop {

                if count % 6 == 0 {
                    tracing::info!("subscribed all as client,status sync");
                    let msg = luffa_rpc_types::Message::StatusSync {
                        to: 0,
                        status: AppStatus::Active,
                        from_id: my_id,
                    };
                    let key: Option<Vec<u8>> = None;
                    let event = Event::new(0, &msg, key, my_id);
                    let data = event.encode().unwrap();
                    let client_t = client_t.clone();
                    tokio::spawn(async move {
                        if let Err(e) = client_t
                            .chat_request(
                                bytes::Bytes::from(data),
                            )
                            .await
                        {
                            tracing::error!("status sync pub>> {e:?}");
                        }
                    });
                }

                let page = count % 8;
                let mut contacts = vec![];
                for lvl in 0..8 {
                    if page >= lvl {
                        if let Some(lvl_0) =
                            Self::db_session_list(db_t.clone(), lvl as u32, 4, my_id)
                        {
                            let lvl_contacts = lvl_0
                                .into_iter()
                                .map(|cs| {
                                    let to = cs.did;
                                    let c_type = Self::get_contacts_type(db_t.clone(), to)
                                        .unwrap_or(ContactsTypes::Private);
                                    let c_type: u8 = c_type as u8;
                                    let c_type = if c_type == 0 {
                                        ContactsTypes::Private
                                    } else {
                                        ContactsTypes::Group
                                    };
                                    let have_time = Self::get_contacts_have_time(db_t.clone(), to);
                                    Contacts {
                                        did: to,
                                        r#type: c_type,
                                        have_time,
                                        wants: vec![],
                                    }
                                })
                                .collect::<Vec<_>>();

                            contacts.extend_from_slice(&lvl_contacts[..]);
                        }
                    }
                }
                // tracing::error!("recent seesion sync:{}",contacts.len());
                if !contacts.is_empty() {
                    let sync = Message::ContactsSync {
                        did: my_id,
                        contacts,
                    };
                    let event = Event::new(0, &sync, None, my_id);
                    let data = event.encode().unwrap();
                    let client_t = client_t.clone();
                    tokio::spawn(async move {
                        if let Err(e) = client_t.chat_request(bytes::Bytes::from(data)).await {
                            tracing::error!("pub contacts sync status >>> {e:?}");
                        }
                    });
                }
                if count % 60 == 0 {
                    let tree = db_t.open_tree(KVDB_CONTACTS_TREE).unwrap();

                    let tag_prefix = format!("TAG-");
                    let itr = tree.scan_prefix(tag_prefix);
                    let contacts = itr
                        .map(|item| {
                            let (k, _v) = item.unwrap();
                            // let tag = String::from_utf8(v.to_vec()).unwrap();
                            let key = String::from_utf8(k.to_vec()).unwrap();
                            let parts = key.split('-');

                            let to = parts.last().unwrap();
                            let to: u64 = to.parse().unwrap();
                            let c_type = Self::get_contacts_type(db_t.clone(), to)
                                .unwrap_or(ContactsTypes::Private);
                            let c_type: u8 = c_type as u8;
                            let c_type = if c_type == 0 {
                                ContactsTypes::Private
                            } else {
                                ContactsTypes::Group
                            };
                            let have_time = Self::get_contacts_have_time(db_t.clone(), to);
                            Contacts {
                                did: to,
                                r#type: c_type,
                                have_time,
                                wants: vec![],
                            }
                        })
                        .collect::<Vec<_>>();

                    let sync = Message::ContactsSync {
                        did: my_id,
                        contacts,
                    };
                    let event = Event::new(0, &sync, None, my_id);
                    let data = event.encode().unwrap();
                    let client_t = client_t.clone();
                    tokio::spawn(async move {
                        if let Err(e) = client_t
                            .chat_request(bytes::Bytes::from(data.clone()))
                            .await
                        {
                            tracing::error!("pub contacts sync status >>> {e:?}");
                        }
                        
                    });
                }
                
                count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });
       
        let client_t = client.clone();
        let db_t = db.clone();
        let idx = idx_writer.clone();
        let idx_t = idx.clone();
        let idx_tt = idx.clone();
        let schema_t = schema.clone();
        let schema_tt = schema.clone();
        let cb_local = cb.clone();
        let process = tokio::spawn(async move {
            while let Some(evt) = events.recv().await {
                match evt {
                    NetworkEvent::RequestResponse(rsp) => {
                        match rsp {
                            luffa_node::ChatEvent::Response { data, .. } => {
                                if let Ok(im) = Event::decode_uncheck(&data) {
                                    let Event {
                                        msg,
                                        nonce,
                                        to,
                                        from_id,
                                        crc,
                                        event_time,
                                        ..
                                    } = im;
                                    
                                    if nonce.is_none() {
                                        
                                        if let Ok(msg_d) = luffa_rpc_types::Message::decrypt(
                                            bytes::Bytes::from(msg.clone()),
                                            None,
                                            nonce,
                                        ) {
                                            tracing::info!("clinet>>>>>nonce {msg_d:?}");
                                            let msg_t = msg_d.clone();
                                            let mut will_to_ui = true;
                                            match msg_d {
                                                Message::ContactsSync { did, contacts } => {
                                                    will_to_ui = false;
                                                    if did != my_id {
                                                        continue;
                                                    }
                                                    for ctt in contacts {
                                                        let did = ctt.did;
                                                        for crc in ctt.wants {
                                                            let table =
                                                                format!("message_{}", ctt.did);

                                                            let clt = client_t.clone();
                                                            let db_tt = db_t.clone();
                                                            // add to wants crc of contacts
                                                            let cb_t = cb.clone();
                                                            let scm = schema_tt.clone();
                                                            let idx = idx_tt.clone();
                                                            tokio::spawn(async move {
                                                                if !Self::have_in_tree(
                                                                    db_tt.clone(),
                                                                    crc,
                                                                    &table,
                                                                ) {
                                                                    match clt
                                                                        .get_crc_record(crc)
                                                                        .await
                                                                    {
                                                                        Ok(res) => {
                                                                            let data = res.data;
                                                                            // TODO remove crc from wants and update want_time
                                                                            tracing::info!(
                                                                                "get record: {crc}"
                                                                            );
                                                                            Self::set_contacts_have_time(db_tt.clone(), did,event_time);
                                                                            let data =
                                                                                data.to_vec();
                                                                            if !Self::have_in_tree(
                                                                                db_tt.clone(),
                                                                                crc,
                                                                                &table,
                                                                            ) {
                                                                                Self::process_event(
                                                                                    db_tt.clone(),
                                                                                    cb_t.clone(),
                                                                                    clt.clone(),
                                                                                    idx.clone(),
                                                                                    scm,
                                                                                    &data,
                                                                                    my_id,
                                                                                )
                                                                                .await;
                                                                            }
                                                                            if let Ok(im) = Event::decode_uncheck(&data) {
                                                                                let Event {
                                                                                    msg,
                                                                                    event_time,
                                                                                    nonce,
                                                                                    ..
                                                                                } = im;
                                                                                if let Some(key) = Self::get_aes_key_from_contacts(db_tt.clone(), did) {
                                                                                    if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), Some(key), nonce) {
                                                                                        let feedback = msg.chat_feedback();
                                                                                        match feedback {
                                                                                            Some((crc,status))=>{
                                                                                                match status {
                                                                                                    FeedbackStatus::Read=>{
                                                                                                        let table = format!("message_{did}");
                                                                                                        Self::save_to_tree_status(db_tt.clone(),did,&table,5);
                                                                                                       
                                                                                                    }
                                                                                                    FeedbackStatus::Reach=>{
                                                                                                        let table = format!("message_{did}");
                                                                                                        Self::save_to_tree_status(db_tt.clone(),did,&table,4);
                                                                                                       
                                                                                                    }
                                                                                                    _=>{

                                                                                                    }
                                                                                                }
                                                                                            }
                                                                                            None=>{

                                                                                            }
                                                                                        }
                                                                                    }
                                                                                }

                                                                            }
                                                                        }
                                                                        Err(e) => {
                                                                            tracing::info!("get crc record failed:{e:?}");
                                                                            Self::set_contacts_have_time(db_tt.clone(), did, event_time);
                                                                        }
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    }
                                                }
                                                Message::Feedback {
                                                    crc, to_id, status, ..
                                                } => match status {
                                                    
                                                    FeedbackStatus::Send | FeedbackStatus::Routing=>{
                                                        let to = to_id.unwrap_or_default();
                                                        if to > 0 {
                                                            tracing::info!("clinet>>>>>on_message send {crc:?} from {from_id} to {to} msg:{msg_t:?}");
                                                            let table = format!("message_{to}");
                                                            Self::save_to_tree_status(db_t.clone(),to,&table,2);
                                                        }
                                                        else{
                                                            
                                                        }
                                                        will_to_ui = false;
                                                    }
                                                    FeedbackStatus::Fetch | FeedbackStatus::Notice => {
                                                        let ls_crc = crc;
                                                        will_to_ui = false;
                                                        let client_t = client_t.clone();
                                                        let db_tt = db_t.clone();
                                                        let cb_t = cb.clone();
                                                        let idx_t = idx.clone();
                                                        let schema_t = schema.clone();
                                                        tokio::spawn(async move {
                                                            let client_t = client_t.clone();
                                                            let db_tt = db_tt.clone();
                                                            let cb_t = cb_t.clone();
                                                            let idx_t = idx_t.clone();
                                                            let schema_t = schema_t.clone();
                                                            for crc in ls_crc {
                                                                match client_t.get_crc_record(crc).await {
                                                                    Ok(res) => {
                                                                        let data = res.data;
                                                                        tracing::warn!("response get record: {crc} , f> {from_id:?}, t> {to_id:?}");
                                                                        let data = data.to_vec();
                                                                        if let Ok(im) = Event::decode_uncheck(&data) {
                                                                            let Event {
                                                                                to,
                                                                                from_id,
                                                                                crc,
                                                                                ..
                                                                            } = im;
                                
                                                                            let did = if to == my_id { from_id } else { to };
                                                                            let table = format!("message_{}", did);
                                
                                                                            if !Self::have_in_tree(db_tt.clone(), crc, &table) {
                                                                                Self::process_event(
                                                                                    db_tt.clone(), cb_t.clone(), client_t.clone(), idx_t.clone(), schema_t.clone(), &data, my_id,
                                                                                )
                                                                                .await;
                                                                            }
                                                                            else{
                                                                                tracing::error!("have in tree {crc}");
                                                                                // client_t.chat_request(bytes::Bytes::from());
                                                                                let feed = luffa_rpc_types::Message::Feedback { crc:vec![crc], from_id:Some(my_id), to_id: Some(0), status: luffa_rpc_types::FeedbackStatus::Reach };
                                                                                let event = luffa_rpc_types::Event::new(
                                                                                    0,
                                                                                    &feed,
                                                                                    None,
                                                                                    my_id,
                                                                                );
                                                                                tracing::warn!("having>>> send feedback reach to relay");
                                                                                let event = event.encode().unwrap();
                                                                                if let Err(e) =
                                                                                    client_t.chat_request(bytes::Bytes::from(event)).await
                                                                                {
                                                                                    error!("{e:?}");
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(e)=> {
                                                                        error!("record not found {crc} error: {e:?}");
                                                                    }
                                                                }
                                                                
                                                            }
                                                        });
                                                    }
                                                    _ => {
                                                        // tracing::warn!("clinet>>>>>on_message send {crc:?} from {from_id} to {to_id:?}");
                                                    }
                                                },
                                                _ => {}
                                            }
                                            if will_to_ui {
                                                cb.on_message(crc,from_id,to,event_time,msg);
                                            }
                                        }
                                    } else {
                                        let did = if to == my_id { from_id } else { to };
                                        let table = format!("message_{}", did);
                                        let idx = idx.clone();
                                        let db_t = db_t.clone();
                                        let schema_tt = schema_tt.clone();
                                        let client_t = client_t.clone();
                                        let cb = cb.clone();
                                        tokio::spawn(async move {
                                            if !Self::have_in_tree(db_t.clone(), crc, &table) {
                                                Self::process_event(
                                                    db_t, cb, client_t, idx, schema_tt, &data,
                                                    my_id,
                                                )
                                                .await;
                                            }
                                        });
                                    }
                                }
                            }
                            luffa_node::ChatEvent::Request(data) => {
                                //TODO message which coming from other
                                if let Ok(im) = Event::decode_uncheck(&data) {
                                    let Event {
                                        from_id, to, crc, nonce,msg,..
                                    } = im;
                                    // if  to == my_id && nonce.is_some() {
                                    //     let feed = luffa_rpc_types::Message::Feedback { crc:vec![crc], from_id:Some(from_id), to_id:Some(to), status: FeedbackStatus::Reach };
                                    //     let event = luffa_rpc_types::Event::new(
                                    //         0,
                                    //         &feed,
                                    //         None,
                                    //         my_id,
                                    //     );
                                    //     tracing::warn!("send feedback to {from_id}");
                                    //     let event = event.encode().unwrap();
                                    //     if let Err(e) =
                                    //         client_t.chat_request(bytes::Bytes::from(event)).await
                                    //     {
                                    //         error!("{e:?}");
                                    //     }
                                    // }
                                    if to == my_id && nonce.is_none() {
                                        if let Ok(m) = Message::decrypt(bytes::Bytes::from(msg.clone()), None, nonce) {
                                            // let m_t = m.clone();
                                            match m {
                                                Message::Feedback {
                                                    crc,
                                                    from_id,
                                                    to_id,
                                                    status,
                                                } => match status {
                                                    FeedbackStatus::Fetch | FeedbackStatus::Notice => {
                                                        let ls_crc = crc;
                                                        
                                                        for crc in ls_crc {
                                                            let client_t = client_t.clone();
                                                            let db_tt = db_t.clone();
                                                            let cb_t = cb.clone();
                                                            let idx_t = idx.clone();
                                                            let schema_t = schema.clone();
                        
                                                            tokio::spawn(async move {
                                                                match client_t.get_crc_record(crc).await {
                                                                    Ok(res) => {
                                                                        let data = res.data;
                                                                        tracing::warn!("get record: {crc} , f> {from_id:?}, t> {to_id:?}");
                                                                        let data = data.to_vec();
                                                                        if let Ok(im) = Event::decode_uncheck(&data) {
                                                                            let Event {
                                                                                to,
                                                                                from_id,
                                                                                crc,
                                                                                ..
                                                                            } = im;
                                
                                                                            let did = if to == my_id { from_id } else { to };
                                                                            let table = format!("message_{}", did);
                                
                                                                            if !Self::have_in_tree(db_tt.clone(), crc, &table) {
                                                                                Self::process_event(
                                                                                    db_tt, cb_t, client_t, idx_t, schema_t, &data, my_id,
                                                                                )
                                                                                .await;
                                                                            }
                                                                            else{
                                                                                tracing::error!("have in tree {crc}");
                                                                                // client_t.chat_request(bytes::Bytes::from());
                                                                                let feed = luffa_rpc_types::Message::Feedback { crc:vec![crc], from_id:Some(my_id), to_id: Some(0), status: luffa_rpc_types::FeedbackStatus::Reach };
                                                                                let event = luffa_rpc_types::Event::new(
                                                                                    0,
                                                                                    &feed,
                                                                                    None,
                                                                                    my_id,
                                                                                );
                                                                                tracing::warn!("having>>> send feedback reach to relay");
                                                                                let event = event.encode().unwrap();
                                                                                if let Err(e) =
                                                                                    client_t.chat_request(bytes::Bytes::from(event)).await
                                                                                {
                                                                                    error!("{e:?}");
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(e)=> {
                                                                        error!("record not found {crc} error: {e:?}");
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    }
                                                    _ => {
                                                        tracing::warn!("from relay request no nonce msg Feedback>>>>{status:?}");
                                                    }
                                                },
                                                _ => {
                                                    tracing::warn!("from relay request no nonce msg>>>>{m:?}");
                                                }
                                            }
                                        }
                                    }
                                    let did = if to == my_id { from_id } else { to };
                                    let table = format!("message_{}", did);
                                    let db_t = db_t.clone();
                                    let cb = cb.clone();
                                    let client_t = client_t.clone();
                                    let idx = idx.clone();
                                    let schema_tt = schema_tt.clone();
                                    tokio::spawn(async move {
                                        if !Self::have_in_tree(db_t.clone(), crc, &table) {
                                            Self::process_event(
                                                db_t, cb, client_t, idx, schema_tt, &data, my_id,
                                            )
                                            .await;
                                        }
                                    });
                                }
                            }
                            luffa_node::ChatEvent::OutboundFailure {
                                peer,
                                request_id,
                                error,
                            } => {}
                            luffa_node::ChatEvent::InboundFailure {
                                peer,
                                request_id,
                                error,
                            } => {}
                            luffa_node::ChatEvent::ResponseSent { peer, request_id } => {}
                        }
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {
                        // TODO: a group member or my friend online?
                        tracing::debug!("Subscribed> peer_id: {peer_id:?} topic:{topic}");
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Message { message, from, id }) => {}
                    NetworkEvent::Gossipsub(GossipsubEvent::Unsubscribed { peer_id, topic }) => {

                        // TODO: a group member or my friend offline?
                    }
                    NetworkEvent::PeerConnected(peer_id) => {
                        tracing::info!("---------PeerConnected-----------{:?}", peer_id);

                        
                    }
                    NetworkEvent::PeerDisconnected(peer_id) => {
                        tracing::debug!("---------PeerDisconnected-----------{:?}", peer_id);

                        
                    }
                    NetworkEvent::CancelLookupQuery(peer_id) => {
                        tracing::debug!("---------CancelLookupQuery-----------{:?}", peer_id);
                    }
                }
            }
        });

        let db_t = db.clone();
        let pendings = VecDeque::<(Event, u32)>::new();
        let pendings = Arc::new(tokio::sync::RwLock::new(pendings));
        let pendings_t = pendings.clone();
        let cb_tt = cb_local.clone();
        let client_pending = client.clone();
        let pending_task = tokio::spawn(async move {
            loop {
                {
                    let p = pendings_t.read().await;
                    if p.is_empty() {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
                if let Some((req,count)) = {
                    let mut p = pendings_t.write().await;
                    tracing::warn!("pending size:{}",p.len());
                    p.pop_front()
                }
                {
                    let to = req.to;
                    let event_time = req.event_time;
                    let data = req.encode().unwrap();
                    match client_pending.chat_request(bytes::Bytes::from(data.clone())).await {
                        Ok(res) => {
                            let feed = Message::Feedback { crc: vec![req.crc], from_id: Some(my_id), to_id: Some(to), status: FeedbackStatus::Send };
                            let feed = serde_cbor::to_vec(&feed).unwrap();
                            cb_tt.on_message(req.crc, my_id, to, event_time,feed);
                            tracing::debug!("{res:?}");
                        }
                        Err(e) => {
                            tracing::error!("pending chat request failed [{}]: {e:?}",req.crc);

                            tokio::time::sleep(Duration::from_millis(1000)).await;
                            let status = if count >= 20 {FeedbackStatus::Failed} else {FeedbackStatus::Sending};
                            let feed = Message::Feedback { crc: vec![req.crc], from_id: Some(my_id), to_id: Some(to), status };
                            let feed = serde_cbor::to_vec(&feed).unwrap();
                            
                            cb_tt.on_message(req.crc, my_id, to,event_time, feed);
                            // if count < 20 {
                                let mut push = pendings_t.write().await;
                                push.push_back((req,count + 1));
                            // }
                        }
                    }
                }

            }
        });

        while let Some((to, msg_data, from_id, channel, k)) = receiver.recv().await {
            if to == u64::MAX {
                channel.send(Ok(u64::MAX)).unwrap();
                break;
            }
            let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
            // let is_exchane = msg.is_contacts_exchange();

            let evt = if msg.need_encrypt() {
                match k {
                    Some(key) => Some(Event::new(to, &msg, Some(key), from_id)),
                    None => {
                        tracing::warn!("----------encrypt------from [{}] to [{}]", from_id, to);
                        match Self::get_aes_key_from_contacts(db.clone(), to) {
                            Some(key) => Some(Event::new(to, &msg, Some(key), from_id)),
                            None => {
                                tracing::error!("aes key not found did:{} msg:{:?}", to, msg);
                                None
                                //    Some(Event::new(to, &msg, None, from_id))
                            }
                        }
                    }
                }
            } else {
                Some(Event::new(to, &msg, None, from_id))
            };
            match evt {
                Some(e) => {
                    let req = e.clone();
                    let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
                    let tag_key = format!("TAG-{}", to);
                    let (tag, msg_type) = match tree.get(&tag_key.as_bytes()) {
                        Ok(Some(v)) => {
                            let tp = match Self::get_contacts_type(db.clone(), to) {
                                Some(tp) => match tp {
                                    ContactsTypes::Private => {
                                        format!("content_private")
                                    }
                                    _ => {
                                        format!("content_group")
                                    }
                                },
                                None => {
                                    format!("content_private")
                                }
                            };
                            (String::from_utf8(v.to_vec()).unwrap(), tp)
                        }
                        _ => (String::new(), format!("exchange")),
                    };
                    let save_job =
                    if to > 0 {
                        let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
                        let data = e.encode().unwrap();
                        let Event {
                            to,
                            event_time,
                            crc,
                            from_id,
                            nonce,
                            ..
                        } = e;
                        // assert!(nonce.is_some(),"nonce is none!!");
                        if let Err(e) = channel.send(Ok(crc)) {
                            tracing::info!("channel send failed {e:?}");
                        }
                        let db_t = db_t.clone();
                        let schema_t = schema_t.clone();
                        let idx_t = idx_t.clone();
                        let job =
                        tokio::spawn(async move {
                            let table = { format!("message_{to}") };
                            tracing::info!("send......");
                            let mut will_save = false;
                            match msg {
                                Message::Chat { content } => {
                                    // TODO index content search engine
                                    match content {
                                        ChatContent::Burn { crc, expires } => {
                                            let p_table = format!("message_{to}");
                                            Self::burn_from_tree(db_t.clone(), crc, p_table);

                                            let fld_crc = schema_t.get_field("crc").unwrap();
                                            let mut wr = idx_t.write();
                                            let del = Term::from_field_u64(fld_crc, crc);
                                            wr.delete_term(del);
                                            wr.commit().unwrap();
                                        }
                                        ChatContent::Send { data } => {
                                            let (title, body) = match data {
                                                luffa_rpc_types::ContentData::Text {
                                                    source,
                                                    reference,
                                                } => match source {
                                                    luffa_rpc_types::DataSource::Cid { cid } => {
                                                        let t = multibase::encode(
                                                            multibase::Base::Base58Btc,
                                                            cid,
                                                        );
                                                        let body = format!("{:?}", reference);
                                                        (t, body)
                                                    }
                                                    luffa_rpc_types::DataSource::Raw { data } => {
                                                        let t = format!("");
                                                        let body = multibase::encode(
                                                            multibase::Base::Base64,
                                                            data,
                                                        );
                                                        (t, body)
                                                    }
                                                    luffa_rpc_types::DataSource::Text {
                                                        content,
                                                    } => {
                                                        let t = format!("");

                                                        (t, content)
                                                    }
                                                },
                                                luffa_rpc_types::ContentData::Link {
                                                    txt,
                                                    url,
                                                    reference,
                                                } => (txt, url),
                                                luffa_rpc_types::ContentData::Media {
                                                    title,
                                                    m_type,
                                                    source,
                                                } => (title, format!("{:?}", m_type)),
                                            };
                                            Self::update_session(db_t.clone(), to, None, Some(crc), Some(crc), Some(body.clone()), None,event_time);
                                            let schema = schema_t.clone();
                                            let fld_crc = schema.get_field("crc").unwrap();
                                            let fld_from = schema.get_field("from_id").unwrap();
                                            let fld_to = schema.get_field("to_id").unwrap();
                                            let fld_time = schema.get_field("event_time").unwrap();
                                            let fld_title = schema.get_field("title").unwrap();
                                            let fld_body = schema.get_field("body").unwrap();
                                            let fld_type = schema.get_field("msg_type").unwrap();
                                            let doc = doc!(
                                                fld_crc => crc,
                                                fld_from => from_id,
                                                fld_to => to,
                                                fld_time => event_time,
                                                fld_type => msg_type,
                                                fld_title => title,
                                                fld_body => body,
                                            );
                                            let mut wr = idx_t.write();
                                            wr.add_document(doc).unwrap();
                                            wr.commit().unwrap();
                                            will_save = true;
                                        }
                                        _ => {}
                                    }
                                }
                                Message::WebRtc { .. }=>{
                                    will_save = true;
                                }
                                _ => {}
                            }
                            if will_save && nonce.is_some() {
                                Self::save_to_tree(
                                    db_t.clone(),
                                    e.crc,
                                    &table,
                                    data.clone(),
                                    event_time,
                                );
                            }
                        });
                        Some(job)
                    } else {
                        if let Err(_e) = channel.send(Ok(0)) {
                            tracing::error!("channel send failed");
                        }
                        None
                    };
                    if to != my_id {
                        let client = client.clone();
                        
                        let event_time = req.event_time;
                        let pendings_t = pendings.clone();
                        let cb_t = cb_local.clone();
                        if req.nonce.is_some() {

                            let sending = Message::Feedback { crc: vec![req.crc], from_id: Some(my_id), to_id: Some(to), status: FeedbackStatus::Sending };
                            let sending = serde_cbor::to_vec(&sending).unwrap();
                            cb_t.on_message(req.crc, my_id, to,event_time, sending);
                        }
                        tokio::spawn(async move {
                            let data = req.encode().unwrap();
                            match client.chat_request(bytes::Bytes::from(data.clone())).await {
                                Ok(res) => {
                                    if let Some(job) = save_job {
                                        if let Err(e) = job.await {
                                            tracing::error!("job>> {e:?}");
                                        }
                                    }
                                    if req.nonce.is_some() {
                                        let feed = Message::Feedback { crc: vec![req.crc], from_id: Some(my_id), to_id: Some(to), status: FeedbackStatus::Send };
                                        let feed = serde_cbor::to_vec(&feed).unwrap();
                                        cb_t.on_message(req.crc, my_id, to,event_time, feed);
                                    }
                                    tracing::debug!("{res:?}");
                                    
                                }
                                Err(e) => {
                                    tracing::error!("chat request failed [{}]: {e:?}",req.crc);
                                    if req.nonce.is_some() {
                                        if let Some(job) = save_job {
                                            if let Err(e) = job.await {
                                                tracing::error!("job>> {e:?}");
                                            }
                                        }
                                        tokio::time::sleep(Duration::from_millis(1000)).await;
                                        if req.nonce.is_some() {
                                            let feed = Message::Feedback { crc: vec![req.crc], from_id: Some(my_id), to_id: Some(to), status: FeedbackStatus::Sending };
                                            let feed = serde_cbor::to_vec(&feed).unwrap();
                                            cb_t.on_message(req.crc, my_id, to,event_time, feed);
                                        }
                                        let mut push = pendings_t.write().await;
                                        push.push_back((req,1));

                                    }
                                    
                                }
                            }
                        });
                    } else {
                        tracing::error!("is to me---->");
                    }
                }
                None => {
                    if let Err(_e) = channel.send(Ok(0)) {
                        tracing::error!("channel send failed");
                    }
                }
            }
        }

        // luffa_util::block_until_sigint().await;
        // ctl.await.unwrap();
        sync_task.abort();
        process.abort();
        p2p_rpc.abort();
        pending_task.abort();
        // metrics_handle.shutdown();
    }

    fn extra_content(data: &ContentData) -> (String, String) {
        match data {
            ContentData::Text { source, reference } => match source {
                luffa_rpc_types::DataSource::Cid { cid } => {
                    let t = multibase::encode(multibase::Base::Base58Btc, cid);
                    let body = format!("");
                    (t, body)
                }
                luffa_rpc_types::DataSource::Raw { data } => {
                    let t = format!("");
                    let body = multibase::encode(multibase::Base::Base64, data);
                    (t, body)
                }
                luffa_rpc_types::DataSource::Text { content } => {
                    let t = format!("");

                    (t, content.clone())
                }
            },
            ContentData::Link {
                txt,
                url,
                reference,
            } => (txt.clone(), url.clone()),
            ContentData::Media {
                title,
                m_type,
                source,
            } => (title.clone(), format!("")),
        }
    }

    async fn process_event(
        db_t: Arc<Db>,
        cb: Arc<Box<dyn Callback>>,
        client_t: P2pClient,
        idx: Arc<RwLock<IndexWriter>>,
        schema: Schema,
        data: &Vec<u8>,
        my_id: u64,
    ) {
        if let Ok(im) = Event::decode_uncheck(&data) {
            let Event {
                msg,
                nonce,
                to,
                from_id,
                crc,
                event_time,
                ..
            } = im.clone();
            if to != my_id && from_id != my_id {
                tracing::warn!("not to my id:{to} or {from_id}");
                return;
            }
            
            let did = if to == my_id { from_id }  else  { to };
            
            if nonce.is_none() {
                tokio::spawn(async move {
                    cb.on_message(crc, from_id, to, event_time,msg);
                });
                return;
            }
            if from_id == my_id {
                tracing::error!("from_id == my_id   crc:{crc}");
            }
            let feed = luffa_rpc_types::Message::Feedback { crc:vec![crc], from_id:Some(my_id), to_id: Some(0), status: luffa_rpc_types::FeedbackStatus::Reach };
            let event = luffa_rpc_types::Event::new(
                0,
                &feed,
                None,
                my_id,
            );
            tracing::info!("send feedback reach to relay");
            let event = event.encode().unwrap();
            if let Err(e) =
                client_t.chat_request(bytes::Bytes::from(event)).await
            {
                error!("{e:?}");
            }
            let evt_data = data.clone();
            match Self::get_aes_key_from_contacts(db_t.clone(), did) {
                Some(key) => {
                    if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                        bytes::Bytes::from(msg),
                        Some(key.clone()),
                        nonce.clone(),
                    ) {
                        // TODO: did is me or I'm a member any local group
                        let msg_data = serde_cbor::to_vec(&msg).unwrap();
                        tracing::info!("from relay request e2e crc:[{crc}] msg>>>>{msg:?}");
                        let mut will_save = false;
                        match msg {
                            Message::Chat { content } => {
                                // TODO index content search engine
                                match content {
                                    ChatContent::Burn { crc, .. } => {
                                        let table = format!("message_{did}");
                                        Self::burn_from_tree(db_t.clone(), crc, table);

                                        let fld_crc = schema.get_field("crc").unwrap();
                                        let mut wr = idx.write();
                                        let del = Term::from_field_u64(fld_crc, crc);
                                        wr.delete_term(del);
                                        wr.commit().unwrap();
                                    }
                                    ChatContent::Send { data } => {
                                        will_save = true;
                                        let feed = luffa_rpc_types::Message::Chat {
                                            content: ChatContent::Feedback {
                                                crc,
                                                status: luffa_rpc_types::FeedbackStatus::Reach,
                                            },
                                        };
                                        let event = luffa_rpc_types::Event::new(
                                            did,
                                            &feed,
                                            Some(key),
                                            my_id,
                                        );
                                        tracing::error!("send feedback reach to {from_id}");
                                        let event = event.encode().unwrap();
                                        if let Err(e) =
                                            client_t.chat_request(bytes::Bytes::from(event)).await
                                        {
                                            error!("{e:?}");
                                        }

                                        let (title, body) = match data {
                                            luffa_rpc_types::ContentData::Text {
                                                source,
                                                reference,
                                            } => match source {
                                                luffa_rpc_types::DataSource::Cid { cid } => {
                                                    let t = multibase::encode(
                                                        multibase::Base::Base58Btc,
                                                        cid,
                                                    );
                                                    let body = format!("");
                                                    (t, body)
                                                }
                                                luffa_rpc_types::DataSource::Raw { data } => {
                                                    let t = format!("");
                                                    let body = multibase::encode(
                                                        multibase::Base::Base64,
                                                        data,
                                                    );
                                                    (t, body)
                                                }
                                                luffa_rpc_types::DataSource::Text { content } => {
                                                    let t = format!("");

                                                    (t, content)
                                                }
                                            },
                                            luffa_rpc_types::ContentData::Link {
                                                txt,
                                                url,
                                                reference,
                                            } => (txt, url),
                                            luffa_rpc_types::ContentData::Media {
                                                title,
                                                m_type,
                                                source,
                                            } => (title, format!("")),
                                        };
                                        // let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
                                        let (msg_type, did) = if to == my_id {
                                            // let tag_key_private = format!("TAG-0-{}", from_id);
                                            (format!("content_private"), from_id)
                                        } else {
                                            // let tag_key_group = format!("TAG-1-{}", to);
                                            // let tag =
                                            // match tree.get(&tag_key_group) {
                                            //     Ok(Some(v))=>{
                                            //         Some(String::from_utf8(v.to_vec()).unwrap())
                                            //     }
                                            //     _=>{
                                            //         None
                                            //     }
                                            // };
                                            (format!("content_group"), to)
                                        };

                                        Self::update_session(
                                            db_t.clone(),
                                            did,
                                            None,
                                            None,
                                            Some(crc),
                                            Some(body.clone()),
                                            Some(4),
                                            event_time,
                                        );
                                        let fld_crc = schema.get_field("crc").unwrap();
                                        let fld_from = schema.get_field("from_id").unwrap();
                                        let fld_to = schema.get_field("to_id").unwrap();
                                        let fld_time = schema.get_field("event_time").unwrap();
                                        let fld_title = schema.get_field("title").unwrap();
                                        let fld_body = schema.get_field("body").unwrap();
                                        let fld_type = schema.get_field("msg_type").unwrap();
                                        let doc = doc!(
                                            fld_crc => crc,
                                            fld_from => from_id,
                                            fld_to => to,
                                            fld_time => event_time,
                                            fld_type => msg_type,
                                            fld_title => title,
                                            fld_body => body,
                                        );
                                        let mut wr = idx.write();
                                        wr.add_document(doc).unwrap();
                                        wr.commit().unwrap();
                                    }
                                    ChatContent::Feedback { crc, status } => match status {
                                        FeedbackStatus::Reach => {
                                            let table = format!("message_{did}");
                                            Self::save_to_tree_status(db_t.clone(),crc,&table,4);
                                            Self::update_session(
                                                db_t.clone(),
                                                did,
                                                None,
                                                None,
                                                None,
                                                None,
                                                Some(4),
                                                event_time,
                                            );
                                        }
                                        FeedbackStatus::Read => {
                                            let table = format!("message_{did}");
                                            Self::save_to_tree_status(db_t.clone(),crc,&table,5);
                                            Self::update_session(
                                                db_t.clone(),
                                                did,
                                                None,
                                                None,
                                                None,
                                                None,
                                                Some(5),
                                                event_time,
                                            );
                                           
                                        }
                                        _ => {
                                           
                                        }
                                    }
                                }
                            }
                            Message::WebRtc { .. } => {
                               will_save = true;
                            }
                            Message::ContactsExchange { exchange } => {
                                will_save = true;
                                let token = match exchange {
                                    ContactsEvent::Answer { token } => {
                                        tracing::error!("G> Answer>>>>>{token:?}");
                                        Self::update_offer_status(db_t.clone(),did, crc, OfferStatus::Answer);
                                        token
                                    }
                                    ContactsEvent::Offer { mut token } => {
                                        tracing::error!("G> Offer>>>>>{token:?}");
                                        // token.validate()
                                        Self::update_offer_status(db_t.clone(),did, crc, OfferStatus::Offer);
                                        let pk =
                                            PublicKey::from_protobuf_encoding(&token.public_key)
                                                .unwrap();
                                        let peer = PeerId::from_public_key(&pk);
                                        let mut digest = crc64fast::Digest::new();
                                        digest.write(&peer.to_bytes());
                                        let did = digest.sum64();
                                        if let Some(key) =
                                            Self::get_contacts_skey(db_t.clone(), did)
                                        {
                                            tracing::info!("change contacts to old s key");
                                            token.secret_key = key;
                                        }
                                        token
                                    }
                                    ContactsEvent::Reject {crc,public_key} =>{
                                        if let Ok(pk) = PublicKey::from_protobuf_encoding(&public_key) {
                                            let peer = PeerId::from_public_key(&pk);
                                            let mut digest = crc64fast::Digest::new();
                                            digest.write(&peer.to_bytes());
                                            let did = digest.sum64();
                                            let table = format!("message_{did}");
                                            Self::save_to_tree(
                                                db_t.clone(),
                                                crc,
                                                &table,
                                                evt_data.clone(),
                                                event_time,
                                            );
                                            Self::update_offer_status(db_t.clone(), did,crc, OfferStatus::Reject);
                                        }
                                        return;
                                    }
                                };

                                let pk =
                                    PublicKey::from_protobuf_encoding(&token.public_key).unwrap();
                                let peer = PeerId::from_public_key(&pk);
                                let mut digest = crc64fast::Digest::new();
                                digest.write(&peer.to_bytes());
                                let did = digest.sum64();

                                let offer_key = &token.secret_key;

                                // add a pending answer for this offer which is received
                                
                                let comment = token.comment.clone();
                                Self::offer_or_answer(
                                    crc,
                                    from_id,
                                    offer_key.clone(),
                                    did,
                                    event_time,
                                    idx.clone(),
                                    schema.clone(),
                                    token,
                                    db_t.clone(),
                                    client_t.clone(),
                                )
                                .await;

                                Self::update_session(
                                    db_t.clone(),
                                    did,
                                    comment.clone(),
                                    None,
                                    None,
                                    None,
                                    None,
                                    event_time,
                                );
                            }
                            _ => {}
                        }
                        if will_save {
                            let table = format!("message_{did}");
                            Self::save_to_tree(
                                db_t.clone(),
                                crc,
                                &table,
                                evt_data.clone(),
                                event_time,
                            );
                        }
                        tokio::spawn(async move {
                            cb.on_message(crc, from_id, to,event_time, msg_data);
                        });
                    } else {
                        eprintln!("decrypt failes!!! {:?}", nonce);
                    }
                }
                None => {
                    tracing::info!("aes not in contacts {did}");
                    // warn!("Gossipsub> peer_id: {from:?} nonce:{:?}", nonce);
                    if let Some(key) = Self::get_offer_by_offer_id(db_t.clone(), from_id) {
                        tracing::info!("offer is:{}  nonce:{:?} key: {:?}", from_id, nonce, key);
                        if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                            bytes::Bytes::from(msg),
                            Some(key.clone()),
                            nonce,
                        ) {
                            let msg_t = msg.clone();
                            let msg_data = serde_cbor::to_vec(&msg).unwrap();
                            let offer_id = from_id;
                            let offer_key = key.clone();
                            match msg_t {
                                Message::ContactsExchange { exchange } => {
                                    let (token, is_answer) = match exchange {
                                        ContactsEvent::Answer { token } => {
                                            tracing::info!("P>Answer>>>>>{token:?}");
                                            Self::update_offer_status(db_t.clone(),did, crc, OfferStatus::Answer);
                                            (token, true)
                                        }
                                        ContactsEvent::Offer { mut token } => {
                                            tracing::info!("Offer>>>>>{token:?}");
                                            // Self::update_offer_status(db_t.clone(), crc, OfferStatus::Offer);
                                            Self::save_offer_to_tree(db_t.clone(),crc,offer_id,offer_key.clone(),OfferStatus::Offer,event_time);
                                            // token.validate()
                                            let pk = PublicKey::from_protobuf_encoding(
                                                &token.public_key,
                                            )
                                            .unwrap();
                                            let peer = PeerId::from_public_key(&pk);
                                            let mut digest = crc64fast::Digest::new();
                                            digest.write(&peer.to_bytes());
                                            let did = digest.sum64();
                                            if let Some(key) =
                                                Self::get_contacts_skey(db_t.clone(), did)
                                            {
                                                tracing::info!("change contacts to old s key");
                                                token.secret_key = key;
                                            }

                                            (token, false)
                                        }
                                        ContactsEvent::Reject {crc,public_key} =>{
                                            if let Ok(pk) = PublicKey::from_protobuf_encoding(&public_key) {
                                                let peer = PeerId::from_public_key(&pk);
                                                let mut digest = crc64fast::Digest::new();
                                                digest.write(&peer.to_bytes());
                                                let did = digest.sum64();
                                                Self::update_offer_status(db_t.clone(),did, crc, OfferStatus::Reject);
                                                let table = format!("message_{did}");
                                                Self::save_to_tree(
                                                    db_t.clone(),
                                                    crc,
                                                    &table,
                                                    evt_data.clone(),
                                                    event_time,
                                                );
                                            }
                                            return;
                                        }
                                    };
                                    let pk = PublicKey::from_protobuf_encoding(&token.public_key)
                                        .unwrap();
                                    let peer = PeerId::from_public_key(&pk);
                                    let mut digest = crc64fast::Digest::new();
                                    digest.write(&peer.to_bytes());
                                    let did = digest.sum64();
                                   
                                    // add a pending answer for this offer which is received
                                    
                                    let comment = token.comment.clone();
                                    Self::offer_or_answer(
                                        crc,
                                        offer_id,
                                        offer_key,
                                        to,
                                        event_time,
                                        idx.clone(),
                                        schema.clone(),
                                        token,
                                        db_t.clone(),
                                        client_t.clone(),
                                    )
                                    .await;
                                    if is_answer {
                                        Self::update_session(
                                            db_t.clone(),
                                            did,
                                            comment.clone(),
                                            None,
                                            None,
                                            None,
                                            None,
                                            event_time,
                                        );
                                    }
                                    let table = format!("message_{did}");
                                    Self::save_to_tree(
                                        db_t.clone(),
                                        crc,
                                        &table,
                                        evt_data.clone(),
                                        event_time,
                                    );
                                }
                                Message::Chat { content }=>{
                                    match content {
                                        ChatContent::Feedback { crc, status }=>{
                                            tracing::warn!("from offer Feedback crc<{crc}> {status:?}");
                                        }
                                        _=>{
                                            tracing::error!("from offer content {content:?}");
                                        }
                                    }
                                }
                                _ => {
                                    tracing::error!("from offer msg {msg:?}");
                                }
                            }
                            tokio::spawn(async move {
                                cb.on_message(crc, from_id, to, event_time,msg_data);
                            });
                        } else {
                            tracing::error!("decrypt failed:>>>>");
                        }
                    } else {
                        tracing::error!("invalid msg {im:?}");
                    }
                }
            }
        } // todo!()
    }

    async fn offer_or_answer(
        crc: u64,
        offer_id: u64,
        offer_key: Vec<u8>,
        to: u64,
        event_time: u64,
        idx: Arc<RwLock<IndexWriter>>,
        schema: Schema,
        token: ContactsToken,
        db_tt: Arc<Db>,
        client_t: P2pClient,
    ) {
        let ContactsToken {
            public_key,
            sign,
            secret_key,
            contacts_type,
            comment,
            group_key,

            ..
        } = token;
        let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
        let peer = PeerId::from_public_key(&pk);
        let mut digest = crc64fast::Digest::new();
        digest.write(&peer.to_bytes());
        let from_id = digest.sum64();
        Self::save_contacts(
            db_tt.clone(),
            from_id,
            secret_key.clone(),
            public_key.clone(),
            contacts_type,
            sign.clone(),
            comment.clone(),
            group_key
        );

        let msg_type = match contacts_type {
            ContactsTypes::Private => {
                format!("contacts_private")
            }
            ContactsTypes::Group => {
                format!("contacts_group")
            }
        };
        let fld_crc = schema.get_field("crc").unwrap();
        let fld_from = schema.get_field("from_id").unwrap();
        let fld_to = schema.get_field("to_id").unwrap();
        let fld_time = schema.get_field("event_time").unwrap();
        let fld_title = schema.get_field("title").unwrap();
        let fld_body = schema.get_field("body").unwrap();
        let fld_type = schema.get_field("msg_type").unwrap();
        let title = multibase::encode(multibase::Base::Base58Btc, public_key);
        let body = format!("{}", comment.clone().unwrap_or_default());
        let doc = doc!(
            fld_crc => crc,
            fld_from => from_id,
            fld_to => to,
            fld_time => event_time,
            fld_type => msg_type,
            fld_title => title,
            fld_body => body,
        );
        let mut wr = idx.write();
        wr.add_document(doc).unwrap();
        wr.commit().unwrap();
        let msg = luffa_rpc_types::Message::Chat {
            content: ChatContent::Feedback {
                crc,
                status: luffa_rpc_types::FeedbackStatus::Reach,
            },
        };

        let event = luffa_rpc_types::Event::new(from_id, &msg, Some(offer_key), offer_id);
        let event = event.encode().unwrap();
        tokio::spawn(async move {
            if let Err(e) = client_t.chat_request(bytes::Bytes::from(event)).await {
                error!("{e:?}");
            }
        });
    }

    pub fn enable_silent(&self, did: u64) -> ClientResult<()> {
        Self::enable_session_silent(self.db(), did);
        Ok(())
    }

    pub fn disable_silent(&self, did: u64) -> ClientResult<()> {
        Self::disable_session_silent(self.db(), did);
        Ok(())
    }
    fn db(&self) -> Arc<Db> {
        self.db.as_ref().unwrap().read().clone()
    }
    fn key_db(&self) -> Arc<Db> {
        self.key_db.as_ref().unwrap().read().clone()
    }
}
/// Starts a new p2p node, using the given mem rpc channel.
pub async fn start_node(
    config: luffa_node::config::Config,
    keychain: Keychain<DiskStorage>,
    db: Arc<luffa_store::Store>,
    filter: Option<KeyFilter>,
) -> anyhow::Result<(
    PeerId,
    JoinHandle<()>,
    Receiver<NetworkEvent>,
    Sender<luffa_node::rpc::RpcMessage>,
)> {
    tracing::info!("node>>>{config:?}");
    let (mut p2p, sender) =
        Node::new(config, keychain, db, Some("Luffa".to_string()), filter).await?;
    let events = p2p.network_events();
    let local_id = p2p.local_peer_id().clone();
    // Start services
    let p2p_task = task::spawn(async move {
        tracing::info!("p2p runnung..");
        if let Err(err) = p2p.run().await {
            error!("{:?}", err);
            tracing::info!("{err:?}");
        } else {
            tracing::info!("p2p run exit!")
        }
    });
    Ok((local_id, p2p_task, events, sender))
}

/// Starts a new store, using the given mem rpc channel.
pub async fn start_store(config: StoreConfig) -> anyhow::Result<luffa_store::Store> {
    // This is the file RocksDB itself is looking for to determine if the database already
    // exists or not.  Just knowing the directory exists does not mean the database is
    // created.
    // let marker = config.path.join("CURRENT");

    tracing::info!("Opening store at {}", config.path.display());
    let store = Store::create(config)
        .await
        .context("failed to open existing store")?;
    Ok(store)
}

include!(concat!(env!("OUT_DIR"), "/luffa_sdk.uniffi.rs"));
