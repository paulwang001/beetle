#[macro_use]
extern crate lazy_static;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::{
    aead::{KeyInit, OsRng},
    Aes256Gcm, // Or `Aes128Gcm`
};
use anyhow::Context;
use api::P2pClient;
use futures::StreamExt;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use libp2p::{gossipsub::TopicHash, identity::Keypair};
use luffa_node::{
    load_identity, DiskStorage, GossipsubEvent, Keychain, NetworkEvent, Node, ENV_PREFIX, KeyFilter,
};
use luffa_rpc_types::{message_from, ChatContent, ContactsTypes, Contacts, message_to, ContentData, FeedbackStatus};
use luffa_rpc_types::{AppStatus, ContactsEvent, ContactsToken, Event, Message};
use luffa_store::{Config as StoreConfig, Store};
use luffa_util::{luffa_config_path, make_config};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info, warn, debug};

use tantivy::collector::TopDocs;
use tantivy::directory::{ManagedDirectory, MmapDirectory};
use tantivy::{doc, Score, DocAddress};
use tantivy::query::QueryParser;
use tantivy::Index;
use tantivy::ReloadPolicy;
use tantivy::{schema::*, IndexWriter};
use anyhow::Result;
use chrono::Utc;
use tokio::sync::oneshot::{Sender as ShotSender};

mod api;
mod config;

use crate::config::Config;

const TOPIC_STATUS: &str = "luffa_status";
// const TOPIC_CONTACTS: &str = "luffa_contacts";
// const TOPIC_CHAT: &str = "luffa_chat";
// const TOPIC_CHAT_PRIVATE: &str = "luffa_chat_private";
// const TOPIC_CONTACTS_SCAN: &str = "luffa_contacts_scan_answer";
const CONFIG_FILE_NAME: &str = "luffa.config.toml";
const KVDB_CONTACTS_FILE: &str = "contacts";
const KVDB_CONTACTS_TREE: &str = "luffa_contacts";
const KVDB_CHAT_SESSION_TREE: &str = "luffa_sessions";
const TOPIC_RELAY: &str = "luffa_relay";
const LUFFA_CONTENT: &str = "index_data";

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = create_runtime();
}

#[derive(Debug, Serialize, Deserialize,Clone)]
pub struct ChatSession {
    pub did: u64,
    pub session_type:u8,
    pub last_time: u64,
    pub tag: String,
    pub read_crc: u64,
    pub reach_crc: Vec<u64>,
    pub last_msg: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContactsView {
    pub did: u64,
    pub tag: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventMeta {
    pub from_id: u64,
    pub to_id: u64,
    pub from_tag: String,
    pub to_tag: String,
    pub event_time:u64,
    pub msg:Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Contancts parse error")]
    CodeParser,
    #[error("Send message failed")]
    SendFailed,
    #[error("Client start failed")]
    StartFailed,
    #[error("Search error")]
    SearchError,
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

pub trait Callback: Send + Sync + Debug {
    fn on_message(&self, crc: u64, from_id: u64, to: u64, msg: Vec<u8>);
}

pub fn public_key_to_id(public_key: Vec<u8>)-> u64 {
    let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
    let peer = PeerId::from_public_key(&pk);
    let mut digest = crc64fast::Digest::new();
    digest.write(&peer.to_bytes());
    let to = digest.sum64();
    to
}

fn content_index(idx_path: &Path) -> (Index, Schema) {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("crc", INDEXED | STORED);
    schema_builder.add_u64_field("from_id", INDEXED | STORED);
    schema_builder.add_u64_field("to_id", INDEXED | STORED);
    schema_builder.add_u64_field("event_time", INDEXED | STORED);
    schema_builder.add_text_field("msg_type", STRING | STORED);
    schema_builder.add_text_field("title", TEXT);
    schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();
    std::fs::create_dir_all(idx_path).unwrap();
    // Indexing documents
    let dir = ManagedDirectory::wrap(Box::new(MmapDirectory::open(idx_path).unwrap())).unwrap();
    let ta = TextAnalyzer::from(SimpleTokenizer)
        .filter(LowerCaser)
        .filter(Stemmer::new(tantivy::tokenizer::Language::English));
    let index = Index::open_or_create(dir, schema.clone()).unwrap();
    index.tokenizers().register("term", ta);
    (index, schema)
}

#[derive(Clone)]
pub struct Client {
    sender: Arc<RwLock<Option<tokio::sync::mpsc::Sender<(u64, Vec<u8>, u64,ShotSender<Result<u64>>,Option<Vec<u8>>)>>>>,
    key: Arc<RwLock<Option<Keychain<DiskStorage>>>>,
    db: Arc<Db>,
    client: Arc<RwLock<Option<Arc<P2pClient>>>>,
    idx: Arc<Index>,
    schema: Schema,
    writer: Arc<RwLock<IndexWriter>>,
    filter:Arc<RwLock<Option<KeyFilter>>>,
    config:Arc<RwLock<Option<Config>>>,
}

impl Client {
    pub fn new() -> Self {
        let path = luffa_util::luffa_data_path(KVDB_CONTACTS_FILE).unwrap();
        let idx_path = luffa_util::luffa_data_path(LUFFA_CONTENT).unwrap();
        tracing::info!("open db>>>>{:?}", &path);
        let db = Arc::new(sled::open(path).unwrap());
        let (idx, schema) = content_index(idx_path.as_path());
        let writer = idx.writer_with_num_threads(1, 12 * 1024 * 1024).unwrap();
        Client {
            key: Arc::new(RwLock::new(None)),
            filter: Arc::new(RwLock::new(None)),
            config: Arc::new(RwLock::new(None)),
            sender: Arc::new(RwLock::new(None)),
            db,
            client: Arc::new(RwLock::new(None)),
            idx: Arc::new(idx),
            schema,
            writer: Arc::new(RwLock::new(writer)),
        }
    }

    async fn get_keypair(&self,filter:Option<KeyFilter>) -> Option<Keypair> {
        let mut keychain = self.key.write().await;
        let chain = keychain.as_mut().unwrap();
        match filter {
            Some(KeyFilter::Phrase(phrase,password))=>{
                if let Ok(key) = chain.create_ed25519_key_from_seed(&phrase, &password).await {
                    let key:Keypair = key.into();
                    return Some(key)
                }
            }
            Some(KeyFilter::Name(name))=>{
                while let Some(key) = chain.keys().next().await {
                    if let Ok(key) = key {
                        if key.name() == name {
                            let key:Keypair = key.into();
                            return Some(key)
                        }
                    }
                }
            }
            _=>{
                while let Some(key) = chain.keys().next().await {
                    if let Ok(key) = key {
                        let key:Keypair = key.into();
                        return Some(key)
                    }
                }
            }
        }
        match chain.create_ed25519_key_bip39("", true).await {
            Ok((_p,key))=>{
                let key:Keypair = key.into();
                Some(key)
            }
            Err(e)=>{
                None
            }
        }
    }
    /// show code
    pub fn show_code(&self) -> Option<String> {
        if let Some(uid)= self.get_did() {
            let s_key = Aes256Gcm::generate_key(&mut OsRng);
            let secret_key = s_key.to_vec();
            let mut digest = crc64fast::Digest::new();
            digest.write(&secret_key);
            let offer_id = digest.sum64();
            tracing::warn!("gen offer id:{}",offer_id);
            self.save_contacts_offer(offer_id, secret_key.clone());
            let my_id = self.get_local_id().unwrap();
            let tag = self.find_contacts_tag(my_id).unwrap_or_default();
            Some(format!("luffa://{}/{}/{}",uid,bs58::encode(secret_key).into_string(),tag))
        }
        else{
            None
        }
    }

    ///Offer contacts 
    pub fn contacts_offer(&self,code:&String) ->u64{
        let mut tmp = code.split('/');
        let _from_tag = tmp.next_back();
        let key = tmp.next_back();
        let uid = tmp.next_back();
        let secret_key = key.unwrap();
        let mut to = [0u8;8];
        to.clone_from_slice(&bs58::decode(uid.unwrap()).into_vec().unwrap());
        let to = u64::from_be_bytes(to);
        let my_id = self.get_local_id().unwrap();
        let tag = self.find_contacts_tag(my_id);
        let key = bs58::decode(secret_key).into_vec().unwrap();

        let mut digest = crc64fast::Digest::new();
        digest.write(&key);
        let from_id = digest.sum64();

        let secret_key = key.to_vec();
        let mut digest = crc64fast::Digest::new();
        digest.write(&secret_key);
        let offer_id = digest.sum64();
        let msg =
        {
            let offer_key = Aes256Gcm::generate_key(&mut OsRng);
            let offer_key = offer_key.to_vec();
            tracing::warn!("secret_key::: {}",secret_key.len());
            let token = RUNTIME.block_on(async {
                let filter = self.filter.read().await;
                let key = self.get_keypair(filter.clone()).await;
                let token = key.as_ref().map(|key| {
                    ContactsToken::new(
                        key,
                        tag,
                        offer_key.clone(),
                        luffa_rpc_types::ContactsTypes::Private,
                    )
                    .unwrap()
                });
                token.unwrap()
            });
            
            tracing::warn!("gen offer id:{}",offer_id);
            self.save_contacts_offer(offer_id, secret_key);
            
            Message::ContactsExchange {
                exchange: ContactsEvent::Offer { token },
            }
        };
        
        match self.send_to(to, msg, from_id,Some(key)).map_err(|e| {
            tracing::warn!("send_to failed:{e:?}");
            ClientError::SendFailed
        })
        {
            Ok(crc)=>{
                let table = format!("offer_{my_id}");
                let status = vec![0u8;1];
                let now = Utc::now().timestamp_millis() as u64;
                Self::save_to_tree(
                    self.db.clone(),
                    offer_id,
                    &table,
                    status,
                    now,
                ); 
                crc
            }
            Err(_)=>{
                0
            }
        }
        
    } 

    /// answer an offer and send it to from 
    pub fn contacts_anwser(&self, to:u64,offer_id: u64,secret_key:Vec<u8>) -> u64{
        let offer_key = Self::get_offer_by_offer_id(self.db.clone(), offer_id);
        
        let my_id = self.get_local_id().unwrap();
        let comment = self.find_contacts_tag(my_id);
        tracing::warn!("secret_key::: {}",secret_key.len());
        let new_key =
        match self.get_secret_key(to) {
            Some(key)=> {
                tracing::warn!("contacts old s key");
                key
            },
            None=> secret_key.clone()
        };
        let token = RUNTIME.block_on(async {
            let filter = self.filter.read().await;
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

        match self.send_to(to, msg, offer_id,offer_key).map_err(|e| {
            tracing::warn!("send_to failed:{e:?}");
            ClientError::SendFailed
        }){
            Ok(crc)=>{
                let table = 
                format!("offer_{my_id}");
                let status = vec![11u8;1];
                let now = Utc::now().timestamp_millis() as u64;
                Self::save_to_tree(
                    self.db.clone(),
                    offer_id,
                    &table,
                    status,
                    now,
                );
                crc
            }
            Err(_)=>{
                0
            }
        }
        
    }

    fn save_contacts(
        db: Arc<Db>,
        to: u64,
        secret_key: Vec<u8>,
        public_key: Vec<u8>,
        c_type:ContactsTypes,
        sign: Vec<u8>,
        comment: Option<String>,
    ) {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        tracing::error!("save_contacts----->{to}");
        let s_key = format!("SKEY-{}", to);
        let p_key = format!("PKEY-{}", to);
        let sig_key = format!("SIG-{}", to);
        let tag_key = format!("TAG-{}", to);
        let type_key = format!("TYPE-{}", to);
        tree.insert(s_key.as_bytes(), secret_key).unwrap();
        tree.insert(p_key.as_bytes(), public_key).unwrap();
        tree.insert(sig_key.as_bytes(), sign).unwrap();
        tree.insert(type_key.as_bytes(), vec![c_type as u8]).unwrap();
        tree.insert(tag_key.as_bytes(), comment.unwrap_or(format!("{to}")).as_bytes())
            .unwrap();
        tree.flush().unwrap();
    }
    fn save_to_tree(db: Arc<Db>, crc: u64, table: &str, data: Vec<u8>,event_time:u64) {
        let tree = db.open_tree(&table).unwrap();
        
        match tree.insert(crc.to_be_bytes(), data) {
            Ok(None)=>{
                let tree_time = db.open_tree(&format!("{table}_time")).unwrap();
                tree_time.insert(event_time.to_be_bytes(), crc.to_be_bytes().to_vec()).unwrap();
                tree_time.flush().unwrap();
            }
            _=>{

            }
        }

        tree.flush().unwrap();
    }
    
    fn have_in_tree(db: Arc<Db>, crc: u64, table: &str)-> bool {
        let tree = db.open_tree(table).unwrap();
        if let Ok(r) = tree.contains_key(crc.to_be_bytes()) {
            r
        }
        else{
            false
        }
    }

    fn burn_from_tree(db: Arc<Db>, crc: u64, table: String) {
        let tree = db.open_tree(&table).unwrap();

        tree.remove(crc.to_be_bytes()).unwrap();
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
    fn get_contacts_skey(db: Arc<Db>, did: u64) -> Option<Vec<u8>> {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let s_key = format!("SKEY-{}", did);
        match tree.get(s_key) {
            Ok(Some(data)) => Some(data.to_vec()),
            _ => None,
        }
    }
    fn get_secret_key(&self,did:u64) -> Option<Vec<u8>> {
        Self::get_contacts_skey(self.db.clone(), did)
    }
    fn send_to(&self, to: u64, msg: Message, from_id: u64,key:Option<Vec<u8>>)->Result<u64> {
        RUNTIME.block_on(async move {
            let sender = self.sender.clone();
            let msg = serde_cbor::to_vec(&msg).unwrap();
            let from_id = if from_id > 0 {from_id} else {self.local_id().await.unwrap_or_default()};
             
            match tokio::time::timeout(Duration::from_secs(15), async move{
                let tx = sender.read().await;
                let tx = tx.as_ref().unwrap();
                let (req,res) = tokio::sync::oneshot::channel();
                tx.send((to, msg, from_id,req,key)).await.unwrap();
                match res.await {
                    Ok(r)=>{
                        r
                    }
                    Err(e)=>{
                        tracing::warn!("{e:?}");
                        Err(anyhow::anyhow!("{e:?}"))
                    }
                }
            }).await
            {
                Ok(Ok(crc))=>{
                    Ok(crc)
                }
                Ok(Err(e))=>{
                    Err(anyhow::anyhow!("{e:?}"))
                }
                Err(t)=>{
                    Err(anyhow::anyhow!("{t:?}"))
                }
            }
        })
    }

    /// Send msg to peer
    pub fn send_msg(&self, to: u64, msg: Vec<u8>) -> u64 {
        match message_from(msg) {
            Some(msg) => {
                
                match self.send_to(to, msg, 0,None).map_err(|e| {
                    tracing::warn!("{e:?}");
                    ClientError::SendFailed
                }){
                    Ok(crc)=> crc,
                    Err(_e)=> 0
                }
            }
            None => 0,
        }
    }

    pub fn recent_messages(&self,did:u64,top:u32) ->Vec<u64>{
        let mut msgs = vec![];
        let table = format!("message_{did}_time");
        let tree = self.db.open_tree(&table).unwrap();
        let mut itr = tree.into_iter();
        while let Some(val) = itr.next_back() {
            let (_k,v) = val.unwrap();
            let mut key = [0u8;8];
            key.clone_from_slice(&v[..8]);
            let crc = u64::from_be_bytes(key);
            msgs.push(crc);
            if msgs.len() >= top as usize {
                break;
            }
        }
        msgs        
    }
    pub fn meta_msg(&self,data:&[u8])-> EventMeta {
        let evt:Event = serde_cbor::from_slice(data).unwrap();
        let Event { to, event_time, from_id,msg, .. } = evt;
        let (to_tag,_) = Self::get_contacts_tag(self.db.clone(), to).unwrap_or_default();
        let (from_tag,_) = Self::get_contacts_tag(self.db.clone(), from_id).unwrap_or_default();

        EventMeta {
            from_id,
            to_id: to,
            from_tag,
            to_tag,
            event_time,
            msg,
        }
    }

    

    pub fn read_msg(&self,did:u64,crc:u64) -> Option<Vec<u8>> {
        let table = format!("message_{did}");
        
        let tree = self.db.open_tree(&table).unwrap();
        let db_t = self.db.clone();
        match tree.get(crc.to_be_bytes()) {
            Ok(v)=>{
                v.map(|v| {
                    let data = v.to_vec();
                    let evt:Event = serde_cbor::from_slice(&data[..]).unwrap();
                    let Event { to, event_time, crc, from_id, nonce, msg } = evt;
                    let key = Self::get_aes_key_from_contacts(db_t.clone(), did);
                    if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), key, nonce) {
                        match &msg {
                            Message::Chat { content }=>{
                                match content {
                                    ChatContent::Send { data }=>{
                                        let (title,body) = Self::extra_content(data);
                                        if let Some((_tag,tp)) = Self::get_contacts_tag(db_t.clone(), to) {
                                            let did = if tp == 0 {from_id} else {to};
                                            let now = Utc::now().timestamp_millis() as u64;
                                            Self::update_session(db_t, did, None, Some(crc), None, Some(body), now);
                                        }
                                    }
                                    _=>{

                                    }
                                }
                            }
                            _=>{

                            }
                        }
                        match message_to(msg) {
                            Some(d)=> Some(d),
                            None=>{
                                None
                            }
                        }
                    }
                    else{
                        eprintln!("c read msg: decrypt failed>>>");
                        None
                    }
                }).unwrap_or_default()
            }
            Err(e)=>{
                error!("{e:?}");
                None
            }
        }
    }
    pub fn read_msg_with_meta(&self,did:u64,crc:u64) -> Option<EventMeta> {
        let table = format!("message_{did}");
        
        let tree = self.db.open_tree(&table).unwrap();
        let db_t = self.db.clone();
        match tree.get(crc.to_be_bytes()) {
            Ok(v)=>{
                v.map(|v| {
                    let data = v.to_vec();
                    let evt:Event = serde_cbor::from_slice(&data[..]).unwrap();
                    let Event { to, event_time, crc, from_id, nonce, msg } = evt;
                    let mut key = Self::get_aes_key_from_contacts(db_t.clone(), did);
                    if key.is_none() {
                        key = Self::get_offer_by_offer_id(db_t.clone(), from_id);
                    }
                    let now = Utc::now().timestamp_millis() as u64;
                    if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg.clone()), key, nonce.clone()) {
                        match &msg {
                            Message::Chat { content }=>{
                                match content {
                                    ChatContent::Send { data }=>{
                                        let (_title,body) = Self::extra_content(data);
                                        if Self::update_session(db_t.clone(), did, None, Some(crc), None, Some(body), now) {
                                            let msg = Message::Chat { content: ChatContent::Feedback { crc, status: luffa_rpc_types::FeedbackStatus::Read } };
                                            if let Some(msg) = message_to(msg) {

                                                if self.send_msg(did, msg) == 0 {
                                                    tracing::warn!("send read feedback failed");
                                                }
                                            }
                                        }
                                        
                                    }
                                    _=>{
                                        Self::update_session(db_t.clone(), did, None, Some(crc), None, None, now);
                                    }
                                }
                            }
                            _=>{
                                Self::update_session(db_t.clone(), did, None, Some(crc), None, None, now);
                            }
                        }
                        
                        let (to_tag,_) = Self::get_contacts_tag(db_t.clone(), to).unwrap_or_default();
                        let (from_tag,_) = Self::get_contacts_tag(db_t.clone(), from_id).unwrap_or_default();
                        match message_to(msg) {
                            Some(msg)=> Some(EventMeta {
                                from_id,
                                to_id: to,
                                from_tag,
                                to_tag,
                                event_time,
                                msg,
                            }),
                            None=>{
                                None
                            }
                        }
                    }
                    else{
                        if let Some(key) = Self::get_offer_by_offer_id(db_t.clone(), from_id) {
                            if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), Some(key), nonce) {
                                Self::update_session(db_t.clone(), did, None, Some(crc), None, None, now);
                                let (to_tag,_) = Self::get_contacts_tag(db_t.clone(), to).unwrap_or_default();
                                let (from_tag,_) = Self::get_contacts_tag(db_t.clone(), from_id).unwrap_or_default();
                                match message_to(msg) {
                                    Some(msg)=> Some(EventMeta {
                                        from_id,
                                        to_id: to,
                                        from_tag,
                                        to_tag,
                                        event_time,
                                        msg,
                                    }),
                                    None=>{
                                        eprintln!("read msg 0: decrypt failed>>>");
                                        None
                                    }
                                }
                            }
                            else{
                                eprintln!("read msg 1: decrypt failed>>>");
                                None
                            }
                        }
                        else{
                            eprintln!("read msg 2: decrypt failed>>>");
                            None
                        }
                    }
                }).unwrap_or_default()
            }
            Err(e)=>{
                error!("{e:?}");
                None
            }
        }
    }

    pub fn get_local_id(&self) -> Option<u64> {
        RUNTIME.block_on(async {
            self.local_id().await
        })
    }

    pub fn get_did(&self) -> Option<String> {
        self.get_local_id().map(|d| {
            // hex::encode(d.to_be_bytes())
            bs58::encode(d.to_be_bytes()).into_string()
        })
    }

    async fn local_id(&self) ->Option<u64> {
        let filter = self.filter.read().await;
        let key = self.get_keypair(filter.clone()).await;
        key.map(|k| {
            let data =  PeerId::from_public_key(&k.public()).to_bytes();
            let mut digest = crc64fast::Digest::new();
            digest.write(&data);
            digest.sum64()
        })
        
    }

    pub fn session_list(&self, top: u32) -> Vec<ChatSession> {
        let tree = self.db.open_tree(KVDB_CHAT_SESSION_TREE).unwrap();
        let my_id = self.get_local_id().unwrap_or_default();
        let mut chats = tree
            .into_iter()
            .map(|item| {
                let (_key, val) = item.unwrap();
                let chat:ChatSession = serde_cbor::from_slice(&val[..]).unwrap();
                chat
            })
            .filter(|c| c.did != my_id )
            .collect::<Vec<_>>();
        
        chats.sort_by(|a, b| a.last_time.partial_cmp(&b.last_time).unwrap());
        chats.reverse();
        
        chats.truncate(top as usize);
        chats
    }

    /// pagination session
    pub fn session_page(&self,page:u32,size:u32) ->Option<Vec<ChatSession>> {
        let my_id = self.get_local_id().unwrap_or_default();
        Self::db_session_list(self.db.clone(), page, size,my_id)
    }
    /// pagination session list
    fn db_session_list(db: Arc<Db>, page: u32,page_size:u32,my_id:u64) -> Option<Vec<ChatSession>> {
        let tree = db.open_tree(KVDB_CHAT_SESSION_TREE).unwrap();

        let mut chats = tree
            .into_iter()
            .map(|item| {
                let (_key, val) = item.unwrap();
                let chat:ChatSession = serde_cbor::from_slice(&val[..]).unwrap();
                chat
            })
            .filter(|c| c.did != my_id )
            .collect::<Vec<_>>();
        chats.sort_by(|a, b| a.last_time.partial_cmp(&b.last_time).unwrap());
        chats.reverse();
        let page = chats.windows(page_size as usize).nth(page as usize);
        page.map(|ls| ls.into_iter().map(|s| {
            s.clone()

        } ).collect::<Vec<_>>())
    }

    pub fn keys(&self) -> Vec<String>{
        // luffa_node::Keychain::keys(&self)
        RUNTIME.block_on(async {

            let keychain = self.key.read().await;
            let chain = keychain.as_ref().unwrap();
            let keys  = chain.keys().map(|m| match m {
                Ok(k)=> Some(k.name()),
                Err(_e)=> None
            }).collect::<Vec<_>>().await;

            keys.into_iter().filter(|x| x.is_some()).map(|x| x.unwrap()).collect::<Vec<_>>()

        })
    }

    pub fn gen_key(&self,password:&str,store:bool) -> Option<String>{
        RUNTIME.block_on(async {
            let mut keychain = self.key.write().await;
            let chain = keychain.as_mut().unwrap();
            match chain.create_ed25519_key_bip39(password, store).await {
                Ok((phrase,key))=>{
                    let name = key.name();
                    if !store {
                        let tree = self.db.open_tree("bip39_keys").unwrap();
                        match key {
                           luffa_node::Keypair::Ed25519(v)=>{
                              let data = v.to_bytes();
                              let k_pair = format!("pair-{}",name);
                              let k_phrase = format!("phrase-{}",name);
                              tree.insert(k_pair, data.to_vec()).unwrap();
                              tree.insert(k_phrase, phrase.as_bytes()).unwrap();
                              tree.flush().unwrap();
                            } 
                            _=>{
                                
                            }    
                        }
                    }
                    Some(name)
                }
                Err(e)=>{
                    None
                }
            }
        })
    }
    pub fn import_key(&self,phrase:&str,password:&str) -> Option<String>{
        RUNTIME.block_on(async {
            let mut keychain = self.key.write().await;
            let chain = keychain.as_mut().unwrap();
            match chain.create_ed25519_key_from_seed(phrase,password).await {
                Ok(key)=>{
                    let name = key.name();
                    Some(name)
                }
                Err(e)=>{
                    None
                }
            }
        })
    }

    pub fn save_key(&self,name:&str) -> bool {
        RUNTIME.block_on(async {
            let tree = self.db.open_tree("bip39_keys").unwrap();
            let k_pair = format!("pair-{}",name);
            if let Ok(Some(k_val)) = tree.get(k_pair) {
                let mut keychain = self.key.write().await;
                let chain = keychain.as_mut().unwrap();
                let mut data = [0u8;64];
                data.clone_from_slice(&k_val);
                if let Ok(k) = chain.create_ed25519_key_from_bytes(&data).await {
                    true
                }
                else {
                    false
                }
            }
            else {
                true
            }
        })
    }
    pub fn read_key_phrase(&self,name:&str) -> Option<String> {
        
        let tree = self.db.open_tree("bip39_keys").unwrap();
        let k_pair = format!("phrase-{}",name);
        if let Ok(Some(k_val)) = tree.get(k_pair) {
            Some(String::from_utf8(k_val.to_vec()).unwrap())
        }
        else {
            None
        }
    }

    pub fn save_session(&self, did: u64, tag: String,read:Option<u64>,reach:Option<u64>,msg:Option<String>) {
        let now = Utc::now().timestamp_millis() as u64;
        Self::update_session(self.db.clone(), did, Some(tag), read, reach, msg, now);
    }
    pub fn update_session(db:Arc<Db>,did:u64,tag:Option<String>,read:Option<u64>,reach:Option<u64>,msg:Option<String>,event_time:u64) -> bool{
        let tree = db.open_tree(KVDB_CHAT_SESSION_TREE).unwrap();
        let mut first_read = false;
        let n_tag = tag.clone();
        if let Err(e) = tree.fetch_and_update(did.to_be_bytes(), |old| {
            match old {
                Some(val)=>{
                    let chat:ChatSession = serde_cbor::from_slice(val).unwrap();
                    let ChatSession { did,session_type, last_time, tag, read_crc, mut reach_crc, last_msg } = chat;
                    let mut last_time = last_time;
                    if let Some(c) = reach {
                        if !reach_crc.contains(&c) {
                            reach_crc.push(c);
                        }
                    }
                    if let Some(c) = read.as_ref() {
                        first_read = reach_crc.contains(c);
                        reach_crc.retain(|x| *x != *c);
                        // assert!(reach_crc.contains(c),"reach contain :{c}");
                        // warn!("reach_crc:{reach_crc:?}   {c}");
                        last_time = event_time;

                    }
                    let upd = ChatSession {
                        did,
                        session_type,
                        last_time,
                        tag:n_tag.clone().unwrap_or(tag),
                        read_crc:read.unwrap_or(read_crc),
                        reach_crc,
                        last_msg:msg.clone().unwrap_or(last_msg),
                    };
                    Some(serde_cbor::to_vec(&upd).unwrap())
                }
                None=>{
                    let (dft,tp) = Self::get_contacts_tag(db.clone(), did).unwrap_or((format!("{did}"),3));
                    if tp == 3 {
                        return None;
                    }
                    let mut reach_crc = vec![];
                    if let Some(c) = reach {
                        reach_crc.push(c);
                    }
                    let upd = ChatSession {
                        did,
                        session_type:tp,
                        last_time:event_time,
                        tag:n_tag.clone().unwrap_or(dft),
                        read_crc:read.unwrap_or_default(),
                        reach_crc,
                        last_msg:msg.clone().unwrap_or_default(),
                    };
                    Some(serde_cbor::to_vec(&upd).unwrap())
                }
            }
        }){
            tracing::warn!("{e:?}");
        }
        tree.flush().unwrap();
        first_read
        
    }

    fn get_contacts_tag(db:Arc<Db>,did:u64)->Option<(String,u8)> {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_key = format!("TAG-{}", did);
        match tree.get(&tag_key) {
            Ok(Some(v))=>{
                let tp = Self::get_contacts_type(db, did).unwrap_or(ContactsTypes::Private);
                Some((String::from_utf8(v.to_vec()).unwrap(),tp as u8))
            }
            _=>{
                None
            }
        }
    }
    fn get_contacts_type(db:Arc<Db>,did:u64)->Option<ContactsTypes> {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let type_key = format!("TYPE-{}", did);
        match tree.get(&type_key) {
            Ok(Some(v))=>{
                let tp = v[0];
                let tp = if tp == 0 { ContactsTypes::Private } else { ContactsTypes::Group };
                Some(tp)
            }
            _=>{
                None
            }
        }
    }
    pub fn find_contacts_tag(&self,did: u64)->Option<String> {
        Self::get_contacts_tag(self.db.clone(), did).map(|(v,_)| v)
    }

    pub fn update_contacts_tag(&self,did:u64,tag:String) {
        Self::set_contacts_tag(self.db.clone(), did, tag)
    }
    fn set_contacts_tag(db:Arc<Db>,did:u64,tag:String) {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_key = format!("TAG-{}",did);
        println!("set tag:{did} ==> {}",tag);
        tree.insert(tag_key.as_bytes(), tag.as_bytes()).unwrap();
        tree.flush().unwrap();
    }
    fn get_contacts_have_time(db:Arc<Db>,did:u64) -> u64 {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_key = format!("H-TIME-{}" ,did);
        if let Ok(Some(x)) = tree.get(tag_key.as_bytes()) {
            let mut val = [0u8;8];
            val.clone_from_slice(&x);
            u64::from_be_bytes(val)
        }
        else{
            0
        }
    }
    fn set_contacts_have_time(db:Arc<Db>,did:u64, now:u64) {

        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_key = format!("H-TIME-{}" ,did);
        tree.fetch_and_update(tag_key.as_bytes(), |old|{
            match old {
                Some(old)=>{
                    let mut val = [0u8;8];
                    val.clone_from_slice(&old);
                    let old = u64::from_be_bytes(val);
                    if old < now {
                        Some(now.to_be_bytes().to_vec())
                    }
                    else{
                        Some(old.to_be_bytes().to_vec())
                    }    
    
                }
                None=>{
                    Some(now.to_be_bytes().to_vec())
                }
            }
        }).unwrap();
        tree.flush().unwrap();
    }

    pub fn contacts_list(&self, c_type: u8) -> Vec<ContactsView> {
        let tree = self.db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_prefix = format!("TAG-");
        let itr = tree.scan_prefix(tag_prefix);
        itr.map(|item| {
            let (k, v) = item.unwrap();
            let tag = String::from_utf8(v.to_vec()).unwrap();
            let key = String::from_utf8(k.to_vec()).unwrap();
            let to = key.split('-').last().unwrap();
            let to: u64 = to.parse().unwrap();
            
            ContactsView { did: to, tag }
        })
        .filter(|x| {
            if let Some(t) = Self::get_contacts_type(self.db.clone(), x.did) {
                c_type == t as u8    
            }
            else if c_type == 0 {
                true
            }
            else{
                false
            }
        })
        .collect::<Vec<_>>()
    }

    pub fn search(
        &self,
        query: String,
        offset: u32,
        limit: u32,
    ) -> std::result::Result<Vec<String>, ClientError> {
        let reader = self.idx.reader().unwrap();
        let schema = self.idx.schema();
        let searcher = reader.searcher();

        let title = schema.get_field("title").unwrap();
        let body = schema.get_field("body").unwrap();

        let query_parser = QueryParser::for_index(&self.idx, vec![title, body]);

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

    pub fn get_peer_id(&self) -> Option<String> {
        
        RUNTIME.block_on(async {
            let filter = self.filter.read().await;
            let key = self.get_keypair(filter.clone()).await;
            key.map(|k| {
                let data =  PeerId::from_public_key(&k.public()).to_bytes();
                bs58::encode(data).into_string()
            })
        })
    }

    pub fn relay_list(&self) -> Vec<String> {
        let client = self.client.clone();

        RUNTIME.block_on(async {
            let c = client.read().await;
        
            if let Some(cc) = c.as_ref() {
                match cc
                    .get_peers()
                    .await
                {
                    Ok(peers) => {
                        tracing::debug!("peers:{peers:?}");
                        return peers
                            .into_iter()
                            .map(|(p,_)| bs58::encode(p.to_bytes()).into_string())
                            .collect::<Vec<_>>();
                    }
                    Err(e) => {
                        tracing::warn!("{e:?}");
                    }
                }
            }
            tracing::warn!("client is None");
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
                    Ok(_) => true,
                    _ => false,
                }
            } else {
                false
            }
        })
    }

    pub fn stop(&self) {
        self.send_to(
            u64::MAX,
            Message::StatusSync {
                to: 0,
                from_id: 0,
                status: AppStatus::Bye,
            },
            u64::MAX,
            None,
        ).unwrap();
    }
    pub fn init(&self,cfg_path: Option<String>) {
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
        println!("cfg_path:{cfg_path:?}");

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

        println!("config--->{config:?}");
        

        RUNTIME.block_on(async {
            let kc = Keychain::<DiskStorage>::new(config.p2p.clone().key_store_path.clone())
                .await
                .unwrap();

            let mut m_key = self.key.write().await;
            *m_key = Some(kc.clone());
            let mut m_cfg = self.config.write().await;
            *m_cfg = Some(config);
        });

    }
    
    pub fn start(&self,key:Option<String>,tag:Option<String>, cb: Box<dyn Callback>) -> u64
    {
        // let keychain = Keychain::<DiskStorage>::new(config.p2p.clone().key_store_path.clone());
        let filter = key.map(|k| KeyFilter::Name(format!("{}",k)));
        let (kc,config) = RUNTIME.block_on(async {
            let mut f = self.filter.write().await;
            *f = filter.clone();
            let m_key = self.key.read().await;
            let cfg = self.config.read().await;
            (m_key.clone().unwrap(),cfg.clone().unwrap())
        });

        let my_id = self.get_local_id();
        if my_id.is_none() {
            return 0;
        }
        let my_id = my_id.unwrap();
        self.update_contacts_tag(my_id, tag.unwrap_or(format!("{}",my_id)).to_string());

        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let db = self.db.clone();
       
        let client = self.client.clone();
        let idx_writer = self.writer.clone();
        let schema = self.schema.clone();
        RUNTIME.block_on(async {
            let mut sender = self.sender.write().await;
            *sender = Some(tx);
            tokio::spawn(async move {
                debug!("runing...");
                Self::run(db, config, kc, cb, client, rx, idx_writer, schema,filter).await;
                debug!("run exit!....");
            });
        });
        my_id
    }

    /// run
    async fn run(
        db: Arc<Db>,
        mut config: Config,
        kc: Keychain<DiskStorage>,
        cb: Box<dyn Callback>,
        client_lock: Arc<RwLock<Option<Arc<P2pClient>>>>,
        mut receiver: tokio::sync::mpsc::Receiver<(u64, Vec<u8>, u64,ShotSender<anyhow::Result<u64>>,Option<Vec<u8>>)>,
        idx_writer: Arc<RwLock<IndexWriter>>,
        schema: Schema,
        filter:Option<KeyFilter>,
    ) {
        // let (tx, rx) = tokio::sync::mpsc::channel::<NetworkEvent>(4096);
        config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);

        let metrics_config = config.metrics.clone();
        let cb = Arc::new(cb);

        let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
            .await
            .expect("failed to initialize metrics");

        let (peer, p2p_rpc, mut events, sender) = {
            let store = start_store(config.store.clone()).await.unwrap();

            let (peer_id, p2p_rpc, events, sender) =
                start_node(config.p2p.clone(), kc, Arc::new(store),filter)
                    .await
                    .unwrap();
            (peer_id, p2p_rpc, events, sender)
        };
        let client = Arc::new(luffa_node::rpc::P2p::new(sender));
        let client = Arc::new(P2pClient::new(client).unwrap());
        let mut digest = crc64fast::Digest::new();
        digest.write(&peer.to_bytes());
        let my_id = digest.sum64();
        let client_t = client.clone();
        let db_t = db.clone();
        tokio::spawn(async move {
            let mut timer = std::time::Instant::now();
            let mut count = 0_u64;
            loop {
                let peers =match client_t.get_peers().await {
                    Ok(peers)=>peers,
                    Err(e)=>{
                        tracing::warn!("{e:?}"); 
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;   
                    }
                };
                if peers.len() < 1 && timer.elapsed().as_millis() < 30000 {
                    
                    tracing::warn!("waiting....{}",1 - peers.len()); 
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;   
                }
                timer = std::time::Instant::now();

                count += 1;
                if count % 30 == 1 {
                    tracing::warn!("subscribed all as client,status sync");
                    let msg = luffa_rpc_types::Message::StatusSync {
                        to: 0,
                        status: AppStatus::Active,
                        from_id: my_id,
                    };
                    let key: Option<Vec<u8>> = None;
                    let event = Event::new(0, &msg, key, my_id);
                    let data = event.encode().unwrap();
                   
                    if let Err(e) = client_t
                        .gossipsub_publish(
                            TopicHash::from_raw(format!("{}", TOPIC_STATUS)),
                            bytes::Bytes::from(data),
                        )
                        .await
                    {
                        tracing::warn!("status sync pub>> {e:?}");
                    }
                }
                let page = count % 8;
                let mut contacts = vec![];
                for lvl in 0..8 {
                    if page <= lvl && count % 120 > 0{
                        if let Some(lvl_0) = Self::db_session_list(db_t.clone(), lvl as u32, 4,my_id) {
                            let lvl_contacts = 
                            lvl_0.into_iter().map(|cs| {
                                let to = cs.did;
                                let c_type = Self::get_contacts_type(db_t.clone(), to).unwrap_or(ContactsTypes::Private);
                                let c_type: u8 = c_type as u8;
                                let c_type = if c_type == 0 {ContactsTypes::Private}  else { ContactsTypes::Group};
                                let have_time = Self::get_contacts_have_time(db_t.clone(), to);
                                Contacts {
                                    did: to,
                                    r#type:c_type,
                                    have_time,
                                    wants:vec![],
                                }  
                            }).collect::<Vec<_>>();
                            contacts.extend_from_slice(&lvl_contacts[..]);
                            
                        }
                    }
                }
                if !contacts.is_empty() {
                    let sync = Message::ContactsSync { did: my_id, contacts };
                    let event = Event::new(0, &sync, None, my_id);
                    let data = event.encode().unwrap();
                    if let Err(e) = client_t.chat_request(bytes::Bytes::from(data)).await {
                        tracing::warn!("pub contacts sync status >>> {e:?}");
                    }
                }
                if count % 120 == 0 {

                    let tree = db_t.open_tree(KVDB_CONTACTS_TREE).unwrap();
    
                    let tag_prefix = format!("TAG-");
                    let itr = tree.scan_prefix(tag_prefix);
                    let contacts = itr.map(|item| {
                        let (k, _v) = item.unwrap();
                        // let tag = String::from_utf8(v.to_vec()).unwrap();
                        let key = String::from_utf8(k.to_vec()).unwrap();
                        let parts = key.split('-');
    
                        let to = parts.last().unwrap();
                        let to: u64 = to.parse().unwrap();
                        let c_type = Self::get_contacts_type(db_t.clone(), to).unwrap_or(ContactsTypes::Private);
                        let c_type: u8 = c_type as u8;
                        let c_type = if c_type == 0 {ContactsTypes::Private}  else { ContactsTypes::Group};
                        let have_time = Self::get_contacts_have_time(db_t.clone(), to);
                        Contacts {
                            did: to,
                            r#type:c_type,
                            have_time,
                            wants:vec![],
                        }  
                    })
                    .collect::<Vec<_>>();
                    
                    let sync = Message::ContactsSync { did: my_id, contacts };
                    let event = Event::new(0, &sync, None, my_id);
                    let data = event.encode().unwrap();
                    if let Err(e) = client_t.chat_request(bytes::Bytes::from(data)).await {
                        tracing::warn!("pub contacts sync status >>> {e:?}");
                    }
                } 
                // if let Err(e) = client_t
                //     .gossipsub_publish(
                //         TopicHash::from_raw(format!("{}", TOPIC_STATUS)),
                //         bytes::Bytes::from(data),
                //     )
                //     .await
                // {
                //     tracing::warn!("pub contacts sync status >>> {e:?}");
                // }
                // break;
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });

        let client_t = client.clone();
        {
            let mut lock = client_lock.write().await;
            *lock = Some(client_t);
        }
        let client_t = client.clone();
        let db_t = db.clone();
        let idx = idx_writer.clone();
        let idx_t = idx.clone();
        let idx_tt = idx.clone();
        let schema_t = schema.clone();
        let schema_tt = schema.clone();
        let process = tokio::spawn(async move {
            while let Some(evt) = events.recv().await {
                match evt {
                    NetworkEvent::RequestResponse(rsp)=>{
                        match rsp {
                            luffa_node::ChatEvent::Response { data,.. } => {
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
                                        if to != my_id {
                                            continue;
                                        }
                                        if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                            bytes::Bytes::from(msg),
                                            None,
                                            nonce,
                                        ) {
                                            match msg {
                                                Message::ContactsSync { did,contacts }=>{
                                                    if did != my_id {
                                                        continue;
                                                    }
                                                    for ctt in contacts {
                                                        let did = ctt.did;
                                                        let table = format!("message_{}",ctt.did);
                                                        for crc in ctt.wants {
                                                            if Self::have_in_tree(db_t.clone(), crc, &table) {
                                                                continue;
                                                            }
                                                            let clt = client_t.clone();
                                                            let db_tt = db_t.clone();
                                                            // add to wants crc of contacts
                                                            let cb_t = cb.clone();
                                                            let scm= schema_tt.clone();
                                                            let idx = idx_tt.clone();
                                                            tokio::spawn(async move {
                                                                match clt.get_crc_record(crc).await {
                                                                    Ok(res)=>{
                                                                        let data = res.data;
                                                                        // TODO remove crc from wants and update want_time
                                                                        tracing::warn!("get record: {crc}");
                                                                        Self::set_contacts_have_time(db_tt.clone(), did,event_time);
                                                                        let data = data.to_vec();
                                                                        Self::process_event(
                                                                            db_tt.clone(),
                                                                            cb_t.clone(),
                                                                            clt.clone(),
                                                                            idx.clone(),
                                                                            scm,
                                                                            &data,
                                                                            my_id,
                                                                        ).await;
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
                                                                                                    
                                                                                                    // Self::update_session(db_tt.clone(),did,None,Some(crc),None,None,event_time);
                                                                                                }
                                                                                                FeedbackStatus::Reach=>{

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
                                                                    Err(e)=>{
                                                                        tracing::warn!("get crc record failed:{e:?}");
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    }
                                                }
                                                _=>{

                                                }
                                            }
                                        }
                                    }
                                    else{
                                        let did = if to == my_id { from_id } else {to};
                                        let table = format!("message_{}",did);
                                        if !Self::have_in_tree(db_t.clone(), crc, &table) {
                                            Self::process_event(
                                                db_t.clone(),
                                                cb.clone(),
                                                client_t.clone(),
                                                idx.clone(),
                                                schema_tt.clone(),
                                                &data,
                                                my_id,
                                            ).await;
                                        }
                                    } 
                                }
                            },
                            luffa_node::ChatEvent::Request(data) => {
                                //TODO message which coming from other
                                if let Ok(im) = Event::decode_uncheck(&data) {
                                    let Event {
                                        from_id,
                                        to,
                                        crc,
                                        ..
                                    } = im;
                                    let did = if to == my_id { from_id } else {to};
                                    let table = format!("message_{}",did);
                                    if !Self::have_in_tree(db_t.clone(), crc, &table) {
                                        Self::process_event(
                                            db_t.clone(),
                                            cb.clone(),
                                            client_t.clone(),
                                            idx.clone(),
                                            schema_tt.clone(),
                                            &data,
                                            my_id,
                                        ).await;
                                    }
                                    
                                }
                            },
                            luffa_node::ChatEvent::OutboundFailure { peer, request_id, error } => {

                            },
                            luffa_node::ChatEvent::InboundFailure { peer, request_id, error } => {

                            },
                            luffa_node::ChatEvent::ResponseSent { peer, request_id } => {

                            },
                        }
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {
                        // TODO: a group member or my friend online?
                        tracing::debug!("Subscribed> peer_id: {peer_id:?} topic:{topic}");
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Message { message, from, id }) => {
                        
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Unsubscribed { peer_id, topic }) => {

                        // TODO: a group member or my friend offline?
                    }
                    NetworkEvent::PeerConnected(peer_id) => {
                        tracing::warn!("---------PeerConnected-----------{:?}", peer_id);
                       
                        // if let Err(e) = client_t.gossipsub_add_explicit_peer(peer_id).await {
                        //     tracing::warn!("gossipsub_add_explicit_peer failed");
                        // }

                        let mut digest = crc64fast::Digest::new();
                        digest.write(&peer_id.to_bytes());
                        let u_id = digest.sum64();
                        let msg = luffa_rpc_types::Message::StatusSync {
                            to: u_id,
                            from_id: my_id,
                            status: AppStatus::Connected,
                        };
                        let event = luffa_rpc_types::Event::new(0, &msg, None, u_id);
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
                    NetworkEvent::PeerDisconnected(peer_id) => {
                        tracing::debug!("---------PeerDisconnected-----------{:?}", peer_id);
                        
                        let mut digest = crc64fast::Digest::new();
                        digest.write(&peer_id.to_bytes());
                        let u_id = digest.sum64();
                        let msg = luffa_rpc_types::Message::StatusSync {
                            to: u_id,
                            from_id: my_id,
                            status: AppStatus::Disconnected,
                        };
                        let event = luffa_rpc_types::Event::new(0, &msg, None, u_id);
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
                        tracing::debug!("---------CancelLookupQuery-----------{:?}", peer_id);
                    }
                }
            }
        });

        let db_t = db.clone();
        while let Some((to, msg_data, from_id,channel,k)) = receiver.recv().await {
            if to == u64::MAX {
                channel.send(Err(anyhow::anyhow!("exit"))).unwrap();
                break;
            }
            let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
            // let is_exchane = msg.is_contacts_exchange();
            
            let evt = if msg.need_encrypt() {
                tracing::info!("----------encrypt------{}",to);
                match k {
                   Some(key)=>{
                       Some(Event::new(to, &msg, Some(key), from_id))
                   }
                   None=>{
                       match Self::get_aes_key_from_contacts(db.clone(), to) {
                           Some(key) => Some(Event::new(to, &msg, Some(key), from_id)),
                           None => { 
                               tracing::warn!("aes key not found did:{} msg:{:?}",to,msg);
                               None
                            //    Some(Event::new(to, &msg, None, from_id))
                           },
                       }
                   }  
                }
            } else {
                Some(Event::new(to, &msg, None, from_id))
            };
            match evt {
                Some(e) => {
                    let crc_id = e.crc;
                    let data = e.encode().unwrap();
                    
                    let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
                    let tag_key = format!("TAG-{}", to);
                    let (tag,msg_type) =
                    match tree.get(&tag_key.as_bytes()) {
                        Ok(Some(v))=>{
                            let tp =
                            match Self::get_contacts_type(db.clone(),to) {
                                Some(tp)=>{
                                    match tp {
                                        ContactsTypes::Private=>{
                                            format!("content_private")
                                        }
                                        _=>{
                                            format!("content_group")
                                        }
                                    }
                                }
                                None=>{
                                    format!("content_private")
                                }
                            };
                            (String::from_utf8(v.to_vec()).unwrap(),tp)
                        }
                        _=>{
                            
                            (String::new(),format!("exchange"))
                        }
                    };
                    if to != my_id {
                        
                        match client.chat_request(bytes::Bytes::from(data.clone())).await {
                            Ok(res)=>{
                                tracing::info!("{res:?}");
                            }
                            Err(e)=>{
                                tracing::error!("{e:?}");
                            }
                        }
                    }
                    else{
                        tracing::warn!("is to me---->");
                    }
                    if to > 0 {
                        let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
                        let Event {
                            to,
                            event_time,
                            crc,
                            from_id,
                            ..
                        } = e;
                        // assert!(nonce.is_some(),"nonce is none!!");
                        if let Err(e) = channel.send(Ok(crc)) {
                            tracing::warn!("channel send failed");
                        }
                        let table =  {
                            format!("message_{to}")
                        };
                        tracing::info!("send......");
                        if to != my_id && !msg.is_contacts_exchange() {
                            Self::save_to_tree(
                                db_t.clone(),
                                e.crc,
                                &table,
                                data.clone(),
                                event_time,
                            );
                        }
                        match msg {
                            Message::Chat { content } => {
                                // TODO index content search engine
                                match content {
                                    ChatContent::Burn { crc, expires } => {
                                        let p_table = format!("message_{to}");
                                        Self::burn_from_tree(db_t.clone(), crc, p_table);

                                        let fld_crc = schema_t.get_field("crc").unwrap();
                                        let mut wr = idx_t.write().await;
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
                                                    let t = multibase::encode(multibase::Base::Base58Btc, cid);
                                                    let body = format!("{:?}",reference);
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
                                            } => (title, format!("{:?}",m_type)),
                                        };
                                        
                                        
                                        
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
                                        let mut wr = idx_t.write().await;
                                        wr.add_document(doc).unwrap();
                                        wr.commit().unwrap();
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                    else{
                        if let Err(e) = channel.send(Ok(0)) {
                            tracing::warn!("channel send failed");
                        }
                    }
                }
                None => {
                    if let Err(e) = channel.send(Ok(0)) {
                        tracing::warn!("channel send failed");
                    }
                }
            }
        }

        // luffa_util::block_until_sigint().await;
        // ctl.await.unwrap();
        process.abort();
        p2p_rpc.abort();

        metrics_handle.shutdown();
    }

    fn get_aes_key_from_contacts(db: Arc<Db>, did: u64) -> Option<Vec<u8>> {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let s_key = format!("SKEY-{}", did);
        match tree.get(&s_key) {
            Ok(Some(d)) => Some(d.to_vec()),
            _ => {
                None   
            },
        }
    }

    fn extra_content(data:&ContentData) -> (String,String){
        match data {
            ContentData::Text { source, reference } => {
                match source {
                    luffa_rpc_types::DataSource::Cid { cid } => {
                        let t = multibase::encode(multibase::Base::Base58Btc, cid);
                        let body = format!("");
                        (t,body)
                    },
                    luffa_rpc_types::DataSource::Raw { data } => {
                        let t = format!("");
                        let body = multibase::encode(multibase::Base::Base64, data);
                        (t,body)
                    },
                    luffa_rpc_types::DataSource::Text { content } => {
                        let t = format!("");

                        (t,content.clone())

                    },
                }

            },
            ContentData::Link { txt, url, reference } => {
                (txt.clone(),url.clone())
            },
            ContentData::Media { title, m_type, source } => {
                (title.clone(),format!(""))
            },
        }
    }

    async fn process_event(
        db_t: Arc<Db>,
        cb: Arc<Box<dyn Callback>>,
        client_t: Arc<P2pClient>,
        idx: Arc<RwLock<IndexWriter>>,
        schema: Schema,
        data:&Vec<u8>,
        my_id:u64
    ) 
    {
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
            if to != my_id {
                return;
            }
            if nonce.is_none() {
                return;
            }
            let did = if to == my_id { from_id } else { to };
            match Self::get_aes_key_from_contacts(db_t.clone(), did) {
                Some(key) => {
                    if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                        bytes::Bytes::from(msg),
                        Some(key.clone()),
                        nonce.clone(),
                    ) {
                        // TODO: did is me or I'm a member any local group
                        let msg_data = serde_cbor::to_vec(&msg).unwrap();
                        let table = 
                            format!("message_{did}");

                        Self::save_to_tree(
                            db_t.clone(),
                            crc,
                            &table,
                            data.clone(),
                            event_time,
                        );
                        cb.on_message(crc, from_id, to, msg_data);
                        
                        match msg {
                            
                            Message::Chat { content } => {
                                // TODO index content search engine
                                match content {
                                    ChatContent::Burn { crc, expires } => {
                                        let table = 
                                            format!("message_{did}")
                                        ;
                                        Self::burn_from_tree(
                                            db_t.clone(),
                                            crc,
                                            table,
                                        );

                                        let fld_crc =
                                            schema.get_field("crc").unwrap();
                                        let mut wr = idx.write().await;
                                        let del =
                                            Term::from_field_u64(fld_crc, crc);
                                        wr.delete_term(del);
                                        wr.commit().unwrap();
                                    }
                                    ChatContent::Send { data } => {
                                        let feed = luffa_rpc_types::Message::Chat { content: ChatContent::Feedback { crc,status: luffa_rpc_types::FeedbackStatus::Reach } };
                                        let event = luffa_rpc_types::Event::new(
                                            did,
                                            &feed,
                                            Some(key),
                                            my_id,
                                        );
                                        tracing::info!("send feedback to {from_id}");
                                        let event = event.encode().unwrap();
                                        if let Err(e) = client_t
                                            .chat_request(
                                                bytes::Bytes::from(event),
                                            )
                                            .await
                                        {
                                            error!("{e:?}");
                                        }
                                        
                                        let (title,body) =
                                        match data {
                                            luffa_rpc_types::ContentData::Text { source, reference } => {
                                                match source {
                                                    luffa_rpc_types::DataSource::Cid { cid } => {
                                                        let t = multibase::encode(multibase::Base::Base58Btc, cid);
                                                        let body = format!("");
                                                        (t,body)
                                                    },
                                                    luffa_rpc_types::DataSource::Raw { data } => {
                                                        let t = format!("");
                                                        let body = multibase::encode(multibase::Base::Base64, data);
                                                        (t,body)
                                                    },
                                                    luffa_rpc_types::DataSource::Text { content } => {
                                                        let t = format!("");

                                                        (t,content)

                                                    },
                                                }

                                            },
                                            luffa_rpc_types::ContentData::Link { txt, url, reference } => {
                                                (txt,url)
                                            },
                                            luffa_rpc_types::ContentData::Media { title, m_type, source } => {
                                                (title,format!(""))
                                            },
                                        };
                                        // let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
                                        let (msg_type,did) = if to == my_id {
                                            // let tag_key_private = format!("TAG-0-{}", from_id);
                                            (format!("content_private"),from_id)
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
                                            (format!("content_group"),to)
                                        };


                                        Self::update_session(db_t.clone(), did, None, None, Some(crc), Some(body.clone()), event_time);
                                        let fld_crc =
                                            schema.get_field("crc").unwrap();
                                        let fld_from = schema
                                            .get_field("from_id")
                                            .unwrap();
                                        let fld_to =
                                            schema.get_field("to_id").unwrap();
                                        let fld_time = schema
                                            .get_field("event_time")
                                            .unwrap();
                                        let fld_title =
                                            schema.get_field("title").unwrap();
                                        let fld_body =
                                            schema.get_field("body").unwrap();
                                        let fld_type = schema
                                            .get_field("msg_type")
                                            .unwrap();
                                        let doc = doc!(
                                            fld_crc => crc,
                                            fld_from => from_id,
                                            fld_to => to,
                                            fld_time => event_time,
                                            fld_type => msg_type,
                                            fld_title => title,
                                            fld_body => body,
                                        );
                                        let mut wr = idx.write().await;
                                        wr.add_document(doc).unwrap();
                                        wr.commit().unwrap();
                                    }
                                    ChatContent::Feedback { crc, status }=>{
                                        match status {
                                            FeedbackStatus::Reach=>{

                                                Self::update_session(db_t.clone(), did, None, None, Some(crc), None, event_time);
                                            }
                                            _=>{

                                                Self::update_session(db_t.clone(), did, None, None, None, None, event_time);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Message::WebRtc { stream_id, action } => {
                                //TODO index to search engine
                            }
                            _ => {}
                        }
                    }
                    else{
                        eprintln!("decrypt failes!!! {:?}",nonce);
                    }
                }
                None => {
                    tracing::warn!("aes not in contacts {did}");
                    // warn!("Gossipsub> peer_id: {from:?} nonce:{:?}", nonce);
                    if let Some(key) =
                        Self::get_offer_by_offer_id(db_t.clone(), from_id)
                    {
                        tracing::warn!("offer is:{}  nonce:{:?} key: {:?}",from_id,nonce,key);
                        if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                            bytes::Bytes::from(msg),
                            Some(key.clone()),
                            nonce,
                        ) {
                            let msg_t = msg.clone();
                            let offer_key = key.clone();
                            let offer_id = from_id;
                            let db_tt = db_t.clone();
                            // TODO: did is me or I'm a member any local group
                            let offer_or_answer = | token: ContactsToken | async move {
                                let ContactsToken {
                                    public_key,
                                    sign,
                                    secret_key,
                                    contacts_type,
                                    comment,
                                    ..
                                } = token;
                                let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
                                let peer = PeerId::from_public_key(&pk);
                                let mut digest = crc64fast::Digest::new();
                                digest.write(&peer.to_bytes());
                                let from_id = digest.sum64();
                                let data = serde_cbor::to_vec(&msg).unwrap();
                                Self::save_contacts(
                                    db_tt.clone(),
                                    from_id,
                                    secret_key.clone(),
                                    public_key.clone(),
                                    contacts_type,
                                    sign.clone(),
                                    comment.clone(),
                                );
                                cb.on_message(crc, offer_id, to, data);

                                
                                let msg_type = match contacts_type {
                                    ContactsTypes::Private => {
                                        format!("contacts_private")
                                    }
                                    ContactsTypes::Group => {
                                        format!("contacts_group")
                                    }
                                };
                                let fld_crc =
                                    schema.get_field("crc").unwrap();
                                let fld_from = schema
                                    .get_field("from_id")
                                    .unwrap();
                                let fld_to =
                                    schema.get_field("to_id").unwrap();
                                let fld_time = schema
                                    .get_field("event_time")
                                    .unwrap();
                                let fld_title =
                                    schema.get_field("title").unwrap();
                                let fld_body =
                                    schema.get_field("body").unwrap();
                                let fld_type = schema
                                    .get_field("msg_type")
                                    .unwrap();
                                let title = multibase::encode(multibase::Base::Base58Btc, public_key);
                                let body = format!(
                                    "{}",
                                    comment.clone().unwrap_or_default()
                                );
                                let doc = doc!(
                                    fld_crc => crc,
                                    fld_from => from_id,
                                    fld_to => to,
                                    fld_time => event_time,
                                    fld_type => msg_type,
                                    fld_title => title,
                                    fld_body => body,
                                );
                                let mut wr = idx.write().await;
                                wr.add_document(doc).unwrap();
                                wr.commit().unwrap();
                                let msg = luffa_rpc_types::Message::Chat { content: ChatContent::Feedback { crc,status: luffa_rpc_types::FeedbackStatus::Reach } };
                                
                                let event = luffa_rpc_types::Event::new(
                                    from_id,
                                    &msg,
                                    Some(offer_key),
                                    offer_id,
                                );
                                let event = event.encode().unwrap();
                                if let Err(e) = client_t
                                    .chat_request(
                                        bytes::Bytes::from(event),
                                    )
                                    .await
                                {
                                    error!("{e:?}");
                                }
                                
                                
                            };
                            match msg_t {
                                Message::ContactsExchange { exchange }=>{
                                    match exchange{
                                        ContactsEvent::Answer { token }=>{
                                            tracing::info!("Answer>>>>>{token:?}");
                                            let comment = &token.comment;
                                            let pk = PublicKey::from_protobuf_encoding(&token.public_key).unwrap();
                                            let peer = PeerId::from_public_key(&pk);
                                            let mut digest = crc64fast::Digest::new();
                                            digest.write(&peer.to_bytes());
                                            let did = digest.sum64();
                                            let table = 
                                                format!("message_{did}");

                                            Self::save_to_tree(
                                                db_t.clone(),
                                                crc,
                                                &table,
                                                data.clone(),
                                                event_time,
                                            );
                                            // update offer which I has send
                                            let table = 
                                                format!("offer_{my_id}");
                                            let status = vec![1u8;1];
                                            Self::save_to_tree(
                                                db_t.clone(),
                                                from_id,
                                                &table,
                                                status,
                                                event_time,
                                            );
                                            Self::update_session(db_t.clone(), did, comment.clone(), None, None, None, event_time);
                                            offer_or_answer(token).await;
                                        }
                                        ContactsEvent::Offer { mut token }=>{
                                            tracing::warn!("Offer>>>>>{token:?}");

                                            // token.validate()
                                            let pk = PublicKey::from_protobuf_encoding(&token.public_key).unwrap();
                                            let peer = PeerId::from_public_key(&pk);
                                            let mut digest = crc64fast::Digest::new();
                                            digest.write(&peer.to_bytes());
                                            let did = digest.sum64();
                                            if let Some(key) = Self::get_contacts_skey(db_t.clone(), did) {
                                                tracing::warn!("change contacts to old s key");
                                                token.secret_key = key;
                                            }
                                            let table = 
                                                format!("message_{did}");

                                            Self::save_to_tree(
                                                db_t.clone(),
                                                crc,
                                                &table,
                                                data.clone(),
                                                event_time,
                                            );
                                            // add a pending answer for this offer which is received
                                            let table = 
                                                format!("offer_{my_id}");
                                            let status = vec![10u8;1];
                                            Self::save_to_tree(
                                                db_t.clone(),
                                                from_id,
                                                &table,
                                                status,
                                                event_time,
                                            );
                                            offer_or_answer(token).await;
                                        }
                                        _=>{

                                        }
                                    }
                                }
                                _=>{

                                }
                            }
                            
                        }
                        else{
                            tracing::error!("decrypt failed:>>>>");
                        }
                    } else {
                        eprintln!("invalid msg");
                    }
                }
            }
        }        // todo!()
    }

}
/// Starts a new p2p node, using the given mem rpc channel.
pub async fn start_node(
    config: luffa_node::config::Config,
    keychain: Keychain<DiskStorage>,
    db: Arc<luffa_store::Store>,
    filter:Option<KeyFilter>,
) -> anyhow::Result<(
    PeerId,
    JoinHandle<()>,
    Receiver<NetworkEvent>,
    Sender<luffa_node::rpc::RpcMessage>,
)> {
    tracing::info!("node>>>{config:?}");
    let (mut p2p, sender) = Node::new(config, keychain, db,Some("Luffa".to_string()),filter).await?;
    let events = p2p.network_events();
    let local_id = p2p.local_peer_id().clone();
    // Start services
    let p2p_task = task::spawn(async move {
        tracing::info!("p2p runnung..");
        if let Err(err) = p2p.run().await {
            error!("{:?}", err);
            tracing::warn!("{err:?}");
        } else {
            tracing::warn!("p2p run exit!")
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
