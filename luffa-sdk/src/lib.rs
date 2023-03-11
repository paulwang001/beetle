#[macro_use]
extern crate lazy_static;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::{
    aead::{KeyInit, OsRng},
    Aes256Gcm, // Or `Aes128Gcm`
};
use anyhow::Context;
use api::P2pClient;
use libp2p::gossipsub::GossipsubMessage;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use libp2p::{gossipsub::TopicHash, identity::Keypair};
use luffa_node::{
    load_identity, DiskStorage, GossipsubEvent, Keychain, NetworkEvent, Node, ENV_PREFIX,
};
use luffa_rpc_types::{message_from, ChatContent, ContactsTypes, Contacts, message_to, ContentData};
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
use tokio::sync::oneshot::{Sender as ShotSender};

mod api;
mod config;

use crate::config::Config;

const TOPIC_STATUS: &str = "luffa_status";
// const TOPIC_CONTACTS: &str = "luffa_contacts";
const TOPIC_CHAT: &str = "luffa_chat";
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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Contancts parse error")]
    CodeParser,
    #[error("Send message failed")]
    SendFailed,
    #[error("Search error")]
    SearchError,
}

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

pub trait Callback: Send + Sync + Debug {
    // fn on_message(&self, msg: Message) -> Result<Option<String>, CallbackError>;
    fn on_message(&self, crc: u64, from_id: u64, to: u64, msg: Vec<u8>);
}

fn content_index(idx_path: &Path) -> (Index, Schema) {
    let mut schema_builder = Schema::builder();
    let crc = schema_builder.add_u64_field("crc", INDEXED | STORED);
    let from_id = schema_builder.add_u64_field("from_id", INDEXED | STORED);
    let to_id = schema_builder.add_u64_field("to_id", INDEXED | STORED);
    let event_time = schema_builder.add_u64_field("event_time", INDEXED | STORED);
    let msg_type = schema_builder.add_text_field("msg_type", STRING | STORED);
    let title = schema_builder.add_text_field("title", TEXT);
    let body = schema_builder.add_text_field("body", TEXT);
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
    sender: Arc<RwLock<Option<tokio::sync::mpsc::Sender<(u64, Vec<u8>, u64,ShotSender<Result<u64>>)>>>>,
    key: Arc<RwLock<Option<Keypair>>>,
    db: Arc<Db>,
    client: Arc<RwLock<Option<Arc<P2pClient>>>>,
    idx: Arc<Index>,
    schema: Schema,
    writer: Arc<RwLock<IndexWriter>>,
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
            sender: Arc::new(RwLock::new(None)),
            db,
            client: Arc::new(RwLock::new(None)),
            idx: Arc::new(idx),
            schema,
            writer: Arc::new(RwLock::new(writer)),
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
        tracing::warn!("secret_key::: {}",secret_key.len());
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
            digest.write(&secret_key);
            let offer_id = digest.sum64();
            tracing::warn!("gen offer id:{}",offer_id);
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
                let msg: Message =
                    serde_cbor::from_slice(&msg).map_err(|e| {
                        tracing::warn!("{e:?}");
                        ClientError::CodeParser
                    })?;
                tracing::warn!("{:?}",msg);    
                match msg {
                    Message::ContactsExchange { exchange } => {
                        match exchange {
                            ContactsEvent::Offer { token } => {
                                match token.validate() {
                                    Ok(true)=>{
                                        let ContactsToken {
                                            public_key,
                                            create_at,
                                            sign,
                                            secret_key,
                                            comment,
                                            ..
                                        } = token;
                                        debug!("p-> secret_key::: {}",secret_key.len());
                                        let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
                                        let to = PeerId::from_public_key(&pk);
                                        let mut digest = crc64fast::Digest::new();
                                        digest.write(&to.to_bytes());
                                        let did = digest.sum64();
                                        Ok(format!("{}@{}",did,comment.unwrap_or_default()))
                                    }
                                    Ok(false)=>{
                                        Err(ClientError::CodeParser)
                                    }
                                    Err(e)=>{
                                        Err(ClientError::CodeParser)
                                    }
                                }
                                
                                // let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
                                // let mut msg = vec![];
                                // msg.extend_from_slice(&secret_key);
                                // // buf.extend_from_slice(&nonce);
                                // msg.extend_from_slice(&create_at.to_be_bytes());
                                // if pk.verify(&msg, &sign) {
                                   
                                // } else {
                                //     error!("is invalid");
                                    
                                // }
                            }
                            _ => Err(ClientError::CodeParser),
                        }
                    }
                    _ => Err(ClientError::CodeParser),
                }
            }
            Err(e) => {
                error!("{e:?}");
                Err(ClientError::CodeParser)
            },
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
                let msg: Message =
                    serde_cbor::from_slice(&msg).map_err(|e| ClientError::CodeParser)?;
                match msg {
                    Message::ContactsExchange { exchange } => {
                        match exchange {
                            ContactsEvent::Offer { token } => {
                                match token.validate() {
                                    Ok(true)=>{
                                        let ContactsToken {
                                            public_key,
                                            create_at,
                                            sign,
                                            secret_key,
                                            contacts_type,
                                            ..
                                        } = token;
                                        tracing::warn!("a secret_key::: {}",secret_key.len());
                                        let code =
                                        self.gen_offer_anwser(comment.clone(), Some(secret_key.clone()));
                                        let (_, data) = multibase::decode(&code).unwrap();
                                        let msg: Message = serde_cbor::from_slice(&data)
                                            .map_err(|e| ClientError::CodeParser)?;
                                        let pk = PublicKey::from_protobuf_encoding(&public_key).unwrap();
                                        let to = PeerId::from_public_key(&pk);
                                        let mut digest = crc64fast::Digest::new();
                                        digest.write(&to.to_bytes());
                                        let to = digest.sum64();
                                        // Save answer to DB
                                        Self::save_contacts(
                                            self.db.clone(),
                                            to,
                                            secret_key.clone(),
                                            public_key,
                                            sign,
                                            contacts_type as u8,
                                            comment,
                                        );
                                        // let from_id = self.get_local_id().unwrap_or_default();
                                        let mut digest = crc64fast::Digest::new();
                                        digest.write(&secret_key);
                                        let offer_id = digest.sum64();
                                        tracing::warn!("answer offer id:{}",offer_id);
                                        self.send_to(to, msg, offer_id).expect("");
                                        Ok(())
                                    }
                                    Ok(false)=>{
                                        Err(ClientError::CodeParser)
                                    }
                                    Err(e)=>{
                                        Err(ClientError::CodeParser)
                                    }
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
        db: Arc<Db>,
        to: u64,
        secret_key: Vec<u8>,
        public_key: Vec<u8>,
        sign: Vec<u8>,
        c_type: u8,
        comment: Option<String>,
    ) {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let s_key = format!("S-KEY-{c_type}-{}", to);
        let p_key = format!("P-KEY-{c_type}-{}", to);
        let sig_key = format!("SIG-{c_type}-{}", to);
        let tag_key = format!("TAG-{c_type}-{}", to);
        tree.insert(s_key.as_bytes(), secret_key).unwrap();
        tree.insert(p_key.as_bytes(), public_key).unwrap();
        tree.insert(sig_key.as_bytes(), sign).unwrap();
        tree.insert(tag_key.as_bytes(), comment.unwrap_or(format!("{to}")).as_bytes())
            .unwrap();
        tree.flush().unwrap();
    }
    fn save_to_tree(db: Arc<Db>, crc: u64, table: String, data: Vec<u8>) {
        let tree = db.open_tree(&table).unwrap();

        tree.insert(crc.to_be_bytes(), data).unwrap();
        tree.flush().unwrap();
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
    fn send_to(&self, to: u64, msg: Message, from_id: u64)->Result<u64> {
        RUNTIME.block_on(async move {
            let sender = self.sender.clone();
            let msg = serde_cbor::to_vec(&msg).unwrap();
            let from_id = if from_id > 0 {from_id} else {self.local_id().await.unwrap_or_default()};
             
            match tokio::time::timeout(Duration::from_secs(5), async move{
                let tx = sender.read().await;
                let tx = tx.as_ref().unwrap();
                let (req,res) = tokio::sync::oneshot::channel();
                tx.send((to, msg, from_id,req)).await.unwrap();
                res.await.unwrap()
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
    pub fn send_msg(&self, to: u64, msg: Vec<u8>) -> std::result::Result<u64, ClientError> {
        match message_from(msg) {
            Some(msg) => {
                
                let crc = self.send_to(to, msg, 0).map_err(|e| {
                    error!("{e:?}");
                    tracing::warn!("{e:?}");
                    ClientError::SendFailed
                })?;
                Ok(crc)
            }
            None => Err(ClientError::SendFailed),
        }
    }

    pub fn read_msg(&self,did:u64,crc:u64,session_type:u8) -> Vec<u8> {
        let table = if session_type == 0 {
            format!("private_{did}")
        } else {
            format!("group_{did}")
        };
        let tree = self.db.open_tree(&table).unwrap();
        let db_t = self.db.clone();
        match tree.get(crc.to_be_bytes()) {
            Ok(v)=>{
                v.map(|v| {
                    let data = v.to_vec();
                    let evt:Event = serde_cbor::from_slice(&data[..]).unwrap();
                    let Event { to, event_time, crc, from_id, nonce, msg } = evt;
                    let key = Self::get_aes_key_from_contacts(db_t.clone(), to);
                    if let Ok(msg) = Message::decrypt(bytes::Bytes::from(msg), key, nonce) {
                        match &msg {
                            Message::Chat { content }=>{
                                match content {
                                    ChatContent::Send { data }=>{
                                        let (title,body) = Self::extra_content(data);
                                        if let Some((_tag,tp)) = Self::get_contacts_tag(db_t.clone(), to) {
                                            let did = if tp == 0 {from_id} else {to};
                                            let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap();
                                            Self::update_session(db_t, did, None, Some(crc), None, Some(body), now.as_secs());
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
                            Some(d)=> d,
                            None=>{
                                vec![]
                            }
                        }
                    }
                    else{
                        vec![]
                    }
                }).unwrap_or_default()
            }
            Err(e)=>{
                error!("{e:?}");
                vec![]
            }
        }
    }

    pub fn get_local_id(&self) -> Option<u64> {
        RUNTIME.block_on(async {
            self.local_id().await
        })
    }

    async fn local_id(&self) ->Option<u64> {
        let key = self.key.read().await;
        let key = key
            .as_ref()
            .map(|p| PeerId::from_public_key(&p.public()).to_bytes());
        key.map(|m| {
            let mut digest = crc64fast::Digest::new();
            digest.write(&m);
            digest.sum64()
        })
    }

    pub fn session_list(&self, top: u32) -> Vec<ChatSession> {
        let tree = self.db.open_tree(KVDB_CHAT_SESSION_TREE).unwrap();

        let mut chats = tree
            .into_iter()
            .map(|item| {
                let (key, val) = item.unwrap();
                let chat:ChatSession = serde_cbor::from_slice(&val[..]).unwrap();
                chat
            })
            .collect::<Vec<_>>();
        chats.sort_by(|a, b| a.last_time.partial_cmp(&b.last_time).unwrap());
        chats.reverse();
        chats.truncate(top as usize);
        chats
    }

    pub fn save_session(&self, did: u64, tag: String,read:Option<u64>,reach:Option<u64>,msg:Option<String>) {
        let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
        Self::update_session(self.db.clone(), did, Some(tag), read, reach, msg, now.as_secs());
    }
    pub fn update_session(db:Arc<Db>,did:u64,tag:Option<String>,read:Option<u64>,reach:Option<u64>,msg:Option<String>,event_time:u64) {
        let tree = db.open_tree(KVDB_CHAT_SESSION_TREE).unwrap();
        let n_tag = tag.clone();
        tree.fetch_and_update(did.to_be_bytes(), |old| {
            match old {
                Some(val)=>{
                    let chat:ChatSession = serde_cbor::from_slice(val).unwrap();
                    let ChatSession { did,session_type, last_time, tag, read_crc, mut reach_crc, last_msg } = chat;
                    if let Some(c) = reach {
                        reach_crc.push(c);
                    }
                    if let Some(c) = read.as_ref() {
                        reach_crc.retain(|x| x != c)
                    }
                    let upd = ChatSession {
                        did,
                        session_type,
                        last_time:event_time,
                        tag:n_tag.clone().unwrap_or(tag),
                        read_crc:read.unwrap_or(read_crc),
                        reach_crc,
                        last_msg:msg.clone().unwrap_or(last_msg),
                    };
                    Some(serde_cbor::to_vec(&upd).unwrap())
                }
                None=>{
                    let (dft,tp) = Self::get_contacts_tag(db.clone(), did).unwrap_or((format!("{did}"),3));
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
        }).unwrap();
        tree.flush().unwrap();
        
    }

    fn get_contacts_tag(db:Arc<Db>,did:u64)->Option<(String,u8)> {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_key_private = format!("TAG-0-{}", did);
        let tag_key_group = format!("TAG-1-{}", did);
        match tree.get(&tag_key_group) {
            Ok(Some(v))=>{
                Some((String::from_utf8(v.to_vec()).unwrap(),1))
            }
            _=>{
                match tree.get(&tag_key_private) {
                    Ok(Some(v))=>{
                        Some((String::from_utf8(v.to_vec()).unwrap(),0))
                    }
                    _=>{
                        None
                    }
                }
            }
        }
    }
    pub fn find_contacts_tag(&self,did: u64)->Option<String> {
        Self::get_contacts_tag(self.db.clone(), did).map(|(v,_)| v)
    }

    pub fn update_contacts_tag(&self,did:u64,contact_type:u8,tag:String) {
        Self::set_contacts_tag(self.db.clone(), did, contact_type, tag)
    }
    fn set_contacts_tag(db:Arc<Db>,did:u64,contact_type:u8,tag:String) {
        let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_key = format!("TAG-{}-{}", contact_type ,did);
        tree.insert(tag_key.as_bytes(), tag.as_bytes()).unwrap();
        tree.flush().unwrap();
    }

    pub fn contacts_list(&self, c_type: u8) -> Vec<ContactsView> {
        let tree = self.db.open_tree(KVDB_CONTACTS_TREE).unwrap();
        let tag_prefix = format!("TAG-{c_type}");
        let itr = tree.scan_prefix(tag_prefix);
        itr.map(|item| {
            let (k, v) = item.unwrap();
            let tag = String::from_utf8(v.to_vec()).unwrap();
            let key = String::from_utf8(k.to_vec()).unwrap();
            let to = key.split('-').last().unwrap();
            let to: u64 = to.parse().unwrap();

            ContactsView { did: to, tag }
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
                        tracing::info!("peers:{peers:?}");
                        return peers
                            .into_iter()
                            .map(|p| bs58::encode(p.to_bytes()).into_string())
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
        ).unwrap();
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
        debug!("cfg_path:{cfg_path:?}");
        let cfg_path = cfg_path.map(|p| PathBuf::from(p));
        let cfg_path = cfg_path.unwrap_or(dft_path);
        debug!("cfg_path:{cfg_path:?}");

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

        debug!("config--->{config:?}");
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
        let db = self.db.clone();
        let client = self.client.clone();
        let idx_writer = self.writer.clone();
        let schema = self.schema.clone();
        RUNTIME.block_on(async {
            let mut sender = self.sender.write().await;
            *sender = Some(tx);
            // let cb = Arc::new(cb);
            // let cb_t = cb.clone();
            tokio::spawn(async move {
                debug!("runing...");
                Self::run(db, config, kc, cb, client, rx, idx_writer, schema).await;
                debug!("run exit!....");
            });
        });
    }
    async fn run(
        db: Arc<Db>,
        mut config: Config,
        kc: Keychain<DiskStorage>,
        cb: Box<dyn Callback>,
        client_lock: Arc<RwLock<Option<Arc<P2pClient>>>>,
        mut receiver: tokio::sync::mpsc::Receiver<(u64, Vec<u8>, u64,ShotSender<anyhow::Result<u64>>)>,
        idx_writer: Arc<RwLock<IndexWriter>>,
        schema: Schema,
    ) {
        // let (tx, rx) = tokio::sync::mpsc::channel::<NetworkEvent>(4096);
        config.metrics = luffa_node::metrics::metrics_config_with_compile_time_info(config.metrics);

        let metrics_config = config.metrics.clone();

        let metrics_handle = luffa_metrics::MetricsHandle::new(metrics_config)
            .await
            .expect("failed to initialize metrics");

        let (peer, p2p_rpc, mut events, sender) = {
            let store = start_store(config.store.clone()).await.unwrap();

            let (peer_id, p2p_rpc, events, sender) =
                start_node(config.p2p.clone(), kc, Arc::new(store))
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
            let mut has_err = false;
            loop {
                let topics = vec![TOPIC_STATUS,TOPIC_CHAT];
                for t in topics.into_iter() {
                    if let Err(e) = client_t.gossipsub_subscribe(TopicHash::from_raw(t)).await {
                        error!("{e:?}");
                        tracing::warn!("{e:?}");
                        has_err = true;
                        break;
                    }
                }

                if !has_err {
                    tracing::info!("subscribed all as client,status sync");
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
                    let tree = db_t.open_tree(KVDB_CONTACTS_TREE).unwrap();

                    let tag_prefix = format!("TAG-");
                    let itr = tree.scan_prefix(tag_prefix);
                    let contacts = itr.map(|item| {
                        let (k, _v) = item.unwrap();
                        // let tag = String::from_utf8(v.to_vec()).unwrap();
                        let key = String::from_utf8(k.to_vec()).unwrap();
                        let mut parts = key.split('-');

                        let c_type = parts.nth(1).unwrap();
                        let to = parts.last().unwrap();
                        let to: u64 = to.parse().unwrap();
                        let c_type: u8 = c_type.parse().unwrap();
                        let c_type = if c_type == 0 {ContactsTypes::Private}  else { ContactsTypes::Group};
                        Contacts {
                            did: to,
                            r#type:c_type,
                            have_time:0,
                            wants:vec![],
                        }  
                    })
                    .collect::<Vec<_>>();
                    let mut itr = contacts.iter();
                    while let Some(ctt) = itr.next() {
                        if ctt.r#type == ContactsTypes::Private {
                            let p_key = format!("P-KEY-0-{}", ctt.did);
                            if let Ok(Some(k)) = tree.get(&p_key) {
                                let pk = PublicKey::from_protobuf_encoding(&k).unwrap();
                                let did_peer = PeerId::from_public_key(&pk);
                                if let Err(e) = client_t.gossipsub_add_explicit_peer(did_peer).await {
                                    tracing::warn!("{e:?}");
                                }
                            }
                            continue;
                        }
                        // let topic = TopicHash::from_raw(format!("{}_{}", TOPIC_CHAT, ctt.did));
                        // if let Err(e) = client_t.gossipsub_subscribe(topic).await {
                        //     tracing::warn!("{e:?}");
                        // }
                    }
                    let sync = Message::ContactsSync { did: my_id, contacts };
                    let event = Event::new(0, &sync, None, my_id);
                    let data = event.encode().unwrap();
                    if let Err(e) = client_t
                        .gossipsub_publish(
                            TopicHash::from_raw(format!("{}", TOPIC_STATUS)),
                            bytes::Bytes::from(data),
                        )
                        .await
                    {
                        tracing::warn!("pub contacts sync status >>> {e:?}");
                    }
                    // break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
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
        let schema_t = schema.clone();
        let process = tokio::spawn(async move {
            while let Some(evt) = events.recv().await {
                match evt {
                    NetworkEvent::RequestResponse(rsp)=>{
                        match rsp {
                            luffa_node::ChatEvent::Response { request_id, data } => {

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
                        tracing::warn!("Subscribed> peer_id: {peer_id:?} topic:{topic}");
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Message { message, from, id }) => {
                        let GossipsubMessage { data, .. } = message;
                        if let Ok(im) = Event::decode(&data) {
                            let Event {
                                msg,
                                nonce,
                                to,
                                from_id,
                                crc,
                                event_time,
                                ..
                            } = im;
                            debug!("Gossipsub> peer_id: {from:?} msg:{}", msg.len());
                            // TODO check did status
                            if nonce.is_none() {
                                tracing::warn!("nonce is None");
                                if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                    bytes::Bytes::from(msg),
                                    None,
                                    nonce,
                                ) {
                                    // TODO: did is me or I'm a member any local group
                                    debug!("Gossipsub> on_message peer_id: {from:?} msg:{:?}", msg);
                                    let msg_data = serde_cbor::to_vec(&msg).unwrap();
                                    let cb = &*cb;
                                    cb.on_message(crc, from_id, to, msg_data);
                                    match msg {
                                        
                                        Message::ContactsSync { did,contacts }=>{
                                            if did == my_id {
                                                for ctt in contacts {
                                                    
                                                    for crc in ctt.wants {
                                                        let clt = client_t.clone();
                                                        tokio::spawn(async move {
                                                            match clt.get_crc_record(crc).await {
                                                                Ok(res)=>{
                                                                    let data = res.data;
                                                                }
                                                                Err(e)=>{
                                                                    tracing::warn!("get crc record failed:{e:?}");
                                                                }
                                                            }
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                        _=>{

                                        }
                                    }
                                    // todo!()
                                }
                            } else {
                                match Self::get_aes_key_from_contacts(db_t.clone(), from_id) {
                                    Some(key) => {
                                        debug!("ase >>> {}",key.len());
                                        if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                            bytes::Bytes::from(msg),
                                            Some(key.clone()),
                                            nonce,
                                        ) {
                                            // TODO: did is me or I'm a member any local group
                                            let msg_data = serde_cbor::to_vec(&msg).unwrap();
                                            let table = if to == my_id {
                                                format!("private_{from_id}")
                                            } else {
                                                format!("group_{to}")
                                            };

                                            Self::save_to_tree(
                                                db_t.clone(),
                                                crc,
                                                table,
                                                data.clone(),
                                            );
                                            let cb = &*cb;
                                            cb.on_message(crc, from_id, to, msg_data);
                                            let msg = luffa_rpc_types::Message::Feedback {
                                                crc,
                                                status: luffa_rpc_types::FeedbackStatus::Reach,
                                            };
                                            let event =
                                                luffa_rpc_types::Event::new(0, &msg, None, my_id);
                                            let event = event.encode().unwrap();
                                            if let Err(e) = client_t
                                                .gossipsub_publish(
                                                    TopicHash::from_raw(TOPIC_STATUS),
                                                    bytes::Bytes::from(event),
                                                )
                                                .await
                                            {
                                                error!("pub reach to all relays>> {e:?}");
                                            }
                                            match msg {
                                                Message::ContactsExchange { exchange } => {
                                                    match exchange {
                                                        ContactsEvent::Answer { token } => {
                                                            let ContactsToken {
                                                                public_key,
                                                                create_at,
                                                                sign,
                                                                secret_key,
                                                                contacts_type,
                                                                comment,
                                                            } = token;
                                                            let c_type = contacts_type as u8;
                                                            Self::save_contacts(
                                                                db_t.clone(),
                                                                from_id,
                                                                secret_key,
                                                                public_key.clone(),
                                                                sign,
                                                                c_type,
                                                                comment.clone(),
                                                            );
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
                                                                comment.unwrap_or_default()
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
                                                                Some(key),
                                                                my_id,
                                                            );
                                                            let event = event.encode().unwrap();
                                                            if let Err(e) = client_t
                                                                .gossipsub_publish(
                                                                    TopicHash::from_raw(format!(
                                                                        "{}",
                                                                        TOPIC_CHAT
                                                                    )),
                                                                    bytes::Bytes::from(event),
                                                                )
                                                                .await
                                                            {
                                                                error!("{e:?}");
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                                Message::Chat { content } => {
                                                    // TODO index content search engine
                                                    match content {
                                                        ChatContent::Burn { crc, expires } => {
                                                            let table = if to == my_id {
                                                                format!("private_{from_id}")
                                                            } else {
                                                                format!("group_{to}")
                                                            };
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
                                                            let msg = luffa_rpc_types::Message::Chat { content: ChatContent::Feedback { crc,status: luffa_rpc_types::FeedbackStatus::Reach } };
                                                            let event = luffa_rpc_types::Event::new(
                                                                from_id,
                                                                &msg,
                                                                Some(key),
                                                                my_id,
                                                            );
                                                            let event = event.encode().unwrap();
                                                            if let Err(e) = client_t
                                                                .gossipsub_publish(
                                                                    TopicHash::from_raw(format!(
                                                                        "{}",
                                                                        TOPIC_CHAT
                                                                    )),
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
                                                        _ => {}
                                                    }
                                                }
                                                Message::WebRtc { stream_id, action } => {
                                                    //TODO index to search engine
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    None => {
                                        
                                        if let Some(key) =
                                            Self::get_offer_by_offer_id(db_t.clone(), from_id)
                                        {
                                            tracing::warn!("offer is:{}  nonce:{:?} key: {:?}",from_id,nonce,key);
                                            if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                                bytes::Bytes::from(msg),
                                                Some(key.clone()),
                                                nonce,
                                            ) {
                                                // TODO: did is me or I'm a member any local group
                                                match &msg {
                                                    Message::ContactsExchange { exchange }=>{
                                                        match exchange{
                                                            ContactsEvent::Answer { token }=>{
                                                                let ContactsToken {
                                                                    public_key,
                                                                    sign,
                                                                    contacts_type,
                                                                    comment,
                                                                    ..
                                                                } = token;
                                                                let pk = PublicKey::from_protobuf_encoding(public_key).unwrap();
                                                                let peer = PeerId::from_public_key(&pk);
                                                                let mut digest = crc64fast::Digest::new();
                                                                digest.write(&peer.to_bytes());
                                                                let from_id = digest.sum64();
                                                                let data = serde_cbor::to_vec(&msg).unwrap();
                                                                let cb = &*cb;
                                                                cb.on_message(crc, from_id, to, data);

                                                                let msg = luffa_rpc_types::Message::Chat { content: ChatContent::Feedback { crc,status: luffa_rpc_types::FeedbackStatus::Reach } };
                                                                let event = luffa_rpc_types::Event::new(
                                                                    from_id,
                                                                    &msg,
                                                                    Some(key.clone()),
                                                                    my_id,
                                                                );
                                                                let event = event.encode().unwrap();
                                                                tracing::warn!("pub to :{}",from_id);
                                                                if let Err(e) = client_t
                                                                    .gossipsub_publish(
                                                                        TopicHash::from_raw(format!(
                                                                            "{}",
                                                                            TOPIC_CHAT
                                                                        )),
                                                                        bytes::Bytes::from(event),
                                                                    )
                                                                    .await
                                                                {
                                                                    error!("{e:?}");
                                                                }
                                                                else{
                                                                    tracing::warn!("pub to :{} Ok!",from_id);
                                                                    let c_type = if contacts_type == &ContactsTypes::Private {0}  else {1};
                                                                    Self::save_contacts(
                                                                        db_t.clone(),
                                                                        from_id,
                                                                        key.clone(),
                                                                        public_key.clone(),
                                                                        sign.clone(),
                                                                        c_type,
                                                                        comment.clone(),
                                                                    );
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
                        tracing::debug!("---------PeerConnected-----------{:?}", peer_id);
                        if let Err(e) = client_t.gossipsub_add_explicit_peer(peer_id).await {
                            tracing::warn!("gossipsub_add_explicit_peer failed");
                        }
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
        while let Some((to, msg_data, from_id,channel)) = receiver.recv().await {
            if to == u64::MAX {
                channel.send(Err(anyhow::anyhow!("exit"))).unwrap();
                break;
            }
            let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
            let is_exchane = msg.is_contacts_exchange();
            let evt = if msg.need_encrypt() {
                tracing::warn!("----------encrypt------{}",to);
                match Self::get_aes_key_from_contacts(db.clone(), to) {
                    Some(key) => Some(Event::new(to, &msg, Some(key), from_id)),
                    None => { 
                        tracing::warn!("aes key not found");
                        Some(Event::new(to, &msg, None, from_id))
                    },
                }
            } else {
                Some(Event::new(to, &msg, None, from_id))
            };
            match evt {
                Some(e) => {
                    
                    let data = e.encode().unwrap();
                    let topic_hash = if to == 0 {
                        TopicHash::from_raw(TOPIC_STATUS)
                    } else {
                        tracing::warn!("----------encrypt---send---");
                        
                        TopicHash::from_raw(format!("{}", TOPIC_CHAT))
                    };
                    let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
                    let tag_key_private = format!("TAG-0-{}", to);
                    let tag_key_me = format!("TAG-0-{}", my_id);
                    let tag_key_group = format!("TAG-1-{}", to);
                    let (tag,msg_type) =
                    match tree.get(&tag_key_group.as_bytes()) {
                        Ok(Some(v))=>{
                            (String::from_utf8(v.to_vec()).unwrap(),format!("content_group"))
                        }
                        _=>{
                            match tree.get(&tag_key_private.as_bytes()) {
                                Ok(Some(v))=>{
                                    (String::from_utf8(v.to_vec()).unwrap(),format!("content_private"))
                                }
                                _=>{
                                    if to > 0 && !is_exchane {
                                        tracing::warn!("unkown {},not in my contacts",to);
                                        continue;
                                    }
                                    else{
                                        match tree.get(&tag_key_me.as_bytes()) {
                                            Ok(Some(v))=>{
                                                (String::from_utf8(v.to_vec()).unwrap(),format!("exchange"))
                                            }
                                            _=>{
                                                (format!("{my_id}"),format!("exchange"))
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    };
                    if to != my_id {
                        if let Err(e) = client
                            .gossipsub_publish(topic_hash, bytes::Bytes::from(data.clone()))
                            .await
                        {
                            error!("{e:?}");
                            tracing::warn!("{e:?}");
                            channel.send(Err(anyhow::anyhow!("publish failed:{:?}",e))).unwrap();
                            continue;
                        }
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
                        if let Err(e) = channel.send(Ok(crc)) {
                            tracing::warn!("channel send failed");
                        }
                        let table = if &msg_type == "content_group" {
                            format!("group_{to}")
                        } else {
                            format!("private_{from_id}")
                        };
                        Self::save_to_tree(
                            db_t.clone(),
                            e.crc,
                            table,
                            data.clone(),
                        );
                        match msg {
                            Message::Chat { content } => {
                                // TODO index content search engine
                                match content {
                                    ChatContent::Burn { crc, expires } => {
                                        let p_table = format!("private_{to}");
                                        let g_table = format!("group_{to}");
                                        Self::burn_from_tree(db_t.clone(), crc, p_table);

                                        Self::burn_from_tree(db_t.clone(), crc, g_table);
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
                None => {}
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
        let s_key = format!("S-KEY-0-{}", did);
        match tree.get(&s_key) {
            Ok(Some(d)) => Some(d.to_vec()),
            _ => {
                let s_key = format!("S-KEY-1-{}", did);
                match tree.get(&s_key) {
                    Ok(Some(d)) => Some(d.to_vec()),
                    _ => {
                      None       
                    },
                }       
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
}
/// Starts a new p2p node, using the given mem rpc channel.
pub async fn start_node(
    config: luffa_node::config::Config,
    keychain: Keychain<DiskStorage>,
    db: Arc<luffa_store::Store>,
) -> anyhow::Result<(
    PeerId,
    JoinHandle<()>,
    Receiver<NetworkEvent>,
    Sender<luffa_node::rpc::RpcMessage>,
)> {
    tracing::info!("node>>>{config:?}");
    let (mut p2p, sender) = Node::new(config, keychain, db,Some("Luffa".to_string())).await?;
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
