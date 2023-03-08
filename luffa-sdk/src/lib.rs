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
use luffa_rpc_types::{message_from, ChatContent, ContactsTypes, Contacts};
use luffa_rpc_types::{AppStatus, ContactsEvent, ContactsToken, Event, Message};
use luffa_store::{Config as StoreConfig, Store};
use luffa_util::{luffa_config_path, make_config};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use tantivy::collector::TopDocs;
use tantivy::directory::{ManagedDirectory, MmapDirectory};
use tantivy::{doc, Score, DocAddress};
use tantivy::query::QueryParser;
use tantivy::Index;
use tantivy::ReloadPolicy;
use tantivy::{schema::*, IndexWriter};

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
const TOPIC_RELAY: &str = "luffa_relay";
const LUFFA_CONTENT: &str = "index_data";

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = create_runtime();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatSession {
    pub did: u64,
    pub last_time: u64,
    pub tag: String,
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
    sender: Arc<RwLock<Option<tokio::sync::mpsc::Sender<(u64, Vec<u8>, u64)>>>>,
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
        println!("open db>>>>{:?}", &path);
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
                let msg: Message =
                    serde_cbor::from_slice(&msg).map_err(|_e| ClientError::CodeParser)?;
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
                let msg: Message =
                    serde_cbor::from_slice(&msg).map_err(|e| ClientError::CodeParser)?;
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
                                let c = comment.clone();
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
                                    Self::save_contacts(
                                        self.db.clone(),
                                        to,
                                        secret_key,
                                        public_key,
                                        sign,
                                        contacts_type as u8,
                                        c,
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

    pub fn session_list(&self, top: u32) -> Vec<ChatSession> {
        let tree = self.db.open_tree("luffa_sessions").unwrap();

        let mut chats = tree
            .into_iter()
            .map(|item| {
                let (key, val) = item.unwrap();
                let mut key1 = [0u8;8];
                let mut time = [0u8;8];
                key1.copy_from_slice(&key[0..8]);
                time.copy_from_slice(&val[0..8]);

                let did = u64::from_be_bytes(key1);
                ChatSession {
                    did,
                    last_time: u64::from_be_bytes(time),
                    tag: String::from_utf8_lossy(&val[8..]).to_string(),
                }
            })
            .collect::<Vec<_>>();
        chats.sort_by(|a, b| a.last_time.partial_cmp(&b.last_time).unwrap());
        chats.reverse();
        chats.truncate(top as usize);
        chats
    }

    pub fn save_session(&self, did: u64, tag: String) {
        let tree = self.db.open_tree("luffa_sessions").unwrap();
        let key = did.to_be_bytes();
        let time = std::time::SystemTime::now();
        let last_time = time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut data = last_time.to_be_bytes().to_vec();
        data.extend_from_slice(tag.as_bytes());
        tree.insert(key, data).unwrap();
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
            // println!("{}", schema.to_json(&retrieved_doc));
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
                        info!("peers:{peers:?}");
                        return peers
                            .into_iter()
                            .map(|p| bs58::encode(p.to_bytes()).into_string())
                            .collect::<Vec<_>>();
                    }
                    Err(e) => {
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
        println!("cfg_path:{cfg_path:?}");
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

        warn!("config--->{config:?}");
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
                println!("runing...");
                Self::run(db, config, kc, cb, client, rx, idx_writer, schema).await;
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
                if let Err(e) = client_t
                    .gossipsub_subscribe(TopicHash::from_raw(format!("{}_{}", TOPIC_CHAT, my_id)))
                    .await
                {
                    error!("{e:?}");
                    eprintln!("{e:?}");
                    has_err = true;
                }
                let topics = vec![TOPIC_STATUS];
                for t in topics.into_iter() {
                    if let Err(e) = client_t.gossipsub_subscribe(TopicHash::from_raw(t)).await {
                        error!("{e:?}");
                        eprintln!("{e:?}");
                        has_err = true;
                        break;
                    }
                }

                if !has_err {
                    info!("subscribed all as client,status sync");
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
                        warn!("{e:?}");
                        eprintln!("{e:?}");
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
                        }  
                    })
                    .collect::<Vec<_>>();
                    let mut itr = contacts.iter();
                    while let Some(ctt) = itr.next() {
                        if ctt.r#type == ContactsTypes::Private {
                            continue;
                        }
                        let topic = TopicHash::from_raw(format!("{}_{}", TOPIC_CHAT, ctt.did));
                        if let Err(e) = client_t.gossipsub_subscribe(topic).await {
                            warn!("{e:?}");
                        }
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
                        warn!("{e:?}");
                        eprintln!("{e:?}");
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
                    NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {
                        // TODO: a group member or my friend online?
                        info!("Subscribed> peer_id: {peer_id:?} topic:{topic}");
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
                            info!("Gossipsub> peer_id: {from:?} msg:{}", msg.len());
                            // TODO check did status
                            if nonce.is_none() {
                                if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                    bytes::Bytes::from(msg),
                                    None,
                                    nonce,
                                ) {
                                    // TODO: did is me or I'm a member any local group
                                    info!("Gossipsub> on_message peer_id: {from:?} msg:{:?}", msg);
                                    let data = serde_cbor::to_vec(&msg).unwrap();
                                    let cb = &*cb;
                                    cb.on_message(crc, from_id, to, data);
                                    // match msg {
                                    //     Message::RelayNode { did }=>{

                                    //     }
                                    //     Message::StatusSync { to, from_id, status }=>{

                                    //     }
                                    //     _=>{

                                    //     }
                                    // }
                                    // todo!()
                                }
                            } else {
                                match Self::get_aes_key_from_contacts(db_t.clone(), from_id) {
                                    Some(key) => {
                                        if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                            bytes::Bytes::from(msg),
                                            Some(key.clone()),
                                            nonce,
                                        ) {
                                            // TODO: did is me or I'm a member any local group
                                            let data = serde_cbor::to_vec(&msg).unwrap();
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
                                            cb.on_message(crc, from_id, to, data);
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
                                                error!("{e:?}");
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
                                                                        "{}_{}",
                                                                        TOPIC_CHAT, from_id
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

                                                            let msg_type = if to == my_id {
                                                                format!("content_private")
                                                            } else {
                                                                format!("content_group")
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
                                                let cb = &*cb;
                                                cb.on_message(crc, from_id, to, data);
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
                        tracing::info!("---------PeerDisconnected-----------{:?}", peer_id);
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
                        tracing::info!("---------CancelLookupQuery-----------{:?}", peer_id);
                    }
                }
            }
        });

        let db_t = db.clone();
        while let Some((to, msg_data, msg_id)) = receiver.recv().await {
            if to == u64::MAX {
                break;
            }
            let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();

            let evt = if msg.need_encrypt() {
                match Self::get_aes_key_from_contacts(db.clone(), to) {
                    Some(key) => Some(Event::new(to, &msg, Some(key), msg_id)),
                    None => Some(Event::new(to, &msg, None, msg_id)),
                }
            } else {
                Some(Event::new(to, &msg, None, msg_id))
            };
            match evt {
                Some(e) => {
                    let data = e.encode().unwrap();
                    let topic_hash = if to == 0 {
                        TopicHash::from_raw(TOPIC_STATUS)
                    } else {
                        TopicHash::from_raw(format!("{}_{}", TOPIC_CHAT, to))
                    };
                    if let Err(e) = client
                        .gossipsub_publish(topic_hash, bytes::Bytes::from(data))
                        .await
                    {
                        error!("{e:?}");
                        eprintln!("{e:?}");
                    } else if to > 0 {
                        let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
                        let Event {
                            to,
                            event_time,
                            crc,
                            from_id,
                            ..
                        } = e;
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

                                        let msg_type = if to == my_id {
                                            format!("content_private")
                                        } else {
                                            format!("content_group")
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
        let s_key = format!("S-KEY-{}", did);
        match tree.get(&s_key) {
            Ok(Some(d)) => Some(d.to_vec()),
            _ => None,
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
    println!("node>>>{config:?}");
    let (mut p2p, sender) = Node::new(config, keychain, db).await?;
    let events = p2p.network_events();
    let local_id = p2p.local_peer_id().clone();
    // Start services
    let p2p_task = task::spawn(async move {
        println!("p2p runnung..");
        if let Err(err) = p2p.run().await {
            error!("{:?}", err);
            eprintln!("{err:?}");
        } else {
            eprintln!("p2p run exit!")
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

    info!("Opening store at {}", config.path.display());
    let store = Store::create(config)
        .await
        .context("failed to open existing store")?;
    Ok(store)
}

include!(concat!(env!("OUT_DIR"), "/luffa_sdk.uniffi.rs"));
