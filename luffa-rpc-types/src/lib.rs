// pub mod addr;
pub mod p2p;
// pub mod store;

use std::fmt;

// pub use addr::Addr;

use serde::{Deserialize, Serialize};

use std::io::Write;

use anyhow::Result;
use bytes::Bytes;
use libp2p::identity::{Keypair, PublicKey};
use multihash::{Code, MultihashDigest};

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use chrono::Utc;

pub trait NamedService {
    const NAME: &'static str;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcError(serde_error::Error);

impl std::error::Error for RpcError {}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<anyhow::Error> for RpcError {
    fn from(e: anyhow::Error) -> Self {
        RpcError(serde_error::Error::new(&*e))
    }
}

pub type RpcResult<T> = std::result::Result<T, RpcError>;

#[derive(Serialize, Deserialize, Debug)]
pub struct WatchRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct WatchResponse {
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionResponse {
    pub version: String,
}

pub const KEY_SIZE: usize = 32;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Session {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    // zero is all
    pub to: u64,
    pub event_time: u64,
    pub crc: u64,
    pub from_id: u64,
    pub nonce: Option<Vec<u8>>,
    pub msg: Vec<u8>,
}

impl Event {
    pub fn new(
        to: u64,
        msg: &Message,
        key: Option<Vec<u8>>,
        from_id: u64,
        last_crc: Option<u64>,
    ) -> Self {
        let event_time = Utc::now().timestamp_millis() as u64;
        let is_important = msg.is_important();
        let (msg, mut nonce) = msg.encrypt(key).unwrap();
        let mut digest = crc64fast::Digest::new();
        digest.write(&msg);
        digest.write(&to.to_be_bytes());
        digest.write(&event_time.to_be_bytes());
        digest.write(&from_id.to_be_bytes());
        let crc = digest.sum64();
        if is_important {
            nonce.as_mut().map(|nc| {
                let last = last_crc.unwrap_or(crc).to_be_bytes();
                nc[12..20].clone_from_slice(&last);
                let mut digest = crc64fast::Digest::new();
                digest.write(&last);
                digest.write(&crc.to_be_bytes());
                let chat_crc = digest.sum64();
                nc[20..28].clone_from_slice(&chat_crc.to_be_bytes());
            });
        }
        Self {
            to,
            event_time,
            from_id,
            nonce,
            crc,
            msg,
        }
    }

    fn validate(&self) -> Result<()> {
        let now = Utc::now().timestamp_millis() as u64;
        if self.event_time < now - 60 * 1000 {
            //return Err(anyhow::anyhow!("event time is invalid."));
        }
        let mut digest = crc64fast::Digest::new();
        digest.write(self.msg.as_ref());
        digest.write(&self.to.to_be_bytes());
        digest.write(&self.event_time.to_be_bytes());
        digest.write(&self.from_id.to_be_bytes());
        let crc = digest.sum64();
        if self.crc == crc {
            Ok(())
        } else {
            Err(anyhow::anyhow!("crc not matched"))
        }
    }

    /// Encrypt to bytes
    pub fn encode(&self) -> Result<Vec<u8>> {
        serde_cbor::to_vec(&self).map_err(|e| anyhow::anyhow!("{e:?}"))
    }
    pub fn decode(data: &Vec<u8>) -> Result<Self> {
        let evt: Self = serde_cbor::from_slice(data).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        evt.validate()?;
        Ok(evt)
    }
    pub fn decode_uncheck(data: &Vec<u8>) -> Result<Self> {
        let evt: Self = serde_cbor::from_slice(data).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(evt)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    /// App  status
    StatusSync {
        to: u64,
        from_id: u64,
        status: AppStatus,
    },

    Feedback {
        crc: Vec<u64>,
        from_id: Option<u64>,
        to_id: Option<u64>,
        status: FeedbackStatus,
    },

    /// I'm a relay
    RelayNode {
        did: u64,
    },

    /// publish all contacts to relay network
    ContactsSync {
        did: u64,
        contacts: Vec<Contacts>,
    },

    /// exchange contacts offer or answer
    ContactsExchange {
        exchange: ContactsEvent,
    },

    Chat {
        content: ChatContent,
    },
    WebRtc {
        action: RtcAction,
    },
    Ping {
        relay_id: u64,
        ttl_ms: u64,
    },
    InnerError {
        /// 1 not found decrypt key
        /// 2 decrypt message failed
        kind: u8,
        reason: String,
    },
}

#[repr(u8)]
pub enum InnerErrorKind {
    NotFoundSecurityKey = 1,
    DecryptFailed = 2,
}

impl Message {
    pub fn text(content: String) -> Self {
        Message::Chat {
            content: ChatContent::Send {
                data: ContentData::Text {
                    source: DataSource::Text { content },
                    reference: None,
                },
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RtcAction {
    Status { code: u32, info: String },
    Offer { audio_id: u32, video_id: u32 },
    Answer { audio_id: u32, video_id: u32 },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contacts {
    pub did: u64,
    pub r#type: ContactsTypes,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ContactsTypes {
    Private,
    Group,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum AppStatus {
    Active,
    Deactive,
    Disconnected,
    Connected,
    Bye,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum FeedbackStatus {
    Sending,
    Send,
    Routing,
    Route,
    Reach,
    Read,
    Fetch,
    Notice,
    Failed,
    Reject,
}

impl Message {
    pub fn encrypt<T>(&self, key: Option<T>) -> Result<(Vec<u8>, Option<Vec<u8>>)>
    where
        T: AsRef<[u8]>,
    {
        match &self {
            Self::ContactsExchange { .. } | Self::Chat { .. } | Self::WebRtc { .. } => {
                let data = serde_cbor::to_vec(&self).map_err(|e| anyhow::anyhow!("{e:?}"))?;

                let cipher = match key {
                    Some(k) => Some(Aes256Gcm::new_from_slice(k.as_ref()).unwrap()),
                    None => {
                        tracing::info!("----------");
                        self.exchange_key().map(|v| {
                            tracing::info!("----------{:?}", v);

                            match Aes256Gcm::new_from_slice(&v) {
                                Ok(a) => a,
                                Err(e) => {
                                    panic!("{e:?}");
                                }
                            }
                        })
                    }
                };
                if cipher.is_none() {
                    tracing::info!("cipher is none");
                    return Err(anyhow::anyhow!("cipher is none"));
                }

                let cipher = cipher.unwrap();
                let digest = Code::Sha2_256.digest(&data);
                let mut nonce_data = digest.to_bytes();
                if self.is_important() {
                    nonce_data[31] = u8::MAX;
                } else {
                    nonce_data[31] = 0;
                }
                let nonce = Nonce::from_slice(&nonce_data[0..12]);
                Ok((
                    cipher
                        .encrypt(nonce, &data[..])
                        .map_err(|e| anyhow::anyhow!("{e:?}"))?,
                    Some(nonce_data),
                ))
            }
            _ => Ok((
                serde_cbor::to_vec(&self).map_err(|e| anyhow::anyhow!("{e:?}"))?,
                None,
            )),
        }
    }

    pub fn decrypt<T>(data: Bytes, key: Option<T>, nonce: Option<T>) -> Result<Self>
    where
        T: AsRef<[u8]>,
    {
        match (key, nonce) {
            (Some(key), Some(nonce_data)) => {
                let cipher = Aes256Gcm::new_from_slice(key.as_ref()).unwrap();
                let nonce = Nonce::from_slice(&nonce_data.as_ref()[0..12]);
                let data = cipher
                    .decrypt(nonce, &data[..])
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;

                serde_cbor::from_slice::<Self>(&data).map_err(|e| anyhow::anyhow!("{e:?}"))
            }
            _ => serde_cbor::from_slice::<Self>(&data[..]).map_err(|e| anyhow::anyhow!("{e:?}")),
        }
    }

    pub fn need_encrypt(&self) -> bool {
        match self {
            Self::ContactsExchange { .. } | Self::Chat { .. } | Self::WebRtc { .. } => true,
            _ => false,
        }
    }
    pub fn is_contacts_exchange(&self) -> bool {
        match self {
            Self::ContactsExchange { .. } => true,
            _ => false,
        }
    }
    pub fn chat_content(&self) -> Option<&ContentData> {
        match self {
            Self::Chat { content } => match content {
                ChatContent::Send { data } => Some(data),
                _ => None,
            },
            _ => None,
        }
    }
    pub fn chat_feedback(&self) -> Option<(u64, FeedbackStatus)> {
        match self {
            Self::Chat { content } => match content {
                ChatContent::Feedback { crc, status, .. } => Some((*crc, *status)),
                _ => None,
            },
            _ => None,
        }
    }
    pub fn is_important(&self) -> bool {
        match self {
            Message::WebRtc { action, .. } => match action {
                _ => false,
            },
            Message::ContactsExchange { exchange } => match exchange {
                ContactsEvent::Offer { .. } => true,
                _ => false,
            },
            Message::Chat { content } => match content {
                ChatContent::Send { .. } => true,
                _ => false,
            },
            _ => false,
        }
    }
    pub fn exchange_key(&self) -> Option<Vec<u8>> {
        match &self {
            Message::ContactsExchange { exchange } => match exchange {
                ContactsEvent::Answer { token, .. } => Some(token.secret_key.clone()),
                ContactsEvent::Offer { token } => Some(token.secret_key.clone()),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ContactsEvent {
    Offer {
        token: ContactsToken,
    },
    Answer {
        token: ContactsToken,
        members: Vec<Member>,
        offer_crc: u64,
    },
    Reject {
        offer_crc: u64,
        public_key: Vec<u8>,
    },
    Join {
        offer_crc: u64,
        group_nickname: String,
    },
    Leave {
        id: u64,
    },
    Sync {
        g_id: u64,
        members: Vec<Member>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Member {
    pub u_id: u64,
    pub group_nickname: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContactsToken {
    pub public_key: Vec<u8>,
    pub group_key: Option<Vec<u8>>,
    pub create_at: u64,
    pub sign: Vec<u8>,
    pub secret_key: Vec<u8>,
    pub contacts_type: ContactsTypes,
    pub comment: Option<String>,
}

impl ContactsToken {
    pub fn new(
        local_key: &Keypair,
        comment: Option<String>,
        secret_key: Vec<u8>,
        contacts_type: ContactsTypes,
    ) -> Result<Self> {
        let create_at = Utc::now().timestamp_millis() as u64;
        let public_key = local_key.public().to_protobuf_encoding();
        let group_key = match contacts_type {
            ContactsTypes::Group => {
                let key = local_key.to_protobuf_encoding()?;
                Some(key)
            }
            ContactsTypes::Private => None,
        };
        let mut buf = vec![];
        buf.extend_from_slice(&create_at.to_be_bytes());
        // buf.extend_from_slice(&secret_key);
        let tp = contacts_type.clone() as u8;
        buf.extend_from_slice(&[tp]);
        if let Some(c) = comment.as_ref() {
            buf.extend_from_slice(c.as_bytes());
        }
        let sign = local_key.sign(&buf)?;
        Ok(Self {
            public_key,
            group_key,
            create_at,
            sign,
            secret_key,
            contacts_type,
            comment,
        })
    }

    pub fn group_answer(
        contacts_type: ContactsTypes,
        public_key: Vec<u8>,
        secret_key: Vec<u8>,
        comment: Option<String>,
    ) -> Result<Self> {
        let create_at = Utc::now().timestamp_millis() as u64;
        let sign = vec![];
        Ok(Self {
            public_key,
            group_key: None,
            create_at,
            sign,
            secret_key,
            contacts_type,
            comment,
        })
    }

    pub fn validate(&self) -> Result<bool> {
        let Self {
            public_key,
            create_at,
            sign,
            contacts_type,
            comment,
            ..
        } = self;
        if contacts_type == &ContactsTypes::Group && sign.is_empty() {
            return Ok(true);
        }
        let mut buf = vec![];
        buf.write(&create_at.to_be_bytes())?;
        // buf.write(secret_key)?;
        let tp = contacts_type.clone() as u8;
        buf.write(&[tp])?;
        if let Some(c) = comment {
            buf.write(c.as_bytes())?;
        }
        let key = PublicKey::from_protobuf_encoding(public_key)?;
        Ok(key.verify(&buf, &sign))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ChatContent {
    Feedback {
        crc: u64,
        last_crc: u64,
        status: FeedbackStatus,
    },
    Burn {
        crc: u64,
        expires: u64,
    },
    Send {
        data: ContentData,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ContentData {
    Text {
        source: DataSource,
        reference: Option<String>,
    },
    Link {
        txt: String,
        url: String,
        reference: Option<String>,
    },
    Media {
        title: String,
        m_type: MediaTypes,
        source: DataSource,
    },
}
#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum MediaTypes {
    File,
    Image,
    Audio,
    Video,
    Html,
    Markdown,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DataSource {
    Cid { cid: Vec<u8> },
    Raw { data: Vec<u8> },
    Text { content: String },
}

pub fn message_from(msg: Vec<u8>) -> Option<Message> {
    match serde_cbor::from_slice::<Message>(&msg) {
        Ok(msg) => Some(msg),
        _ => None,
    }
}

pub fn message_to(msg: Message) -> Option<Vec<u8>> {
    match serde_cbor::to_vec(&msg) {
        Ok(msg) => Some(msg),
        _ => None,
    }
}

include!(concat!(env!("OUT_DIR"), "/luffa_rpc_types.uniffi.rs"));
