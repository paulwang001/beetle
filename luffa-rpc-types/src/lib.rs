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
    aead::{generic_array::GenericArray, Aead, KeyInit},
    Aes256Gcm, Nonce,
};

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
#[derive(Debug, Serialize, Deserialize)]
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
    pub fn new(to: u64, msg: &Message, key: Option<Vec<u8>>, from_id: u64) -> Self
    {
        let event_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (msg, nonce) = msg.encrypt(key).unwrap();

        let mut digest = crc64fast::Digest::new();
        digest.write(&msg);
        digest.write(&to.to_be_bytes());
        digest.write(&event_time.to_be_bytes());
        digest.write(&from_id.to_be_bytes());
        let crc = digest.sum64();
        // let msg = Bytes::from(msg);
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if self.event_time < now - 60 {
            return Err(anyhow::anyhow!("event time is invalid."));
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
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// App  status
    StatusSync {
        to: u64,
        from_id: u64,
        status: AppStatus,
    },

    Feedback {
       crc:u64,
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
        stream_id: u32,
        action: RtcAction,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RtcAction {
    Push {
        audio_id: u32,
        video_id: u32,
    },
    Pull {
        audio_id: u32,
        video_id: u32,
    },
    Reject {
        audio_id: u32,
        video_id: u32,
    },
    Status {
        timestamp:u64,
        code:String,
    },
    Offer { dsp: String },
    Answer { dsp: String },
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Contacts {
    pub did: u64,
    pub r#type: ContactsTypes,
    pub have_time:u64,
    pub wants:Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy,PartialEq, Eq)]
#[repr(u8)]
pub enum ContactsTypes {
    Private,
    Group,
}

#[derive(Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum AppStatus {
    Active,
    Deactive,
    Disconnected,
    Connected,
    Bye,
}

#[derive(Debug,Serialize,Deserialize)]
#[repr(u8)]
pub enum FeedbackStatus {
    Reach,
    Read,
    Fetch,
    Notice,
}

impl Message {
    pub fn encrypt<T>(&self, key: Option<T>) -> Result<(Vec<u8>, Option<Vec<u8>>)>
    where
        T: AsRef<[u8]>,
    {
        match &self {
            
            Self::ContactsExchange { .. } | Self::Chat { .. } => {
                let data = serde_cbor::to_vec(&self).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                
                let cipher = 
                match key {
                    Some(k)=> Some(Aes256Gcm::new_from_slice(k.as_ref()).unwrap()),
                    None=>{
                        tracing::warn!("----------");
                        self.exchange_key().map(|v| {
                            tracing::warn!("----------{:?}",v);
                            
                            match Aes256Gcm::new_from_slice(&v) {
                                Ok(a)=> a,
                                Err(e)=>{
                                    panic!("{e:?}");
                                }
                            }
                        })
                    }
                };
                if cipher.is_none() {
                    tracing::warn!("cipher is none");
                    return Err(anyhow::anyhow!("cipher is none"));
                }
                tracing::info!("00000000000000");
                let cipher = cipher.unwrap();
                let digest = Code::Sha2_256.digest(&data);
                let nonce_data = digest.to_bytes();
                let nonce = Nonce::from_slice(&nonce_data[0..12]);
                Ok((
                    cipher
                        .encrypt(nonce, &data[..])
                        .map_err(|e| anyhow::anyhow!("{e:?}"))?,
                    Some(nonce.to_vec()),
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
                let nonce = Nonce::from_slice(&nonce_data.as_ref());
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
            Self::ContactsExchange { .. } | Self::Chat { .. } => true,
            _ => false,
        }
    }
    pub fn is_contacts_exchange(&self) -> bool {
        match self {
            Self::ContactsExchange { .. }=> true,
            Self::WebRtc { stream_id, action }=> true,
            _ => false,
        }
    }



    pub fn exchange_key(&self) -> Option<Vec<u8>>
    {
        match &self {
            Message::ContactsExchange { exchange }=>{
                match exchange {
                    ContactsEvent::Answer { token } =>{
                        Some(token.secret_key.clone())
                    }
                    ContactsEvent::Offer { token }=>{
                        Some(token.secret_key.clone())
                    }
                    
                }
            }
            _=>{
                None
            }
        }

    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ContactsEvent {
    Offer { token: ContactsToken },
    Answer { token: ContactsToken },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContactsToken {
    pub public_key: Vec<u8>,
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
        let create_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let public_key = local_key.public().to_protobuf_encoding();
        let mut buf = vec![];
        buf.extend_from_slice(&create_at.to_be_bytes());
        buf.extend_from_slice(&secret_key);
        let tp = contacts_type.clone() as u8;
        buf.extend_from_slice(&[tp]);
        if let Some(c) = comment.as_ref() {
            buf.extend_from_slice(c.as_bytes());
        }
        let sign = local_key.sign(&buf)?;
        Ok(Self {
            public_key,
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
            secret_key,
            contacts_type,
            comment,
        } = self;
        let mut buf = vec![];
        buf.write(&create_at.to_be_bytes())?;
        buf.write(secret_key)?;
        let tp = contacts_type.clone() as u8;
        buf.write(&[tp])?;
        if let Some(c) = comment {
            buf.write(c.as_bytes())?;
        }
        let key = PublicKey::from_protobuf_encoding(public_key)?;
        Ok(key.verify(&buf, &sign))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ChatContent {
    Feedback {
        crc:u64,
        status: FeedbackStatus,
    },
    Burn {
        crc:u64,
        expires: u64,
    },
    Send {
        data: ContentData,
    },
}

#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum MediaTypes {
    File,
    Image,
    Audio,
    Video,
    Html,
    Markdown,
}
#[derive(Debug, Serialize, Deserialize)]
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
