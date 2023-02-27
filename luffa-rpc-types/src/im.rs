use std::io::Write;

use anyhow::Result;
use bytes::Bytes;
use libp2p::identity::{Keypair, PublicKey};
use multihash::{Code, MultihashDigest};
use serde::{Deserialize, Serialize};

use aes_gcm::{
    aead::{generic_array::GenericArray, Aead, KeyInit},
    Aes256Gcm, Nonce,
};

pub const KEY_SIZE: usize = 32;
#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub did: u64,
    pub event_time: u64,
    pub crc: u64,
    pub nonce: Option<Vec<u8>>,
    pub msg: Vec<u8>,
}

impl Event {
    pub fn new<T>(did: u64, msg: Message, key: Option<T>) -> Self
    where
        T: AsRef<[u8]>,
    {
        let event_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (msg, nonce) = msg.encrypt(key).unwrap();

        let mut digest = crc64fast::Digest::new();
        digest.write(&msg);
        let crc = digest.sum64();
        // let msg = Bytes::from(msg);
        Self {
            did,
            event_time,
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
        let crc = digest.sum64();
        if self.crc == crc {
            Ok(())
        } else {
            Err(anyhow::anyhow!("crc not matched"))
        }
    }

    /// Encrypt to bytes
    pub fn encode(&self) -> Result<Vec<u8>> {
        bson::to_vec(&self).map_err(|e| anyhow::anyhow!("{e:?}"))
    }
    pub fn decode(data: Vec<u8>) -> Result<Self> {
        let evt: Self = bson::de::from_slice(&data).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        evt.validate()?;
        Ok(evt)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// App  status
    StatusSync {
        did: u64,
        status: AppStatus,
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Contacts {
    pub did: u64,
    pub r#type: ContactsTypes,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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

impl Message {
    pub fn encrypt<T>(&self, key: Option<T>) -> Result<(Vec<u8>, Option<Vec<u8>>)>
    where
        T: AsRef<[u8]>,
    {
        match &self {
            Self::ContactsExchange { .. } | Self::Chat { .. } => {
                let data = bson::to_vec(&self).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                let key = key.unwrap();
                let key = GenericArray::clone_from_slice(key.as_ref());
                let cipher = Aes256Gcm::new(&key);
                let digest = Code::Sha2_256.digest(&data);
                let nonce_data = digest.to_bytes();
                let nonce = Nonce::from_slice(&nonce_data);
                Ok((
                    cipher
                        .encrypt(nonce, &data[..])
                        .map_err(|e| anyhow::anyhow!("{e:?}"))?,
                    Some(nonce_data),
                ))
            }
            _ => Ok((
                bson::to_vec(&self).map_err(|e| anyhow::anyhow!("{e:?}"))?,
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
                let key = GenericArray::clone_from_slice(key.as_ref());
                let cipher = Aes256Gcm::new(&key);
                let nonce = Nonce::from_slice(&nonce_data.as_ref());
                let data = cipher
                    .decrypt(nonce, &data[..])
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;

                bson::de::from_slice::<Self>(&data).map_err(|e| anyhow::anyhow!("{e:?}"))
            }
            _ => bson::de::from_slice::<Self>(&data[..]).map_err(|e| anyhow::anyhow!("{e:?}")),
        }
    }

    pub fn need_encrypt(&self) -> bool {
        match self {
            Self::ContactsExchange { .. } | Self::Chat { .. } => true,
            _ => false,
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
    Receipt {
        status: ReceiptStatus,
    },
    Send {
        publisher_id: u64,
        data: ContentData,
    },
}
#[derive(Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum ReceiptStatus {
    Loaded,
    Shown,
    Failed,
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
