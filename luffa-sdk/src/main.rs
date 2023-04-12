// #![feature(poll_ready)]
use anyhow::Result;
use bip39::{Language, Mnemonic, MnemonicType};
/// CLI arguments support.
use clap::Parser;
use futures::pending;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use luffa_rpc_types::{
    message_from, message_to, AppStatus, ChatContent, ContactsEvent, ContactsToken, Message,
    RtcAction,
};
use luffa_sdk::{Callback, Client, OfferStatus};
use std::future::{Future, IntoFuture};
use std::path::PathBuf;
use std::sync::mpsc::sync_channel;
use std::sync::RwLock;
use std::task::Poll;
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc, sync::Mutex};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::log::warn;
use tracing::{error, info};

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long)]
    to: Option<u64>,
    #[clap(long)]
    scan: Option<String>,
    #[clap(long)]
    pub cfg: Option<String>,
    #[clap(long)]
    pub tag: Option<String>,
}

#[derive(Debug)]
struct Process {
    tx: std::sync::mpsc::SyncSender<(u64, u64, u64, Vec<u8>)>,
}

impl Process {
    pub fn new(tx: std::sync::mpsc::SyncSender<(u64, u64, u64, Vec<u8>)>) -> Self {
        Self { tx }
    }
}

impl Callback for Process {
    fn on_message(&self, crc: u64, from_id: u64, to: u64, event_time: u64, msg: Vec<u8>) {
        self.tx.send((crc, from_id, to, msg)).unwrap();
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let client = Client::new();
    let mut timer = std::time::Instant::now();
    let (tx, rx) = sync_channel(1024);

    // let tx = Arc::new(tx);
    let cfg_path = args.cfg;
    // let msg_t = msg.clone();
    tracing::info!("init...");
    client.init(cfg_path).unwrap();

    tracing::info!("inited.");
    let client = Arc::new(client);
    let client_t = client.clone();
    let client_ctl = client.clone();
    let to_id = args.to;
    let scan = args.scan;
    let tag = args.tag;

    let process = Process::new(tx.clone());
    let msg = Box::new(process);

    let id = client_ctl.start(None, tag.clone(), msg).unwrap();
    tracing::info!("started. {id}");
    // std::thread::spawn(move || {
    //     loop {
    //         let process = Process::new(tx.clone());
    //         let msg = Box::new(process);

    //         client_ctl.start(None,tag.clone(),msg).unwrap();
    //         tracing::info!("started.");
    //         if timer.elapsed().as_secs() < 3600 {
    //             std::thread::sleep(std::time::Duration::from_secs(3600));
    //         }
    //         timer = std::time::Instant::now();
    //         client_ctl.stop().unwrap();
    //         tracing::info!("stoped.");
    //         std::thread::sleep(std::time::Duration::from_secs(5));
    //     }
    // });

    std::thread::spawn(move || {
        let mut x = 0;
        let mut code = String::new();
        loop {
            std::thread::sleep(Duration::from_secs(10));
            // let peer_id = client.get_local_id().unwrap();
            std::thread::sleep(Duration::from_secs(1));
            let peer_id = client.get_local_id().unwrap().unwrap();
            tracing::warn!("peer id: {peer_id:?}");
            let peers = client.relay_list();
            // client.send_msg(to, msg)
            tracing::debug!("{:?}", peers);
            match to_id {
                Some(to_id) => {
                    match client.find_contacts_tag(to_id).unwrap() {
                        Some(tag) => {
                            x += 1;
                            tracing::info!("is man");

                            // let msg = Message::WebRtc { stream_id: 1000, action: RtcAction::Push { audio_id: 2, video_id: 3 } };
                            let mnemonic = Mnemonic::new(MnemonicType::Words24, Language::English);
                            let msg = Message::Chat {
                                content: luffa_rpc_types::ChatContent::Send {
                                    data: luffa_rpc_types::ContentData::Text {
                                        source: luffa_rpc_types::DataSource::Text {
                                            content: mnemonic.into_phrase(),
                                        },
                                        reference: None,
                                    },
                                },
                            };
                            let msg = message_to(msg).unwrap();
                            let crc = client.send_msg(to_id, msg).unwrap();
                            tracing::warn!("send seccess {crc}");
                        }
                        None => {
                            match scan.as_ref() {
                                Some(scan) => {
                                    client.contacts_offer(scan).unwrap();
                                }
                                None => {
                                    if code.is_empty() {
                                        code = client.show_code("https://luffa.putdev.com", "p").unwrap().unwrap();
                                    }
                                    tracing::warn!("scan me :{}", code);
                                }
                            }
                            // client.contacts_offer(&code);
                            // let msg = Message::WebRtc {
                            //     stream_id: 0,
                            //     action: luffa_rpc_types::RtcAction::Status { timestamp: 0, code },
                            // };
                            // let msg = Message::StatusSync { to:to_id, from_id: 0, status: AppStatus::Deactive };
                            // msg
                        }
                    };
                }
                None => match scan.as_ref() {
                    Some(scan) => {
                        let crc = client.contacts_offer(scan);
                    }
                    None => {
                        if code.is_empty() {
                            code = client.show_code("https://luffa.putdev.com", "p").unwrap().unwrap();
                        }
                        let relays = client.relay_list().unwrap();
                        tracing::warn!("scan me :{}  --->>{:?}", code, relays);
                        let offers = client.recent_offser(10).unwrap();
                        for offer in offers  {
                            tracing::warn!("offer>>> {offer:?}");
                            if offer.status == OfferStatus::Offer {
                                let ret = client.contacts_anwser(offer.did, offer.offer_crc).unwrap();
                                tracing::warn!("anwser>>> {ret}");
                            }
                        }
                        let list = client.contacts_list(0).unwrap();
                        let mut members = vec![];
                        for c in list {
                            members.push(c.did);
                            let ls = client.recent_messages(c.did, 0,10).unwrap();
                            {
                                let msg_len = ls.len();
                                tracing::info!(" contacts>> {:?} msg_len>>{}", c, msg_len);

                                for crc in ls {
                                    if let Some(meta) =
                                        client.read_msg_with_meta(c.did, crc).unwrap()
                                    {
                                        let msg = message_from(meta.msg).unwrap();
                                        match &msg {
                                            Message::Chat { content } => {}
                                            Message::WebRtc { stream_id, action } => {}
                                            Message::ContactsExchange { exchange } => {}
                                            _ => {
                                                tracing::info!("[{msg_len}] {:?}", msg);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        let groups = client.contacts_list(1).unwrap();
                        if !members.is_empty() && groups.len() < 1 {
                            let created = client.contacts_group_create(members, None).is_ok();
                            tracing::warn!("group created:{created}");
                        }
                        if !groups.is_empty() {
                            let x:usize = rand::random();
                            let x = x % groups.len();
                            for (i,g) in groups.into_iter().enumerate() {
                                if x != i {
                                    continue;
                                }
                                let msgs = client.recent_messages(g.did, 0, 10000).unwrap();
                                let msg_len = msgs.len();
                                let mut from_count = 0_u32 ;
                                for msg in msgs {
                                    if let Ok(Some(evt)) = client.read_msg_with_meta(g.did, msg) {
                                        if evt.from_id != peer_id {

                                            let msg = message_from(evt.msg).unwrap();
                                            tracing::info!("evt:from> {} >msg: {:?}",evt.from_id, msg);
                                            from_count += 1;
                                        }
                                    }

                                }
                                // if from_count == 0 {
                                //     let created = client.contacts_group_create(members.clone(), Some(g.tag.clone())).unwrap();
                                //     tracing::warn!("empty msg>>> group created:{created} [{}] invitee:{:?}",g.tag,&members);
                                //     continue;
                                // }
                                let mnemonic = Mnemonic::new(MnemonicType::Words24, Language::English);
                                let msg = Message::Chat {
                                    content: luffa_rpc_types::ChatContent::Send {
                                        data: luffa_rpc_types::ContentData::Text {
                                            source: luffa_rpc_types::DataSource::Text {
                                                content: mnemonic.into_phrase(),
                                            },
                                            reference: None,
                                        },
                                    },
                                };
                                let msg = message_to(msg).unwrap();
                                let crc = client.send_msg(g.did, msg).unwrap();
                                tracing::error!("[len: {from_count}] group [{g:?}] msg send seccess {crc}");
                            }
                        }
                        let list = client.session_list(10).unwrap();
                        tracing::info!(" session>> {:?}", list);

                        for s in list {
                            let did = s.did;
                            for crc in s.reach_crc {
                                if let Some(meta) = client.read_msg_with_meta(did, crc).unwrap() {
                                    tracing::warn!("read msg> {} ,from: {} to:{}", crc,meta.from_id,meta.to_id);
                                }
                            }
                        }
                    }
                },
            }
        }
    });

    while let Ok((crc, from_id, to, data)) = rx.recv() {
        let msg: Message = serde_cbor::from_slice(&data).unwrap();
        let peer_id = client_t.get_local_id().unwrap().unwrap();
        let did = if to == peer_id { from_id } else {to};
        match &msg {
            Message::Ping { relay_id, ttl_ms }=>{
                tracing::info!("-----relay------{} ---ttl:{} ms",relay_id,ttl_ms);
            }
            Message::WebRtc { stream_id, action } => match action {
                RtcAction::Status {
                    timestamp,
                    code,
                    info,
                } => {}
                RtcAction::Push { audio_id, video_id } => {
                    tracing::info!("{}-----push-----{}", stream_id, audio_id);
                }
                _ => {}
            },
            Message::ContactsExchange { exchange } => match exchange {
                ContactsEvent::Offer { token } => {
                    let ContactsToken {
                        public_key,
                        create_at,
                        sign,
                        secret_key,
                        contacts_type,
                        comment,
                        ..
                    } = token;
                    let pk = PublicKey::from_protobuf_encoding(public_key).unwrap();
                    let peer = PeerId::from_public_key(&pk);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer.to_bytes());
                    let to = digest.sum64();
                    // let crc = client_t.contacts_anwser(to, from_id, crc,secret_key.clone(),secret_key);
                }
                ContactsEvent::Answer {offer_crc ,token } => {
                    let ContactsToken {
                        public_key,
                        create_at,
                        sign,
                        secret_key,
                        contacts_type,
                        comment,
                        ..
                    } = token;
                    let pk = PublicKey::from_protobuf_encoding(public_key).unwrap();
                    let peer = PeerId::from_public_key(&pk);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer.to_bytes());
                    let to = digest.sum64();
                    let msg = Message::Chat {
                        content: luffa_rpc_types::ChatContent::Send {
                            data: luffa_rpc_types::ContentData::Text {
                                source: luffa_rpc_types::DataSource::Text {
                                    content: format!(
                                        "Hello {}",
                                        comment.clone().unwrap_or_default()
                                    ),
                                },
                                reference: None,
                            },
                        },
                    };
                    let msg = message_to(msg).unwrap();
                    let crc = client_t.send_msg(to, msg).unwrap();
                    tracing::info!("Answer from:offer_id {} ,did {}  ==> {}", from_id, to, crc);
                }
                _=> {
                    
                }
            },
            Message::Chat { .. } => match client_t.read_msg_with_meta(did, crc)? {
                Some(meta) => {
                    let msg = message_from(meta.msg.clone()).unwrap();
                    tracing::error!("on message meta>>crc: {crc}  from: {} to: {} >> {:?}", meta.from_id,meta.to_id,msg );
                }
                None => {
                    tracing::error!("msg not found {}->{}", did, crc);
                }
            },
            _ => {
                let list = client_t.contacts_list(0)?;
                tracing::debug!("contacts>> {:?}", list);
                let list = client_t.session_list(10)?;

                tracing::debug!(" session>> {:?}", list);

                for s in list {
                    let did = s.did;
                    for crc in s.reach_crc {
                        if let Some(meta) = client_t.read_msg_with_meta(did, crc)? {
                            tracing::debug!("{:?}", meta);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
