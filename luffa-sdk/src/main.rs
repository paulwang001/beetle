// #![feature(poll_ready)]
use anyhow::Result;
use bip39::{Mnemonic, MnemonicType, Language};
use futures::pending;
use libp2p::PeerId;
use libp2p::identity::PublicKey;
use luffa_rpc_types::{message_to, ChatContent, ContactsEvent, ContactsToken, Message, RtcAction, message_from, AppStatus};
use luffa_sdk::{Callback, Client};
use std::future::{Future, IntoFuture};
use std::sync::mpsc::sync_channel;
use std::sync::RwLock;
use std::task::Poll;
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc, sync::Mutex};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::log::warn;
use tracing::{error, info};
/// CLI arguments support.
use clap::Parser;
use std::path::PathBuf;

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
    fn on_message(&self, crc: u64, from_id: u64, to: u64, msg: Vec<u8>) {
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
    tracing::info!("starting");
    client.init(cfg_path);
    let client = Arc::new(client);
    let client_t = client.clone();
    let client_ctl = client.clone();
    let to_id = args.to;
    let scan = args.scan;
    let tag = args.tag;

    std::thread::spawn(move || {
        loop {
            let process = Process::new(tx.clone());                                                                                                                                  
            let msg = Box::new(process);
            
            client_ctl.start(None,tag.clone(),msg);
            tracing::warn!("started.");
            if timer.elapsed().as_secs() < 120 {
                std::thread::sleep(std::time::Duration::from_secs(120));
            }
            timer = std::time::Instant::now();
            client_ctl.stop();
            tracing::warn!("stoped.");
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    });

    std::thread::spawn(move || {
        let mut x = 0;
        let mut code = String::new();
        loop {
            std::thread::sleep(Duration::from_secs(60));
            let peer_id = client.get_local_id();
            tracing::warn!("peer id: {peer_id:?}");
            let peers = client.relay_list();
            // client.send_msg(to, msg)
            tracing::debug!("{:?}", peers);
            match to_id {
                Some(to_id)=>{
                    match client.find_contacts_tag(to_id) {
                        Some(tag) => {
                            x += 1;
                            tracing::warn!("is man");
                          
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
                            let crc = client.send_msg(to_id, msg);
                            tracing::info!("send seccess {crc}");
                                                    }
                        None => {
                            match scan.as_ref() {
                                Some(scan)=>{
                                    client.contacts_offer(scan);
                                }
                                None=>{
                                    if code.is_empty() {
                                        code = client.show_code().unwrap();
                                    }
                                    tracing::warn!("scan me :{}",code);
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
                None=>{
                    match scan.as_ref() {
                        Some(scan)=>{
                            let crc = client.contacts_offer(scan);

                        }
                        None=>{
                            if code.is_empty() {
                                code = client.show_code().unwrap();
                            }
                            tracing::warn!("scan me :{}",code);
                            let list = client.contacts_list(0).unwrap();
                            for c in list {
                                
                                let ls = client.recent_messages(c.did, 10).unwrap();
                                {
                                    let msg_len = ls.len();
                                    tracing::warn!(" contacts>> {:?} msg_len>>{}", c,msg_len);
            
                                    for crc in ls {
                                        if let Some(meta) = client.read_msg_with_meta(c.did, crc).unwrap() {
                                            let msg = message_from(meta.msg).unwrap();
                                            match &msg {
                                                Message::Chat { content }=>{
            
                                                }
                                                Message::WebRtc { stream_id, action }=>{
            
                                                }
                                                Message::ContactsExchange { exchange }=>{

                                                }
                                                _=>{
                                                    tracing::warn!("[{msg_len}] {:?}",msg);
                                                }
                                            }    
                                        }
                                    }
                                }
                            }
                            let list = client.session_list(10).unwrap();
                            tracing::warn!(" session>> {:?}", list);
            
                            for s in list {
                                let did = s.did;
                                for crc in s.reach_crc {
                                    if let Some(meta) = client.read_msg_with_meta(did, crc).unwrap() {
                                        tracing::warn!("{:?}",meta);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });


    while let Ok((crc, from_id, to, data)) = rx.recv() {
        let msg: Message = serde_cbor::from_slice(&data).unwrap();

        match &msg {
            Message::WebRtc { stream_id, action } => match action {
                RtcAction::Status { timestamp, code,info } => {
                    
                }
                RtcAction::Push { audio_id, video_id }=>{
                    tracing::warn!("{}-----push-----{}",stream_id,audio_id);
                }
                _ => {}
            },
            Message::ContactsExchange { exchange } => match exchange {
                ContactsEvent::Offer { token } => {
                    let ContactsToken { public_key, create_at, sign, secret_key, contacts_type, comment } = token;
                    let pk = PublicKey::from_protobuf_encoding(public_key).unwrap();
                    let peer = PeerId::from_public_key(&pk);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer.to_bytes());
                    let to = digest.sum64();
                    let crc =
                    client_t
                    .contacts_anwser(to, from_id,secret_key.clone());
            }
            ContactsEvent::Answer { token } => {
                    let ContactsToken { public_key, create_at, sign, secret_key, contacts_type, comment } = token;
                    let pk = PublicKey::from_protobuf_encoding(public_key).unwrap();
                    let peer = PeerId::from_public_key(&pk);
                    let mut digest = crc64fast::Digest::new();
                    digest.write(&peer.to_bytes());
                    let to = digest.sum64();
                    let msg = Message::Chat {
                        content: luffa_rpc_types::ChatContent::Send {
                            data: luffa_rpc_types::ContentData::Text {
                                source: luffa_rpc_types::DataSource::Text {
                                    content: format!("Hello {}",comment.clone().unwrap_or_default()),
                                },
                                reference: None,
                            },
                        },
                    };
                    let msg = message_to(msg).unwrap();
                    let crc = client_t.send_msg(to, msg);
                    tracing::warn!("Answer from:offer_id {} ,did {}  ==> {}", from_id, to,crc);
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
                            tracing::debug!("{:?}",meta);
                        }
                    }
                }

                match client_t.read_msg_with_meta(from_id, crc)? {
                    Some(meta)=>{

                    }
                    None=>{
                        tracing::error!("msg not found {}->{}",from_id,crc);
                    }
                }
            }
        }
    }
    Ok(())
}
