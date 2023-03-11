#![feature(poll_ready)]
use anyhow::Result;
use futures::pending;
use luffa_rpc_types::{message_to, ChatContent, ContactsEvent, ContactsToken, Message, RtcAction};
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
    let (tx, rx) = sync_channel(1024);

    let process = Process::new(tx);

    let msg = Box::new(process);
    let to_id = std::env::args().nth(1).unwrap_or_default();
    let to_id: u64 = to_id.parse().unwrap_or_default();
    // let msg_t = Arc::new(msg.clone());
    let client = Client::new();
    let cfg_path = std::env::args().nth(2);
    // let msg_t = msg.clone();
    tracing::info!("starting");
    client.start(cfg_path, msg);
    tracing::info!("started.");
    let client = Arc::new(client);
    let client_t = client.clone();
    std::thread::spawn(move || {
        let mut x = 0;
        loop {
            std::thread::sleep(Duration::from_secs(5));
            let peer_id = client.get_local_id();
            tracing::info!("peer id: {peer_id:?}");
            let peers = client.relay_list();
            // client.send_msg(to, msg)
            tracing::info!("{:?}", peers);
            if to_id > 0 {
                let msg = match client.find_contacts_tag(to_id) {
                    Some(tag) => {
                        x += 1;
                        tracing::warn!("is man");
                        Message::Chat {
                            content: ChatContent::Send {
                                data: luffa_rpc_types::ContentData::Text {
                                    source: luffa_rpc_types::DataSource::Text {
                                        content: format!("{tag} Hello - {x}"),
                                    },
                                    reference: None,
                                },
                            },
                        }
                    }
                    None => {
                        let code = client.show_code(Some("Hello".to_owned()));
                        let msg = Message::WebRtc {
                            stream_id: 0,
                            action: luffa_rpc_types::RtcAction::Status { timestamp: 0, code },
                        };
                        msg
                    }
                };

                let msg = message_to(msg).unwrap();
                match client.send_msg(to_id, msg) {
                    Ok(_) => {
                        tracing::info!("send seccess");
                    }
                    Err(e) => {
                        error!("{e:?}");
                    }
                }
            }
        }
    });

    while let Ok((crc, from_id, to, data)) = rx.recv() {
        let msg: Message = serde_cbor::from_slice(&data).unwrap();

        match &msg {
            Message::WebRtc { stream_id, action } => match action {
                RtcAction::Status { timestamp, code } => {
                    if let Ok(c) = client_t.parse_contacts_code(code.clone()) {
                        tracing::warn!("scan code: {c}");
                        client_t
                            .answer_contacts_code(code.clone(), Some("World".to_owned()))
                            .unwrap();
                    }
                }
                _ => {}
            },
            Message::ContactsExchange { exchange } => match exchange {
                ContactsEvent::Offer { token } => {
                    let code = serde_cbor::to_vec(&msg).unwrap();
                    let code = multibase::encode(multibase::Base::Base64, code);
                    client_t
                        .answer_contacts_code(code, Some("World".to_owned()))
                        .unwrap();
                }
                ContactsEvent::Answer { token } => {
                    let msg = Message::Chat {
                        content: luffa_rpc_types::ChatContent::Send {
                            data: luffa_rpc_types::ContentData::Text {
                                source: luffa_rpc_types::DataSource::Text {
                                    content: "Ok".to_owned(),
                                },
                                reference: None,
                            },
                        },
                    };
                    let msg = message_to(msg).unwrap();
                    tracing::warn!("Answer from:{}", from_id);
                    client_t.send_msg(from_id, msg).unwrap();
                }
            },
            _ => {
                let list = client_t.contacts_list(0);
                tracing::warn!("contacts>> {:?}", list);
                let list = client_t.session_list(10);
                tracing::warn!(" session>> {:?}", list);
            }
        }
    }

    Ok(())
}
