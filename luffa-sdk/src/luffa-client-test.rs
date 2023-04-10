use clap::Parser;
use futures::select;
use luffa_sdk::{Callback, Client, ClientResult};
use once_cell::sync::Lazy;
use serde_json;
use std::io;
use std::io::BufRead;
use std::sync::mpsc::sync_channel;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use luffa_rpc_types::{ContactsEvent, ContactsToken, Message, message_to, RtcAction};

// 12D3KooWA4xFkMebyXj4TEwgFxwZBn1baWByzWekafdJXEDqcnAP
// 12D3KooWBG8PcmWFas9vi1McucfPE6N6Qj1mMB86GwvrtnJx9tPw
pub static KEYS: Lazy<Vec<User>> = Lazy::new(|| {
    vec![
        User {
            mnemonic: "quick visa mad coyote amateur dirt idea wheel dune wash crew error"
                .to_string(),
            to: 10871006697545602478,
        },
        User {
            mnemonic: "hour upper shock ranch effort interest avocado carry travel soda rival that"
                .to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic: "hawk nasty wreck brisk target immune height december vault cliff tower merry".to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic: "duck culture horror sausage jungle wait dirt elegant hold van learn match".to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic: "suggest option city crucial maid catch win prevent bind thing disagree boil".to_string(),
            to: 13473655988076347637,
        }
    ]
});

#[derive(Debug)]
pub struct User {
    pub mnemonic: String,
    pub to: u64,
}

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct TestArgs {
    #[clap(short, long)]
    idx: usize,
}

// 13473655988076347637
// 10871006697545602478
// 11837182690600253035
fn main() -> ClientResult<()> {
    let args = TestArgs::parse();
    let idx = args.idx;
    let user = KEYS.get(idx).clone().unwrap();
    println!("{:?}", user);
    let client = Client::new();
    let (tx, rx) = sync_channel(1024);
    client.init(None)?;
    let name = client.import_key(&user.mnemonic, "")?.unwrap();
    client.save_key(&name)?;
    let process = Process::new(tx.clone());
    let msg = Box::new(process);
    let my_id = client.start(Some(name), None, msg)?;
    // Read full lines from stdin
    println!("my_id: {my_id}");
    let peer_id = client.get_peer_id()?.unwrap();
    println!("peer_id: {peer_id}");
    let code = client.gen_offer_code(my_id)?;
    println!("{code}");
    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();
    //
    let client1 = client.clone();
    let client2 = client.clone();
    std::thread::spawn(move || {
        loop {
            let line = stdin.next().unwrap().unwrap();
            let line: Vec<&str> = line.split(" ").collect();
            if line.len() < 1 {
                tracing::error!("command: {:?} error", line);
                continue;
            }
            let command = *line.get(0).unwrap();
            let mut args = "";
            if line.len() > 1 {
                args = *line.get(1).unwrap();
            }
            println!("{:?}", line);
            match command {
                // send_msg
                "send_msg" => {
                    let msg = Message::Chat {
                        content: luffa_rpc_types::ChatContent::Send {
                            data: luffa_rpc_types::ContentData::Text {
                                source: luffa_rpc_types::DataSource::Text {
                                    content: args.to_string(),
                                },
                                reference: None,
                            },
                        },
                    };
                    let msg = message_to(msg).unwrap();
                    let msg_id = client1.send_msg(user.to, msg).unwrap();
                    tracing::error!("msg_id: {msg_id}");
                }
                // recent
                "recent" => {
                    let data = client1.recent_messages(user.to, 0, 10).unwrap();
                    tracing::error!("recent_messages: {data:?}");
                }
                // last_chat
                "last_chat" => {
                    let data = client1.last_chat_msg_with_meta(user.to).unwrap();
                    tracing::error!("last_chat_msg_with_meta: {data:?}");
                }
                // relay_list
                "relay_list" => {
                    let relays = client1.relay_list().unwrap();
                    tracing::error!("relay_list: {relays:?}");
                }
                // show_code p/g
                "show_code" => {
                    let show_code = client1.show_code("https://luffa.putdev.com", args).unwrap().unwrap();
                    tracing::error!("show_code: {show_code:?}");
                }
                // contacts_offer https://luffa.putdev.com/p/YGz62Wdxqx8/9farAph9G5KLaEfLYRkoj8Roo8EVfpfHNa5hSZaZeLNB/Uncharted Banana pepper
                "contacts_offer" => {
                    let crc = client1.contacts_offer(&args.to_string()).unwrap();
                    tracing::error!("contacts_offer: {crc}");
                }
                // contacts_anwser 588347625583463033
                "contacts_anwser" => {
                    let id = client1.contacts_anwser(user.to, args.parse().unwrap()).unwrap();
                    tracing::error!("contacts_anwser: {id}");
                }
                // read_msg 14044266394953996910
                "read_msg" => {
                   let data = client1.read_msg_with_meta(user.to, args.parse().unwrap()).unwrap().unwrap();
                    tracing::error!("read_msg_with_meta: {data:?}");
                }
                // contacts_search1 Empathetic
                "contacts_search1" => {
                    let contacts = client1.contacts_search(0, args).unwrap();
                    tracing::error!("contacts_search: {contacts:?}");
                }
                // contacts_search2 Empathetic
                "contacts_search2" => {
                    let contacts = client1.contacts_search(2, args).unwrap();
                    tracing::error!("contacts_search: {contacts:?}");
                }
                // group_create my_group
                "group_create" => {
                    let group_id = client1.contacts_group_create(vec![13473655988076347637, user.to], Some(args.to_string())).unwrap();
                    tracing::error!("group_id: {group_id:?}");
                }
                // nickname 11837182690600253035
                "nickname" => {
                    let nickname = client1.find_contacts_tag(args.parse().unwrap()).unwrap();
                    tracing::error!("find_contacts_tag: {nickname:?}");
                }
                // group_invite 11837182690600253035
                "group_invite" => {
                    let tag = client1.contacts_group_invite_member(4832256763054520653, vec![args.parse().unwrap()]).unwrap();
                    tracing::error!("contacts_group_invite_member: {tag:?}");
                }
                // group_members 4832256763054520653
                "group_members" => {
                    let members = client1.contacts_group_members(args.parse().unwrap()).unwrap();
                    tracing::error!("contacts_group_members: {members:?}");
                }
                _ => {}
            }
        }
    });

    while let Ok((crc, from_id, to, data)) = rx.recv() {
        let msg: Message = serde_cbor::from_slice(&data).unwrap();

        match &msg {
            Message::Ping { relay_id, ttl_ms } => {
                tracing::info!("-----relay------{} ---ttl:{} ms", relay_id, ttl_ms);
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
                    // let crc = client2.contacts_anwser(to, from_id, crc,secret_key.clone(),secret_key);
                }
                ContactsEvent::Answer { offer_crc, token } => {
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
                    let crc = client2.send_msg(to, msg).unwrap();
                    tracing::info!("Answer from:offer_id {} ,did {}  ==> {}", from_id, to, crc);
                }
                _ => {}
            },
            Message::Chat { .. } => match client2.read_msg_with_meta(from_id, crc)? {
                Some(meta) => {
                    tracing::error!(
                        "on message meta>>crc: {crc}  from: {} to: {}",
                        meta.from_id,
                        meta.to_id
                    );
                }
                None => {
                    tracing::error!("msg not found {}->{}", from_id, crc);
                }
            },
            _ => {
                let list = client2.contacts_list(0)?;
                tracing::debug!("contacts>> {:?}", list);
                let list = client2.session_list(10)?;

                tracing::debug!(" session>> {:?}", list);

                for s in list {
                    let did = s.did;
                    for crc in s.reach_crc {
                        if let Some(meta) = client2.read_msg_with_meta(did, crc)? {
                            tracing::debug!("{:?}", meta);
                        }
                    }
                }
            }
        }
    }

    Ok(())
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