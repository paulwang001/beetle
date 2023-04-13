use clap::Parser;
use futures::select;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use luffa_rpc_types::{message_to, ContactsEvent, ContactsToken, Message, RtcAction};
use luffa_sdk::{Callback, Client, ClientResult};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json;
use std::io;
use std::io::BufRead;
use std::sync::mpsc::sync_channel;

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
            mnemonic:
                "hawk nasty wreck brisk target immune height december vault cliff tower merry"
                    .to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic: "duck culture horror sausage jungle wait dirt elegant hold van learn match"
                .to_string(),
            to: 13473655988076347637,
        },
        User {
            mnemonic: "suggest option city crucial maid catch win prevent bind thing disagree boil"
                .to_string(),
            to: 13473655988076347637,
        },
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

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub command: String,
    pub to: Option<u64>,
    pub msg: Option<String>,
    pub param: Option<String>,
    pub group_id: Option<u64>,
    pub groups: Option<Vec<u64>>,
}

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
            if line.is_empty() {
                continue
            }
            println!("{:?}", line);
            let params: Params = serde_json::from_str(&line).unwrap_or_default();
            match params.command.as_str() {
                // { "command": "send_msg", "to": 242692364427292578, "msg": "test" }
                "send_msg" => {
                    let msg = Message::Chat {
                        content: luffa_rpc_types::ChatContent::Send {
                            data: luffa_rpc_types::ContentData::Text {
                                source: luffa_rpc_types::DataSource::Text {
                                    content: params.msg.unwrap().to_string(),
                                },
                                reference: None,
                            },
                        },
                    };
                    let msg = message_to(msg).unwrap();
                    let msg_id = client1.send_msg(params.to.unwrap(), msg).unwrap();
                    tracing::error!("msg_id: {msg_id}");
                }
                // recent
                "recent" => {
                    let data = client1.recent_messages(params.to.unwrap(), 0, 10).unwrap();
                    tracing::error!("recent_messages: {data:?}");
                }
                // last_chat
                "last_chat" => {
                    let data = client1.last_chat_msg_with_meta(params.to.unwrap()).unwrap();
                    tracing::error!("last_chat_msg_with_meta: {data:?}");
                }
                // relay_list
                "relay_list" => {
                    let relays = client1.relay_list().unwrap();
                    tracing::error!("relay_list: {relays:?}");
                }
                // { "command": "gen_offer_code", "to": 12243737405236716139 }
                "gen_offer_code" => {
                    let gen_offer_code = client1
                        .gen_offer_code(params.to.unwrap())
                        .unwrap();
                    tracing::error!("gen_offer_code: {gen_offer_code:?}");
                }
                // { "command": "contacts_offer", "param": "https://luffa.putdev.com/p/YGz62Wdxqx8/H1NojzUMJ3LqhcjXCT11aCpVWjkb1JdfBpZRkAyPpNML/Uncharted Banana pepper"}
                "contacts_offer" => {
                    let crc = client1.contacts_offer(&params.param.unwrap()).unwrap();
                    tracing::error!("contacts_offer: {crc}");
                }
                // { "command": "contacts_anwser",  "to": 13803873857834870216, "param": "10166184139820202798" }
                // { "command": "contacts_anwser", "to": 10871006697545602478, "param": "5047934144353555626" }
                // { "command": "contacts_anwser",  "to": 11837182690600253035, "param": "10531898908420903627" }
                "contacts_anwser" => {
                    let id = client1
                        .contacts_anwser(
                            params.to.unwrap(),
                            params.param.unwrap().parse().unwrap(),
                        )
                        .unwrap();
                    tracing::error!("contacts_anwser: {id}");
                }
                // read_msg 14044266394953996910
                "read_msg" => {
                    let data = client1
                        .read_msg_with_meta(
                            params.to.unwrap(),
                            params.param.unwrap().parse().unwrap(),
                        )
                        .unwrap()
                        .unwrap();
                    tracing::error!("read_msg_with_meta: {data:?}");
                }
                // contacts_search1 Empathetic
                "contacts_search1" => {
                    let contacts = client1
                        .contacts_search(0, &params.param.unwrap())
                        .unwrap();
                    tracing::error!("contacts_search: {contacts:?}");
                }
                // contacts_search2${ "command": "group_test" }
                "contacts_search2" => {
                    let contacts = client1
                        .contacts_search(2, &params.param.unwrap())
                        .unwrap();
                    tracing::error!("contacts_search: {contacts:?}");
                }
                // 13685501506277185778 8191288328679216604
                // { "command": "group_create" ,"groups": [10871006697545602478, 11837182690600253035], "param": "group_test31" }
                "group_create" => {
                    let group_id = client1
                        .contacts_group_create(params.groups.unwrap(), params.param)
                        .unwrap();
                    tracing::error!("group_id: {group_id:?}");
                }
                // { "command": "nickname" , "param": "11837182690600253035" }
                // { "command": "nickname" , "param": "10871006697545602478" }
                // { "command": "nickname" , "param": "13473655988076347637" }
                "nickname" => {
                    let nickname = client1
                        .find_contacts_tag(params.param.unwrap().parse().unwrap())
                        .unwrap();
                    tracing::error!("find_contacts_tag: {nickname:?}");
                }
                // group_invite${"group_id": 12243737405236716139, "groups": [11837182690600253035]}
                "group_invite" => {
                    let tag = client1
                        .contacts_group_invite_member(
                            params.group_id.unwrap(),
                            params.groups.unwrap(),
                        )
                        .unwrap();
                    tracing::error!("contacts_group_invite_member: {tag:?}");
                }
                // group_members${ "command": "12243737405236716139" }
                "group_members" => {
                    let members = client1
                        .contacts_group_members(params.param.unwrap().parse().unwrap(), 1, 10)
                        .unwrap();
                    tracing::error!("contacts_group_members: {members:?}");
                }
                // { "command": "groups" }
                "groups" => {
                    let members = client1.groups().unwrap();
                    tracing::error!("groups: {members:?}");
                }
                // { "command": "group_info", "group_id": 242692364427292578 }
                "group_info" => {
                    let group = client1.contacts_group_members(params.group_id.unwrap() , 1, 10).unwrap();
                    tracing::error!("group_info: {group:?}");
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
            Message::WebRtc {
                stream_id,
                action_type,
                action,
            } => match action {
                RtcAction::Status {
                    timestamp,
                    code,
                    info,
                } => {}
                RtcAction::Push { audio_id,action_type, video_id } => {
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
                ContactsEvent::Answer {
                    offer_crc,
                    token,
                    members,
                } => {
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
