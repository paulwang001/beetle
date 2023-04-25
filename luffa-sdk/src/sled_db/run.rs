use std::{sync::Arc, time::{Duration, Instant}, collections::{VecDeque, HashSet}};

use chrono::Utc;
use libp2p::PeerId;
use luffa_node::{NetworkEvent, GossipsubEvent};
use luffa_rpc_types::{
    ChatContent, Contacts,ContactsTypes, Event,
    FeedbackStatus, Message, AppStatus,
};
use parking_lot::RwLock;
use sled::Db;
use tantivy::{doc, schema::Schema, IndexWriter, Term};

use crate::{api::P2pClient, Callback, Client, bs58_encode};
use tokio::{sync::oneshot::Sender as ShotSender, task::JoinHandle};
use chrono::offset::TimeZone;
use tracing::error;


use crate::sled_db::contacts::{ContactsDb, KVDB_CONTACTS_TREE};
use crate::sled_db::session::SessionDb;
impl Client {
    pub async fn run(
        db: Arc<Db>,
        cb: Box<dyn Callback>,
        mut receiver: tokio::sync::mpsc::Receiver<(
            u64,
            Vec<u8>,
            u64,
            ShotSender<anyhow::Result<u64>>,
            Option<Vec<u8>>,
        )>,
        sender: tokio::sync::mpsc::Sender<(
            u64,
            Vec<u8>,
            u64,
            ShotSender<anyhow::Result<u64>>,
            Option<Vec<u8>>,
        )>,
        idx_writer: Arc<RwLock<IndexWriter>>,
        schema: Schema,
        peer: &PeerId,
        client: P2pClient,
        mut events: tokio::sync::mpsc::Receiver<NetworkEvent>,
        p2p_rpc: JoinHandle<()>,
    ) {
        // let (tx, rx) = tokio::sync::mpsc::channel::<NetworkEvent>(4096);
        let cb = Arc::new(cb);

        let mut digest = crc64fast::Digest::new();
        digest.write(&peer.to_bytes());
        let my_id = digest.sum64();
        let db_t = db.clone();
        let client_t = client.clone();
        let sync_task = tokio::spawn(async move {
            let mut count = 0_u64;
            loop {
                match client_t.get_peers().await {
                    Ok(peers) => {
                        if peers.is_empty() {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
                if count % 6 == 0 {
                    tracing::info!("subscribed all as client,status sync");
                    let msg = luffa_rpc_types::Message::StatusSync {
                        to: 0,
                        status: AppStatus::Active,
                        from_id: my_id,
                    };
                    let key: Option<Vec<u8>> = None;
                    let event = Event::new(0, &msg, key, my_id, None);
                    let data = event.encode().unwrap();
                    let client_t = client_t.clone();
                    tokio::spawn(async move {
                        if let Err(e) = client_t.chat_request(bytes::Bytes::from(data)).await {
                            tracing::error!("status sync pub>> {e:?}");
                        }
                    });
                }

                let page = count % 8;
                let mut contacts = vec![];
                for lvl in 0..8 {
                    if page >= lvl {
                        if let Some(lvl_0) =
                            Self::db_session_list(db_t.clone(), lvl as u32, 4, my_id)
                        {
                            let lvl_contacts = lvl_0
                                .into_iter()
                                .map(|cs| {
                                    let to = cs.did;
                                    let c_type = Self::get_contacts_type(db_t.clone(), to)
                                        .unwrap_or(ContactsTypes::Private);
                                    let c_type: u8 = c_type as u8;
                                    let c_type = if c_type == 0 {
                                        ContactsTypes::Private
                                    } else {
                                        ContactsTypes::Group
                                    };
                                    // let have_time = Self::get_contacts_have_time(db_t.clone(), to);
                                    Contacts {
                                        did: to,
                                        r#type: c_type,
                                    }
                                })
                                .collect::<Vec<_>>();

                            contacts.extend_from_slice(&lvl_contacts[..]);
                        }
                    }
                }
                // tracing::error!("recent seesion sync:{}",contacts.len());
                if !contacts.is_empty() {
                    let sync = Message::ContactsSync {
                        did: my_id,
                        contacts,
                    };
                    let event = Event::new(0, &sync, None, my_id, None);
                    let data = event.encode().unwrap();
                    let client_t = client_t.clone();
                    tokio::spawn(async move {
                        let d_size = data.len();
                        if let Err(e) = client_t.chat_request(bytes::Bytes::from(data)).await {
                            tracing::error!("pub contacts sync status >>> {e:?}");
                        } else {
                            tracing::warn!("pub contacts sync status >>> {sync:?}",);
                        }
                    });
                }
                if count % 60 == 0 {
                    let tree = db_t.open_tree(KVDB_CONTACTS_TREE).unwrap();

                    let tag_prefix = format!("TAG-");
                    let itr = tree.scan_prefix(tag_prefix);
                    let mut contacts = itr
                        .map(|item| {
                            let (k, _v) = item.unwrap();
                            // let tag = String::from_utf8(v.to_vec()).unwrap();
                            let key = String::from_utf8(k.to_vec()).unwrap();
                            let parts = key.split('-');

                            let to = parts.last().unwrap();
                            let to: u64 = to.parse().unwrap();
                            let c_type = Self::get_contacts_type(db_t.clone(), to)
                                .unwrap_or(ContactsTypes::Private);
                            let c_type: u8 = c_type as u8;
                            let c_type = if c_type == 0 {
                                ContactsTypes::Private
                            } else {
                                ContactsTypes::Group
                            };
                            Contacts {
                                did: to,
                                r#type: c_type,
                            }
                        })
                        .collect::<Vec<_>>();
                    contacts.retain(|c| c.did != my_id);
                    let client_t = client_t.clone();
                    if !contacts.is_empty() {
                        let sync = Message::ContactsSync {
                            did: my_id,
                            contacts,
                        };
                        let event = Event::new(0, &sync, None, my_id, None);
                        let data = event.encode().unwrap();

                        tokio::spawn(async move {
                            let d_size = data.len();
                            if let Err(e) = client_t
                                .chat_request(bytes::Bytes::from(data.clone()))
                                .await
                            {
                                tracing::error!("pub contacts sync status >>> {e:?}");
                            } else {
                                tracing::warn!("pub contacts sync status >>> {d_size}");
                                if count == 0 {
                                    tracing::warn!("pub contacts sync status >>> {sync:?}");
                                }
                            }
                        });
                    }
                }

                count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });

        let client_t = client.clone();
        let db_t = db.clone();
        let idx = idx_writer.clone();
        let idx_t = idx.clone();
        let session_last_crc = Arc::new(RwLock::new(db.open_tree("session_last").unwrap()));
        let session_last_crc_t = session_last_crc.clone();
        let schema_t = schema.clone();
        let schema_tt = schema.clone();
        let cb_local = cb.clone();
        let process = tokio::spawn(async move {
            let fetching_crc = Arc::new(RwLock::new(HashSet::<u64>::new()));
            while let Some(evt) = events.recv().await {
                match evt {
                    NetworkEvent::RequestResponse(rsp) => {
                        match rsp {
                            luffa_node::ChatEvent::Response { data, .. } => {
                                if let Ok(im) = Event::decode_uncheck(&data) {
                                    let Event {
                                        msg,
                                        nonce,
                                        to,
                                        from_id,
                                        crc,
                                        event_time,
                                        ..
                                    } = im;

                                    if nonce.is_none() {
                                        if let Ok(msg_d) = luffa_rpc_types::Message::decrypt(
                                            bytes::Bytes::from(msg.clone()),
                                            None,
                                            nonce,
                                        ) {
                                            tracing::info!("clinet>>>>>nonce {msg_d:?}");
                                            let msg_t = msg_d.clone();
                                            let mut will_to_ui = true;
                                            let relay_id = from_id;
                                            match msg_d {
                                                Message::Feedback {
                                                    crc, to_id, status, ..
                                                } => {
                                                    match status {
                                                        FeedbackStatus::Send
                                                        | FeedbackStatus::Routing => {
                                                            let to = to_id.unwrap_or_default();
                                                            if to > 0 {
                                                                tracing::warn!("response>>>>>on_message send {crc:?} from {from_id} to {to} msg:{msg_t:?}");
                                                                let table = format!("message_{to}");
                                                                for c in crc {
                                                                    Self::save_to_tree_status(
                                                                        db_t.clone(),
                                                                        c,
                                                                        &table,
                                                                        2,
                                                                    );
                                                                }
                                                            } else {
                                                                tracing::warn!("ccc clinet>>>>>on_message send {crc:?} from {from_id} to {to} msg:{msg_t:?}");
                                                                will_to_ui = false;
                                                            }
                                                        }
                                                        FeedbackStatus::Fetch
                                                        | FeedbackStatus::Notice => {
                                                            let mut ls_crc = crc;
                                                            will_to_ui = false;
                                                            let client_t = client_t.clone();
                                                            let db_tt = db_t.clone();
                                                            let cb_t = cb.clone();
                                                            let idx_t = idx.clone();
                                                            let schema_t = schema.clone();
                                                            {
                                                                let mut f_crc =
                                                                    fetching_crc.write();
                                                                let mut fetchs = vec![];
                                                                for c in ls_crc.iter() {
                                                                    if !f_crc.insert(*c) {
                                                                        fetchs.push(*c);
                                                                        tracing::warn!("r response fetching: {c} ,r> {relay_id} f> {from_id:?}, t> {to_id:?}");
                                                                    } else {
                                                                        tracing::warn!("n response fetching: {c} ,r> {relay_id} f> {from_id:?}, t> {to_id:?}");
                                                                    }
                                                                }
                                                                ls_crc.retain(|c| {
                                                                    !fetchs.contains(c)
                                                                });
                                                            }
                                                            let fetching_crc_t =
                                                                fetching_crc.clone();

                                                            let session_last_crc_tt = session_last_crc_t.clone();    
                                                            let msg_tx = sender.clone();

                                                            tokio::spawn(async move {
                                                                let client_t = client_t.clone();
                                                                let db_tt = db_tt.clone();
                                                                let cb_t = cb_t.clone();
                                                                let idx_t = idx_t.clone();
                                                                let schema_t = schema_t.clone();
                                                                let session_last_crc_tt =
                                                                    session_last_crc_tt.clone();
                                                                for crc in ls_crc {
                                                                    match client_t
                                                                        .get_crc_record(crc)
                                                                        .await
                                                                    {
                                                                        Ok(res) => {
                                                                            let data = res.data;
                                                                            tracing::warn!("response get record: {crc} , f> {from_id:?}, t> {to_id:?}");
                                                                            let data =
                                                                                data.to_vec();
                                                                            if let Ok(im) = Event::decode_uncheck(&data) {
                                                                                let Event {
                                                                                    to,
                                                                                    from_id,
                                                                                    crc,
                                                                                    ..
                                                                                } = im;

                                                                                let did = if to == my_id { from_id } else { to };
                                                                                let table = format!("message_{}", did);

                                                                                if !Self::have_in_tree(db_tt.clone(), crc, &table) {
                                                                                    Self::process_event(
                                                                                        db_tt.clone(), cb_t.clone(), client_t.clone(), 
                                                                                        idx_t.clone(), schema_t.clone(), &data, my_id,session_last_crc_tt.clone(),
                                                                                        msg_tx.clone(),
                                                                                    )
                                                                                        .await;
                                                                                } else {
                                                                                    tracing::error!("have in tree {crc}");
                                                                                    // client_t.chat_request(bytes::Bytes::from());
                                                                                    let feed = luffa_rpc_types::Message::Feedback { crc: vec![crc], from_id: Some(my_id), to_id: Some(to), status: luffa_rpc_types::FeedbackStatus::Reach };
                                                                                    let event = luffa_rpc_types::Event::new(
                                                                                        0,
                                                                                        &feed,
                                                                                        None,
                                                                                        my_id,
                                                                                        None,
                                                                                    );
                                                                                    tracing::warn!("having>>> send feedback reach to relay");
                                                                                    let event = event.encode().unwrap();
                                                                                    if let Err(e) =
                                                                                        client_t.chat_request(bytes::Bytes::from(event)).await
                                                                                    {
                                                                                        error!("{e:?}");
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                        Err(e) => {
                                                                            error!("record not found {crc} error: {e:?}");
                                                                            // if Self::have_message_in_tree(db_tt.clone(), crc) {
                                                                                tracing::warn!("message {crc} is reached");

                                                                                let feed = luffa_rpc_types::Message::Feedback { crc: vec![crc], from_id: Some(my_id), to_id: Some(0), status: luffa_rpc_types::FeedbackStatus::Reach };
                                                                                let event = luffa_rpc_types::Event::new(
                                                                                    0,
                                                                                    &feed,
                                                                                    None,
                                                                                    my_id,
                                                                                    None,
                                                                                );
                                                                                let event = event
                                                                                    .encode()
                                                                                    .unwrap();
                                                                                if let Err(e) =
                                                                                    client_t.chat_request(bytes::Bytes::from(event)).await
                                                                                {
                                                                                    error!("send feedback failed in run {e:?}");
                                                                                }
                                                                            // }
                                                                        }
                                                                    }
                                                                    {
                                                                        let mut f_crc =
                                                                            fetching_crc_t.write();
                                                                        f_crc.remove(&crc);
                                                                    }
                                                                }
                                                            });
                                                        }
                                                        _ => {
                                                            // tracing::warn!("clinet>>>>>on_message send {crc:?} from {from_id} to {to_id:?}");
                                                        }
                                                    }
                                                }
                                                _ => {}
                                            }
                                            if will_to_ui {
                                                cb.on_message(crc, from_id, to, event_time, msg);
                                            }
                                        }
                                    } else {
                                        let did = if to == my_id { from_id } else { to };
                                        let table = format!("message_{}", did);
                                        let idx = idx.clone();
                                        let db_t = db_t.clone();
                                        let schema_tt = schema_tt.clone();
                                        let client_t = client_t.clone();
                                        let cb = cb.clone();
                                        let session_last_crc_tt = session_last_crc_t.clone();
                                        let msg_tx = sender.clone();
                                        tokio::spawn(async move {
                                            if !Self::have_in_tree(db_t.clone(), crc, &table) {
                                                Self::process_event(
                                                    db_t,
                                                    cb,
                                                    client_t,
                                                    idx,
                                                    schema_tt,
                                                    &data,
                                                    my_id,
                                                    session_last_crc_tt.clone(),
                                                    msg_tx,
                                                )
                                                .await;
                                            }
                                        });
                                    }
                                }
                            }
                            luffa_node::ChatEvent::Request(data) => {
                                //TODO message which coming from other
                                if let Ok(im) = Event::decode_uncheck(&data) {
                                    let Event {
                                        from_id,
                                        to,
                                        crc,
                                        nonce,
                                        msg,
                                        ..
                                    } = im;

                                    let session_last_crc_t = session_last_crc_t.clone();
                                    if to == my_id && nonce.is_none() {
                                        if let Ok(m) = Message::decrypt(
                                            bytes::Bytes::from(msg.clone()),
                                            None,
                                            nonce,
                                        ) {
                                            // let m_t = m.clone();
                                            let relay_id = from_id;
                                            match m {
                                                Message::Feedback {
                                                    crc,
                                                    from_id,
                                                    to_id,
                                                    status,
                                                } => {
                                                    match status {
                                                        FeedbackStatus::Fetch
                                                        | FeedbackStatus::Notice => {
                                                            let mut ls_crc = crc;
                                                            let client_t = client_t.clone();
                                                            let db_tt = db_t.clone();
                                                            let cb_t = cb.clone();
                                                            let idx_t = idx.clone();
                                                            let schema_t = schema.clone();
                                                            {
                                                                let mut f_crc =
                                                                    fetching_crc.write();
                                                                let mut fetchs = vec![];
                                                                for c in ls_crc.iter() {
                                                                    if !f_crc.insert(*c) {
                                                                        fetchs.push(*c);
                                                                        tracing::warn!("r request fetching: {c} ,r> {relay_id} f> {from_id:?}, t> {to_id:?}");
                                                                    } else {
                                                                        tracing::warn!("n request fetching: {c} ,r> {relay_id} f> {from_id:?}, t> {to_id:?}");
                                                                    }
                                                                }
                                                                ls_crc.retain(|c| {
                                                                    !fetchs.contains(c)
                                                                });
                                                            }
                                                            let fetching_crc_t =
                                                                fetching_crc.clone();
                                                            let session_last_crc_t = session_last_crc_t.clone();
                                                            let msg_tx = sender.clone();

                                                            tokio::spawn(async move {
                                                                let client_t = client_t.clone();
                                                                let db_tt = db_tt.clone();
                                                                let cb_t = cb_t.clone();
                                                                let idx_t = idx_t.clone();
                                                                let schema_t = schema_t.clone();
                                                                for crc in ls_crc {
                                                                    match client_t
                                                                        .get_crc_record(crc)
                                                                        .await
                                                                    {
                                                                        Ok(res) => {
                                                                            let data = res.data;
                                                                            tracing::warn!("request get record: {crc} , f> {from_id:?}, t> {to_id:?}");
                                                                            let data =
                                                                                data.to_vec();
                                                                            if let Ok(im) = Event::decode_uncheck(&data) {
                                                                                let Event {
                                                                                    to,
                                                                                    from_id,
                                                                                    crc,
                                                                                    ..
                                                                                } = im;

                                                                                let did = if to == my_id { from_id } else { to };
                                                                                let table = format!("message_{}", did);

                                                                                if !Self::have_in_tree(db_tt.clone(), crc, &table) {
                                                                                    Self::process_event(
                                                                                        db_tt.clone(), cb_t.clone(), client_t.clone(), idx_t.clone(), schema_t.clone(), &data, my_id,
                                                                                        session_last_crc_t.clone(),
                                                                                        msg_tx.clone(),
                                                                                    )
                                                                                        .await;
                                                                                } else {
                                                                                    tracing::error!("have in tree {crc}");
                                                                                    // client_t.chat_request(bytes::Bytes::from());
                                                                                    let feed = luffa_rpc_types::Message::Feedback { crc: vec![crc], from_id: Some(my_id), to_id: Some(0), status: luffa_rpc_types::FeedbackStatus::Reach };
                                                                                    let event = luffa_rpc_types::Event::new(
                                                                                        0,
                                                                                        &feed,
                                                                                        None,
                                                                                        my_id,
                                                                                        None,
                                                                                    );
                                                                                    tracing::warn!("having>>> send feedback reach to relay");
                                                                                    let event = event.encode().unwrap();
                                                                                    if let Err(e) =
                                                                                        client_t.chat_request(bytes::Bytes::from(event)).await
                                                                                    {
                                                                                        error!("{e:?}");
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                        Err(e) => {
                                                                            error!("record not found {crc} error: {e:?}");
                                                                        }
                                                                    }
                                                                    {
                                                                        let mut f_crc =
                                                                            fetching_crc_t.write();
                                                                        f_crc.remove(&crc);
                                                                    }
                                                                }
                                                            });
                                                        }
                                                        _ => {
                                                            tracing::warn!("from relay request no nonce msg Feedback>>>>{status:?}");
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    tracing::warn!(
                                                        "from relay request no nonce msg>>>>{m:?}"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    let did = if to == my_id { from_id } else { to };
                                    let table = format!("message_{}", did);
                                    let db_t = db_t.clone();
                                    let cb = cb.clone();
                                    let client_t = client_t.clone();
                                    let idx = idx.clone();
                                    let schema_tt = schema_tt.clone();
                                    let fetching_crc_t = fetching_crc.clone();
                                    let session_last_crc_t = session_last_crc_t.clone();
                                    let msg_tx = sender.clone();
                                    tokio::spawn(async move {
                                        {
                                            let mut f_crc = fetching_crc_t.write();
                                            if !f_crc.insert(crc) {
                                                tracing::warn!("request has pushing record: {crc}, f> {from_id}, t> {to}");
                                                return;
                                            }
                                        }
                                        if !Self::have_in_tree(db_t.clone(), crc, &table) {
                                            Self::process_event(
                                                db_t,
                                                cb,
                                                client_t,
                                                idx,
                                                schema_tt,
                                                &data,
                                                my_id,
                                                session_last_crc_t.clone(),
                                                msg_tx.clone(),
                                            )
                                            .await;
                                        }
                                        {
                                            let mut f_crc = fetching_crc_t.write();
                                            f_crc.remove(&crc);
                                        }
                                    });
                                } else {
                                    panic!("decode failed");
                                }
                            }
                            luffa_node::ChatEvent::OutboundFailure {
                                peer,
                                request_id,
                                error,
                            } => {}
                            luffa_node::ChatEvent::InboundFailure {
                                peer,
                                request_id,
                                error,
                            } => {}
                            luffa_node::ChatEvent::ResponseSent { peer, request_id } => {}
                        }
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Subscribed { peer_id, topic }) => {
                        // TODO: a group member or my friend online?
                        tracing::debug!("Subscribed> peer_id: {peer_id:?} topic:{topic}");
                    }
                    NetworkEvent::Gossipsub(GossipsubEvent::Message { message, from, id }) => {}
                    NetworkEvent::Gossipsub(GossipsubEvent::Unsubscribed { peer_id, topic }) => {

                        // TODO: a group member or my friend offline?
                    }
                    NetworkEvent::PeerConnected(peer_id) => {
                        tracing::info!("---------PeerConnected-----------{:?}", peer_id);
                    }
                    NetworkEvent::PeerDisconnected(peer_id) => {
                        tracing::debug!("---------PeerDisconnected-----------{:?}", peer_id);
                    }
                    NetworkEvent::CancelLookupQuery(peer_id) => {
                        tracing::debug!("---------CancelLookupQuery-----------{:?}", peer_id);
                    }
                    NetworkEvent::Ping(info) => {
                        let mut digest = crc64fast::Digest::new();
                        digest.write(&info.peer.to_bytes());
                        let relay_id = digest.sum64();

                        cb.on_message(
                            0,
                            0,
                            0,
                            0,
                            serde_cbor::to_vec(&Message::Ping {
                                relay_id,
                                ttl_ms: info.ttl.as_millis() as u64,
                            })
                            .expect("deserialize message ping failed"),
                        );
                    }
                }
                // tracing::warn!("");
            }
        });

        let db_t = db.clone();
        let db_t2 = db.clone();
        let mut pendings = VecDeque::<(Event, u32, Instant)>::new();
        let some_pendding = db.open_tree("some_pendding").unwrap();
        let mut itr = some_pendding.iter();
        while let Some(item) = itr.next() {
            if let Ok((_k, v)) = item {
                let e = Event::decode_uncheck(&v.to_vec()).unwrap();
                pendings.push_back((e, 1, Instant::now()));
            }
        }
        let pendings = Arc::new(tokio::sync::RwLock::new(pendings));
        let pendings_t = pendings.clone();
        let cb_tt = cb_local.clone();
        let client_pending = client.clone();
        let pending_task = tokio::spawn(async move {
            // let c = client_pending.clone();
            let client_connected = || async {
                match client_pending.get_peers().await {
                    Ok(peers) => {
                        if peers.is_empty() {
                            false
                        } else {
                            true
                        }
                    }
                    _ => true,
                }
            };
            let mut first = true;
            loop {
                {
                    let p = pendings_t.read().await;
                    if p.is_empty() {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }

                if first {
                    let feed = Message::StatusSync {
                        from_id: my_id,
                        to: 0,
                        status: AppStatus::Connected,
                    };
                    let feed = serde_cbor::to_vec(&feed).unwrap();
                    let now = Utc::now().timestamp_millis() as u64;
                    cb_tt.on_message(0, my_id, 0, now, feed);
                    first = false;
                }
                if let Some((req, count, _time, all_size)) =
                    {
                    let mut p = pendings_t.write().await;
                    let all_size = p.len();
                    tracing::info!("pending size:{}", all_size);
                    let mut req = None;
                    while let Some((r, c, t)) = p.pop_front() {
                        if r.nonce.is_none() && c >= 20 {
                            continue;
                        }
                        if t.elapsed().as_millis() < c as u128 * 300 && r.nonce.is_none() {
                            p.push_back((r, c, t));
                            continue;
                        }
                        else if t.elapsed().as_millis() < 300 && r.nonce.is_some() {
                            if c > 32 {
                                let now = Utc::now().timestamp_millis() as u64;
                                let (tt,u)= {
                                    let mut time = now - r.event_time;
                                    let mut u = format!("ms");
                                    if time > 1000 {
                                        time = time / 1000;
                                        u = format!("sec");
                                    }
                                    if time > 60 {
                                        time = time / 60;
                                        u = format!("min");
                                    }
                                    (time,u)
                                };
                                let crc = r.crc;
                                let err = Message::InnerError { kind: 200, reason: format!("Warnning >>crc {crc} send count {c} in {tt} {u}") };
                                let err = serde_cbor::to_vec(&err).unwrap();
                                cb_tt.on_message(r.crc, 0, 0, now, err);
                            }
                            p.push_back((r, c, t));
                            continue;
                        }
                        req = Some((r, c, t, all_size));
                        break;
                    }
                    req
                }
                {
                    let to = req.to;
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let event_time = req.event_time;
                    // let nonce = &req.nonce;
                    let data = req.encode().unwrap();
                    some_pendding.insert(req.crc.to_be_bytes(), data.clone()).unwrap();
                    some_pendding.flush().unwrap();
                    Self::save_to_tree_status(
                        db_t2.clone(),
                        req.crc,
                        &format!("message_{to}"),
                        FeedbackStatus::Sending as u8,
                    );

                    let crc = req.crc;
                    let nonce_is_some = req.nonce.is_some();
                    let handle_send_failed = || async {
                        if all_size > 64 && req.nonce.is_none() {
                            return;
                        }

                        let is_failed =
                            Utc::now() - Utc.timestamp_millis(req.event_time as i64) > chrono::Duration::seconds(60 * 2);
                        // let status = if count >= 20 {FeedbackStatus::Failed} else {FeedbackStatus::Sending};
                        if req.nonce.is_some() {
                            let status = if is_failed {FeedbackStatus::Failed } else {FeedbackStatus::Sending};
                            let feed = Message::Feedback {
                                crc: vec![req.crc],
                                from_id: Some(my_id),
                                to_id: Some(to),
                                status,
                            };
                            let feed = serde_cbor::to_vec(&feed).unwrap();

                            cb_tt.on_message(req.crc, my_id, to, event_time, feed);

                            if is_failed  {
                                Self::save_to_tree_status(
                                    db_t2.clone(),
                                    req.crc,
                                    &format!("message_{to}"),
                                    FeedbackStatus::Failed as u8,
                                );

                                some_pendding.remove(&req.crc.to_be_bytes()).unwrap();
                                some_pendding.flush().unwrap();
                                return;
                            }
                        }

                        let mut push = pendings_t.write().await;
                        push.push_back((req, count + 1, Instant::now()));
                    };

                    if !client_connected().await {
                        handle_send_failed().await;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }

                    match client_pending
                        .chat_request(bytes::Bytes::from(data.clone()))
                        .await
                    {
                        Ok(res) => {
                            if nonce_is_some {
                                let feed = Message::Feedback {
                                    crc: vec![crc],
                                    from_id: Some(my_id),
                                    to_id: Some(to),
                                    status: FeedbackStatus::Send,
                                };
                                let feed = serde_cbor::to_vec(&feed).unwrap();
                                let table = format!("message_{to}");
                                Self::save_to_tree_status(db_t2.clone(), crc, &table, FeedbackStatus::Send as u8);
                                cb_tt.on_message(crc, my_id, to, event_time, feed);
                                some_pendding.remove(&crc.to_be_bytes()).unwrap();
                                some_pendding.flush().unwrap();
                            }
                            tracing::debug!("{res:?}");
                        }
                        Err(e) => {
                            tracing::error!("pending chat request failed [{}]: {e:?}",crc);
                            tokio::time::sleep(Duration::from_millis(300)).await;

                            handle_send_failed().await;
                        }
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        });

        while let Some((to, msg_data, from_id, channel, k)) = receiver.recv().await {
            if to == u64::MAX {
                channel.send(Ok(u64::MAX)).unwrap();
                break;
            }
            let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
            // let is_exchane = msg.is_contacts_exchange();
            let evt = if msg.need_encrypt() {
                let last_crc = {
                    let last_crc = session_last_crc.read();
                    Self::get_u64_from_tree(&last_crc, &to.to_be_bytes())
                };
                match k {
                    Some(key) => Some(Event::new(to, &msg, Some(key), from_id, last_crc)),
                    None => {
                        tracing::info!("----------encrypt------from [{}] to [{}]", from_id, to);
                        match Self::get_aes_key_from_contacts(db.clone(), to) {
                            Some(key) => Some(Event::new(to, &msg, Some(key), from_id, last_crc)),
                            None => {
                                tracing::error!("aes key not found did:{} msg:{:?}", to, msg);
                                None
                                //    Some(Event::new(to, &msg, None, from_id))
                            }
                        }
                    }
                }
            } else {
                Some(Event::new(to, &msg, None, from_id, None))
            };
            match evt {
                Some(e) => {
                    let req = e.clone();
                    let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
                    let tag_key = format!("TAG-{}", to);
                    let (tag, msg_type) = match tree.get(&tag_key.as_bytes()) {
                        Ok(Some(v)) => {
                            let tp = match Self::get_contacts_type(db.clone(), to) {
                                Some(tp) => match tp {
                                    ContactsTypes::Private => {
                                        format!("content_private")
                                    }
                                    _ => {
                                        format!("content_group")
                                    }
                                },
                                None => {
                                    format!("content_private")
                                }
                            };
                            (String::from_utf8(v.to_vec()).unwrap(), tp)
                        }
                        _ => (String::new(), format!("exchange")),
                    };
                    let save_job = if to > 0 {
                        let msg = serde_cbor::from_slice::<Message>(&msg_data).unwrap();
                        let data = e.encode().unwrap();
                        let Event {
                            to,
                            event_time,
                            crc,
                            from_id,
                            nonce,
                            ..
                        } = e;
                        // assert!(nonce.is_some(),"nonce is none!!");
                        if let Err(e) = channel.send(Ok(crc)) {
                            tracing::info!("channel send failed {e:?}");
                        }
                        let db_t = db_t.clone();
                        let schema_t = schema_t.clone();
                        let idx_t = idx_t.clone();
                        let job = tokio::spawn(async move {
                            let table = { format!("message_{to}") };
                            tracing::info!("send......");
                            let mut will_save = false;
                            match msg {
                                Message::Chat { content } => {
                                    // TODO index content search engine
                                    match content {
                                        ChatContent::Burn { crc, expires } => {
                                            let p_table = format!("message_{to}");
                                            Self::burn_from_tree(db_t.clone(), crc, p_table);

                                            let fld_crc = schema_t.get_field("crc").unwrap();
                                            let mut wr = idx_t.write();
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
                                                        let t = multibase::encode(
                                                            multibase::Base::Base58Btc,
                                                            cid,
                                                        );
                                                        let body = format!("{:?}", reference);
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
                                                    luffa_rpc_types::DataSource::Text {
                                                        content,
                                                    } => {
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
                                                } => (title, format!("{:?}", m_type)),
                                            };
                                            Self::update_session(
                                                db_t.clone(),
                                                to,
                                                None,
                                                Some(crc),
                                                Some(crc),
                                                Some(body.clone()),
                                                None,
                                                event_time,
                                            );

                                            let title = {
                                                // title 内容有 title 短id 用户nickname
                                                let bs_id = bs58_encode(to).ok();
                                                let nickname =
                                                    Self::get_contacts_tag(db_t.clone(), to)
                                                        .map(|(v, _)| v);
                                                let titles: Vec<_> =
                                                    vec![Some(title), bs_id, nickname]
                                                        .into_iter()
                                                        .flatten()
                                                        .collect();
                                                let title: String = titles.join(" ");
                                                title
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
                                            let mut wr = idx_t.write();
                                            wr.add_document(doc).unwrap();
                                            wr.commit().unwrap();
                                            will_save = true;
                                        }
                                        _ => {}
                                    }
                                }
                                Message::WebRtc { .. } => {
                                    will_save = true;
                                }
                                _ => {}
                            }
                            if will_save && nonce.is_some() {
                                Self::save_to_tree(
                                    db_t.clone(),
                                    e.crc,
                                    &table,
                                    data.clone(),
                                    event_time,
                                );
                            }
                        });
                        Some(job)
                    } else {
                        if let Err(_e) = channel.send(Ok(0)) {
                            tracing::error!("channel send failed");
                        }
                        None
                    };
                    if to != my_id {
                        let client = client.clone();
                        if msg.is_important() {
                            let last_crc = session_last_crc.write();
                            match last_crc.insert(to.to_be_bytes(), req.crc.to_be_bytes().to_vec())
                            {
                                Ok(_v) => {
                                    last_crc.flush().unwrap();
                                }
                                Err(e) => {
                                    tracing::error!("{e:?}");
                                }
                            }
                        }
                        let event_time = req.event_time;
                        let pendings_t = pendings.clone();
                        let cb_t = cb_local.clone();
                        // if req.nonce.is_some() {
                        //     let sending = Message::Feedback {
                        //         crc: vec![req.crc],
                        //         from_id: Some(my_id),
                        //         to_id: Some(to),
                        //         status: FeedbackStatus::Sending,
                        //     };
                        //     let sending = serde_cbor::to_vec(&sending).unwrap();
                        //     cb_t.on_message(req.crc, my_id, to, event_time, sending);
                        // }
                        let db_t2 = db_t.clone();
                        if let Some(job) = save_job {
                            if let Err(e) = job.await {
                                tracing::error!("job>> {e:?}");
                            }
                        }
                        tokio::spawn(async move {
                            let data = req.encode().unwrap();
                            tracing::info!("sending: [ {} ] size:{}", req.crc, data.len());

                            if req.nonce.is_some() {
                                let some_pendding = db_t2.open_tree("some_pendding").unwrap();
                                some_pendding.insert(req.crc.to_be_bytes(), data.clone()).unwrap();
                                some_pendding.flush().unwrap();
                            }

                            match client.chat_request(bytes::Bytes::from(data.clone())).await {
                                Ok(res) => {
                                    tracing::warn!("send: [ {} ] ", req.crc);

                                    if req.nonce.is_some() {
                                        let some_pendding = db_t2.open_tree("some_pendding").unwrap();
                                        some_pendding.remove(req.crc.to_be_bytes()).unwrap();
                                        some_pendding.flush().unwrap();

                                        let feed = Message::Feedback {
                                            crc: vec![req.crc],
                                            from_id: Some(my_id),
                                            to_id: Some(to),
                                            status: FeedbackStatus::Send,
                                        };
                                        let feed = serde_cbor::to_vec(&feed).unwrap();
                                        let table = format!("message_{to}");
                                        Self::save_to_tree_status(
                                            db_t2.clone(),
                                            req.crc,
                                            &table,
                                            1,
                                        );
                                        cb_t.on_message(req.crc, my_id, to, event_time, feed);
                                    }
                                    tracing::debug!("{res:?}");
                                }
                                Err(e) => {
                                    tracing::error!("chat request failed [{}]: {e:?}", req.crc);
                                    // if req.nonce.is_some() {
                                    // // tokio::time::sleep(Duration::from_millis(1000)).await;
                                    // if req.nonce.is_some() {
                                    //     let feed = Message::Feedback { crc: vec![req.crc], from_id: Some(my_id), to_id: Some(to), status: FeedbackStatus::Sending };
                                    //     let feed = serde_cbor::to_vec(&feed).unwrap();
                                    //     cb_t.on_message(req.crc, my_id, to,event_time, feed);
                                    // }
                                    let mut push = pendings_t.write().await;
                                    push.push_back((req, 1, Instant::now()));

                                    // }
                                }
                            }
                        });
                    } else {
                        tracing::error!("is to me---->");
                    }
                }
                None => {
                    if let Err(_e) = channel.send(Ok(0)) {
                        tracing::error!("channel send failed");
                    }
                }
            }
        }

        // luffa_util::block_until_sigint().await;
        // ctl.await.unwrap();
        sync_task.abort();
        process.abort();
        p2p_rpc.abort();
        pending_task.abort();
        // metrics_handle.shutdown();
    }
}