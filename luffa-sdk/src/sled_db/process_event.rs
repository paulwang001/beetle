use std::sync::Arc;

use libp2p::{identity::PublicKey, PeerId};
use luffa_rpc_types::{
    message_from, ChatContent, Contacts, ContactsEvent, ContactsToken, ContactsTypes, Event,
    FeedbackStatus, Message,
};
use parking_lot::RwLock;
use sled::Db;
use tantivy::{doc, schema::Schema, IndexWriter, Term};

use crate::event::group::EventGroup;
use crate::sled_db::contacts::{ContactsDb, KVDB_CONTACTS_TREE};
use crate::sled_db::group_members::{GroupMemberNickname, GroupMembersDb};
use crate::sled_db::mnemonic::Mnemonic;
use crate::sled_db::nickname::Nickname;
use crate::sled_db::session::SessionDb;
use crate::sled_db::SledDbAll;
use crate::{api::P2pClient, Callback, Client, OfferStatus};
use tokio::sync::oneshot::Sender as ShotSender;
use tracing::error;

impl Client {
    pub async fn process_event(
        db_t: Arc<Db>,
        cb: Arc<Box<dyn Callback>>,
        client_t: P2pClient,
        idx: Arc<RwLock<IndexWriter>>,
        schema: Schema,
        data: &Vec<u8>,
        my_id: u64,
        session_last_crc: Arc<RwLock<sled::Tree>>,
        sender: tokio::sync::mpsc::Sender<(
            u64,
            Vec<u8>,
            u64,
            ShotSender<anyhow::Result<u64>>,
            Option<Vec<u8>>,
        )>,
    ) {
        if let Ok(im) = Event::decode_uncheck(&data) {
            let Event {
                msg,
                nonce,
                to,
                from_id,
                crc,
                event_time,
                ..
            } = im.clone();
            let feed = luffa_rpc_types::Message::Feedback {
                crc: vec![crc],
                from_id: Some(my_id),
                to_id: Some(0),
                status: luffa_rpc_types::FeedbackStatus::Reach,
            };
            let event = luffa_rpc_types::Event::new(0, &feed, None, my_id, None);
            tracing::warn!("send feedback reach to relay {crc}");
            let event = event.encode().unwrap();
            let client_tt = client_t.clone();
            tokio::spawn(async move {
                if let Err(e) = client_tt.chat_request(bytes::Bytes::from(event)).await {
                    error!("{e:?}");
                }
            });

            let did = if to == my_id { from_id } else { to };

            if nonce.is_none() {
                let msg_t = message_from(msg.clone());
                tracing::warn!("nonce is none>> {crc} msg:{msg_t:?}");
                tokio::spawn(async move {
                    cb.on_message(crc, from_id, to, event_time, msg);
                });
                return;
            }
            if from_id == my_id {
                tracing::error!("from_id == my_id   crc:{crc}");
                return;
            }

            let evt_data = data.clone();

            let keys = vec![Self::get_aes_key_from_contacts(db_t.clone(), did), None];
            for key in keys {
                let msg = msg.clone();
                let cb = cb.clone();
                let nonce = nonce.clone();
                match key {
                    Some(key) => {
                        if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                            bytes::Bytes::from(msg),
                            Some(key.clone()),
                            nonce.clone(),
                        ) {
                            // TODO: did is me or I'm a member any local group
                            let msg_data = serde_cbor::to_vec(&msg).unwrap();
                            tracing::warn!("from relay request e2e crc:[{crc}] msg>>>>{msg:?}");
                            if msg.is_important() {
                                let last_crc = session_last_crc.write();
                                match last_crc.insert(did.to_be_bytes(), crc.to_be_bytes().to_vec())
                                {
                                    Ok(_v) => {
                                        last_crc.flush().unwrap();
                                    }
                                    Err(e) => {
                                        tracing::error!("{e:?}");
                                    }
                                }
                            }
                            let mut will_save = false;
                            match msg {
                                Message::Chat { content } => {
                                    // TODO index content search engine
                                    match content {
                                        ChatContent::Burn { crc, .. } => {
                                            let table = format!("message_{did}");
                                            Self::burn_from_tree(db_t.clone(), crc, table);

                                            let fld_crc = schema.get_field("crc").unwrap();
                                            let mut wr = idx.write();
                                            let del = Term::from_field_u64(fld_crc, crc);
                                            wr.delete_term(del);
                                            wr.commit().unwrap();
                                            will_save = true;
                                        }
                                        ChatContent::Send { data } => {
                                            will_save = true;
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
                                                        let body = format!("");
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
                                                } => (title, format!("")),
                                            };
                                            // let tree = db.open_tree(KVDB_CONTACTS_TREE).unwrap();
                                            let (msg_type, did) = if to == my_id {
                                                // let tag_key_private = format!("TAG-0-{}", from_id);
                                                (format!("content_private"), from_id)
                                            } else {
                                                // let tag_key_group = format!("TAG-1-{}", to);
                                                // let tag =
                                                // match tree.get(&tag_key_group) {
                                                //     Ok(Some(v))=>{
                                                //         Some(String::from_utf8(v.to_vec()).unwrap())
                                                //     }
                                                //     _=>{
                                                //         None
                                                //     }
                                                // };
                                                (format!("content_group"), to)
                                            };

                                            Self::update_session(
                                                db_t.clone(),
                                                did,
                                                None,
                                                None,
                                                Some(crc),
                                                Some(body.clone()),
                                                Some(4),
                                                event_time,
                                            );
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
                                            let mut wr = idx.write();
                                            wr.add_document(doc).unwrap();
                                            wr.commit().unwrap();
                                        }
                                        ChatContent::Feedback { crc, status, .. } => match status {
                                            FeedbackStatus::Reach => {
                                                let table = format!("message_{did}");
                                                Self::save_to_tree_status(
                                                    db_t.clone(),
                                                    crc,
                                                    &table,
                                                    4,
                                                );
                                                Self::update_session(
                                                    db_t.clone(),
                                                    did,
                                                    None,
                                                    None,
                                                    None,
                                                    None,
                                                    Some(4),
                                                    event_time,
                                                );
                                            }
                                            FeedbackStatus::Read => {
                                                let table = format!("message_{did}");
                                                Self::save_to_tree_status(
                                                    db_t.clone(),
                                                    crc,
                                                    &table,
                                                    5,
                                                );
                                                Self::update_session(
                                                    db_t.clone(),
                                                    did,
                                                    None,
                                                    None,
                                                    None,
                                                    None,
                                                    Some(5),
                                                    event_time,
                                                );
                                            }
                                            _ => {}
                                        },
                                    }
                                }
                                Message::WebRtc { .. } => {
                                    will_save = true;
                                }
                                Message::ContactsExchange { exchange } => {
                                    will_save = true;
                                    match exchange {
                                        ContactsEvent::Answer {
                                            token,
                                            members,
                                            offer_crc,
                                        } => {
                                            if offer_crc > 0 {
                                                Self::update_offer_status(
                                                    db_t.clone(),
                                                    offer_crc,
                                                    OfferStatus::Answer,
                                                );
                                            }

                                            let comment = token.comment.clone();
                                            let secret_key = token.secret_key.clone();
                                            // let contacts_type = token.contacts_type.clone();
                                            let did = Self::on_answer(
                                                crc,
                                                from_id,
                                                event_time,
                                                idx.clone(),
                                                schema.clone(),
                                                token,
                                                db_t.clone(),
                                            )
                                            .await;

                                            if members.len() > 0 {
                                                let member_ids: Vec<u64> =
                                                    members.iter().map(|a| a.u_id).collect();
                                                Self::group_member_insert(
                                                    db_t.clone(),
                                                    did,
                                                    member_ids,
                                                )
                                                .unwrap();
                                                Self::set_group_members_nickname(
                                                    db_t.clone(),
                                                    did,
                                                    members,
                                                )
                                                .unwrap();
                                            }
                                            tracing::warn!("G> Answer>>>>>{did}");

                                            let contacts = vec![Contacts {
                                                did: did,
                                                r#type: ContactsTypes::Group,
                                            }];

                                            let sync = Message::ContactsSync {
                                                did: my_id,
                                                contacts,
                                            };

                                            let event = Event::new(did, &sync, None, my_id, None);
                                            let data = event.encode().unwrap();
                                            let client_tt = client_t.clone();
                                            let db_tt = db_t.clone();
                                            tokio::spawn(async move {
                                                if let Err(e) = client_tt
                                                    .chat_request(bytes::Bytes::from(data))
                                                    .await
                                                {
                                                    error!("sync contacts {did} {e:?}");
                                                }
                                                Self::send_group_join(
                                                    db_tt.clone(),
                                                    client_tt.clone(),
                                                    secret_key,
                                                    did,
                                                    my_id,
                                                    offer_crc,
                                                )
                                                .await;
                                            });
                                            Self::update_session(
                                                db_t.clone(),
                                                did,
                                                comment.clone(),
                                                None,
                                                None,
                                                None,
                                                None,
                                                event_time,
                                            );
                                        }
                                        ContactsEvent::Offer { token } => {
                                            tracing::warn!("G> Offer>>>>>{token:?}");
                                            // token.validate()

                                            let ContactsToken {
                                                public_key,
                                                secret_key,
                                                contacts_type,
                                                comment,
                                                ..
                                            } = token;
                                            let pk = PublicKey::from_protobuf_encoding(&public_key)
                                                .unwrap();
                                            let peer = PeerId::from_public_key(&pk);
                                            let mut digest = crc64fast::Digest::new();
                                            digest.write(&peer.to_bytes());
                                            let did = digest.sum64();

                                            let offer_key = if let Some(key) =
                                                Self::get_contacts_skey(db_t.clone(), did)
                                            {
                                                tracing::error!("change contacts to old s key");
                                                key
                                            } else {
                                                secret_key
                                            };
                                            let tag = comment.unwrap_or_default();
                                            Self::save_offer_to_tree(
                                                db_t.clone(),
                                                did,
                                                crc,
                                                from_id,
                                                offer_key,
                                                OfferStatus::Offer,
                                                contacts_type,
                                                tag,
                                                event_time,
                                            );
                                        }
                                        ContactsEvent::Reject {
                                            offer_crc,
                                            public_key,
                                        } => {
                                            if let Ok(pk) =
                                                PublicKey::from_protobuf_encoding(&public_key)
                                            {
                                                let peer = PeerId::from_public_key(&pk);
                                                let mut digest = crc64fast::Digest::new();
                                                digest.write(&peer.to_bytes());
                                                let did = digest.sum64();
                                                let table = format!("message_{did}");
                                                Self::save_to_tree(
                                                    db_t.clone(),
                                                    crc,
                                                    &table,
                                                    evt_data.clone(),
                                                    event_time,
                                                );
                                                Self::update_offer_status(
                                                    db_t.clone(),
                                                    offer_crc,
                                                    OfferStatus::Reject,
                                                );
                                                let feed = luffa_rpc_types::Message::Chat {
                                                    content: ChatContent::Feedback {
                                                        crc,
                                                        last_crc: 0,
                                                        status:
                                                            luffa_rpc_types::FeedbackStatus::Reach,
                                                    },
                                                };
                                                let event = luffa_rpc_types::Event::new(
                                                    from_id,
                                                    &feed,
                                                    Some(key),
                                                    my_id,
                                                    None,
                                                );
                                                tracing::error!("send feedback reach to {from_id}");
                                                let event = event.encode().unwrap();
                                                if let Err(e) = client_t
                                                    .chat_request(bytes::Bytes::from(event))
                                                    .await
                                                {
                                                    error!("{e:?}");
                                                }
                                                tokio::spawn(async move {
                                                    cb.on_message(
                                                        crc, from_id, to, event_time, msg_data,
                                                    );
                                                });
                                            }
                                            return;
                                        }
                                        ContactsEvent::Join {
                                            offer_crc,
                                            group_nickname,
                                        } => {
                                            error!("ContactsEvent::Join1: crc {crc} ,{offer_crc} {group_nickname} {did} {from_id}");
                                            Self::group_join_member(
                                                db_t.clone(),
                                                did,
                                                from_id,
                                                &group_nickname,
                                            )
                                            .await;
                                            let secret_key = Self::get_key(
                                                db_t.clone(),
                                                &format!("SKEY-{}", did),
                                            )
                                            .unwrap();
                                            Self::group_sync(
                                                db_t.clone(),
                                                client_t.clone(),
                                                secret_key,
                                                did,
                                                my_id,
                                            )
                                            .await;
                                        }
                                        ContactsEvent::Leave { id } => {
                                            Self::group_member_remove(db_t.clone(), did, id)
                                                .expect("remove group member failed");
                                        }

                                        ContactsEvent::Sync { g_id, members } => {
                                            error!("ContactsEvent::Sync1: {g_id}");
                                            Self::group_join_members(db_t.clone(), g_id, members)
                                                .await;
                                        }
                                    }
                                }
                                _ => {
                                    error!("ContactsEvent::Unknown:{crc}");
                                }
                            }
                            if will_save {
                                let table = format!("message_{did}");
                                Self::save_to_tree(
                                    db_t.clone(),
                                    crc,
                                    &table,
                                    evt_data.clone(),
                                    event_time,
                                );
                                let last_crc = session_last_crc.read();
                                let last_crc =
                                    Self::get_u64_from_tree(&last_crc, &did.to_be_bytes())
                                        .unwrap_or_default();
                                let feed = luffa_rpc_types::Message::Chat {
                                    content: ChatContent::Feedback {
                                        crc,
                                        last_crc,
                                        status: luffa_rpc_types::FeedbackStatus::Reach,
                                    },
                                };
                                let event =
                                    luffa_rpc_types::Event::new(did, &feed, Some(key), my_id, None);
                                tracing::error!("send feedback reach to {from_id}");
                                let event = event.encode().unwrap();
                                let client_tt = client_t.clone();
                                tokio::spawn(async move {
                                    if let Err(e) =
                                        client_tt.chat_request(bytes::Bytes::from(event)).await
                                    {
                                        error!("{e:?}");
                                    }
                                });
                            }
                            tokio::spawn(async move {
                                cb.on_message(crc, from_id, to, event_time, msg_data);
                            });

                            return;
                        } else {
                            error!("decrypt failed!!! {did} {crc} {:?}", nonce);
                        }
                    }
                    None => {
                        tracing::warn!("aes not in contacts {did}  {crc}");
                        // warn!("Gossipsub> peer_id: {from:?} nonce:{:?}", nonce);
                        if let Some(key) = Self::get_offer_by_offer_id(db_t.clone(), from_id) {
                            tracing::info!(
                                "offer is:{}  nonce:{:?} key: {:?}",
                                from_id,
                                nonce,
                                key
                            );
                            if let Ok(msg) = luffa_rpc_types::Message::decrypt(
                                bytes::Bytes::from(msg),
                                Some(key.clone()),
                                nonce,
                            ) {
                                let msg_t = msg.clone();

                                let msg_data = serde_cbor::to_vec(&msg).unwrap();
                                let offer_id = from_id;
                                let offer_key = key.clone();
                                match msg_t {
                                    Message::ContactsExchange { exchange } => {
                                        match exchange {
                                            ContactsEvent::Answer {
                                                token,
                                                offer_crc,
                                                members,
                                            } => {
                                                tracing::warn!("[P]Answer>>>>>{token:?}");
                                                if offer_crc > 0 {
                                                    Self::update_offer_status(
                                                        db_t.clone(),
                                                        offer_crc,
                                                        OfferStatus::Answer,
                                                    );
                                                }

                                                let comment = token.comment.clone();
                                                let did = Self::on_answer(
                                                    crc,
                                                    offer_id,
                                                    event_time,
                                                    idx.clone(),
                                                    schema.clone(),
                                                    token.clone(),
                                                    db_t.clone(),
                                                )
                                                .await;
                                                let table = format!("message_{did}");
                                                Self::save_to_tree(
                                                    db_t.clone(),
                                                    crc,
                                                    &table,
                                                    evt_data.clone(),
                                                    event_time,
                                                );
                                                if token.contacts_type == ContactsTypes::Group {
                                                    if members.len() > 0 {
                                                        let member_ids: Vec<u64> = members
                                                            .iter()
                                                            .map(|a| a.u_id)
                                                            .collect();
                                                        Self::group_member_insert(
                                                            db_t.clone(),
                                                            did,
                                                            member_ids,
                                                        )
                                                        .unwrap();
                                                        Self::set_group_members_nickname(
                                                            db_t.clone(),
                                                            did,
                                                            members,
                                                        )
                                                        .unwrap();
                                                    }
                                                    Self::send_group_join(
                                                        db_t.clone(),
                                                        client_t.clone(),
                                                        token.secret_key.clone(),
                                                        did,
                                                        my_id,
                                                        offer_crc,
                                                    )
                                                    .await;
                                                } else {
                                                    let nickname =
                                                        Self::get_contacts_tag(db_t.clone(), my_id)
                                                            .map(|(name, _)| name)
                                                            .unwrap_or(
                                                                comment.clone().unwrap_or_default(),
                                                            );
                                                    let hi = luffa_rpc_types::Message::text(
                                                        format!("Hi,I'm {}", nickname),
                                                    );
                                                    let hi = serde_cbor::to_vec(&hi).unwrap();
                                                    let (req, res) =
                                                        tokio::sync::oneshot::channel();
                                                    sender
                                                        .send((
                                                            did,
                                                            hi,
                                                            my_id,
                                                            req,
                                                            Some(token.secret_key.clone()),
                                                        ))
                                                        .await
                                                        .unwrap();
                                                    tokio::spawn(async move {
                                                        match res.await {
                                                            Ok(r) => {
                                                                tracing::info!(
                                                                    "Hi ok:{}",
                                                                    r.is_ok()
                                                                );
                                                            }
                                                            Err(e) => {
                                                                tracing::warn!("{e:?}");
                                                            }
                                                        }
                                                    });
                                                }
                                                Self::update_session(
                                                    db_t.clone(),
                                                    did,
                                                    comment.clone(),
                                                    None,
                                                    None,
                                                    None,
                                                    None,
                                                    event_time,
                                                );
                                            }
                                            ContactsEvent::Offer { token } => {
                                                tracing::info!("[P] Offer>>>>>{token:?}");

                                                let ContactsToken {
                                                    public_key,
                                                    contacts_type,
                                                    comment,
                                                    ..
                                                } = token;
                                                if contacts_type == ContactsTypes::Group {
                                                    let group_keypair =
                                                        format!("GROUPKEYPAIR-{}", to);
                                                    let group_keypair =
                                                        Self::get_key(db_t.clone(), &group_keypair);
                                                    if group_keypair.is_none() {
                                                        tracing::warn!(
                                                            "group offer ---> not admin,skip"
                                                        );

                                                        println!("group offer ---> not admin,skip");
                                                        return;
                                                    }
                                                }
                                                let pk =
                                                    PublicKey::from_protobuf_encoding(&public_key)
                                                        .unwrap();
                                                let peer = PeerId::from_public_key(&pk);
                                                let mut digest = crc64fast::Digest::new();
                                                digest.write(&peer.to_bytes());

                                                //Offer from
                                                let did = digest.sum64();

                                                let tag = comment.unwrap_or_default();
                                                Self::save_offer_to_tree(
                                                    db_t.clone(),
                                                    did,
                                                    crc,
                                                    offer_id,
                                                    offer_key,
                                                    OfferStatus::Offer,
                                                    contacts_type,
                                                    tag,
                                                    event_time,
                                                );
                                                let table = format!("message_{did}");
                                                Self::save_to_tree(
                                                    db_t.clone(),
                                                    crc,
                                                    &table,
                                                    evt_data.clone(),
                                                    event_time,
                                                );
                                            }
                                            ContactsEvent::Reject {
                                                offer_crc,
                                                public_key,
                                            } => {
                                                if let Ok(pk) =
                                                    PublicKey::from_protobuf_encoding(&public_key)
                                                {
                                                    let peer = PeerId::from_public_key(&pk);
                                                    let mut digest = crc64fast::Digest::new();
                                                    digest.write(&peer.to_bytes());
                                                    let did = digest.sum64();
                                                    Self::update_offer_status(
                                                        db_t.clone(),
                                                        offer_crc,
                                                        OfferStatus::Reject,
                                                    );
                                                    let table = format!("message_{did}");
                                                    Self::save_to_tree(
                                                        db_t.clone(),
                                                        crc,
                                                        &table,
                                                        evt_data.clone(),
                                                        event_time,
                                                    );
                                                    tokio::spawn(async move {
                                                        cb.on_message(
                                                            crc, from_id, to, event_time, msg_data,
                                                        );
                                                    });
                                                }
                                                return;
                                            }
                                            ContactsEvent::Join {
                                                offer_crc,
                                                group_nickname,
                                            } => {
                                                error!("ContactsEvent::Join2: {offer_crc} {group_nickname} {did} {from_id}");
                                                Self::group_join_member(
                                                    db_t.clone(),
                                                    did,
                                                    from_id,
                                                    &group_nickname,
                                                )
                                                .await;
                                                let secret_key = Self::get_key(
                                                    db_t.clone(),
                                                    &format!("SKEY-{}", did),
                                                )
                                                .unwrap();
                                                Self::group_sync(
                                                    db_t.clone(),
                                                    client_t.clone(),
                                                    secret_key,
                                                    did,
                                                    my_id,
                                                )
                                                .await;
                                            }
                                            ContactsEvent::Leave { id } => {
                                                Self::group_member_remove(db_t.clone(), did, id)
                                                    .expect("remove group member failed");
                                            }
                                            ContactsEvent::Sync { g_id, members } => {
                                                error!("ContactsEvent::Sync2: {g_id} {members:?}");
                                                Self::group_join_members(
                                                    db_t.clone(),
                                                    g_id,
                                                    members,
                                                )
                                                .await;
                                            }
                                        };
                                    }
                                    Message::Chat { content } => match content {
                                        ChatContent::Feedback { crc, status, .. } => {
                                            tracing::warn!(
                                                "from offer Feedback crc<{crc}> {status:?}"
                                            );
                                        }
                                        _ => {
                                            tracing::error!("from offer content {crc} {content:?}");
                                        }
                                    },
                                    _ => {
                                        tracing::error!("from offer msg {crc} {msg:?}");
                                    }
                                }
                                tokio::spawn(async move {
                                    cb.on_message(crc, from_id, to, event_time, msg_data);
                                });

                                return;
                            } else {
                                tracing::error!("offer decrypt failed:>>>> crc: {crc}");
                            }
                        } else {
                            tracing::error!("invalid msg crc[{crc}] {im:?}");
                        }
                    }
                }
            }
        }
    }
}
