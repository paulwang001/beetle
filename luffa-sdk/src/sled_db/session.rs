use crate::sled_db::contacts::ContactsDb;
use crate::{ChatSession, ClientResult};
use sled::{Db, Tree};
use std::sync::Arc;
use tracing::warn;

pub const KVDB_CHAT_SESSION_TREE: &str = "luffa_sessions";

pub trait SessionDb: ContactsDb {
    fn open_session_tree(db: Arc<Db>) -> ClientResult<Tree> {
        let table = format!("{}", KVDB_CHAT_SESSION_TREE);
        let tree = db.open_tree(table)?;
        Ok(tree)
    }

    /// pagination session list
    fn db_session_list(
        db: Arc<Db>,
        page: u32,
        page_size: u32,
        my_id: u64,
    ) -> Option<Vec<ChatSession>> {
        let tree = Self::open_session_tree(db).unwrap();

        let mut chats = tree
            .into_iter()
            .map(|item| {
                let (_key, val) = item.unwrap();
                let chat: ChatSession = serde_cbor::from_slice(&val[..]).unwrap();
                chat
            })
            .filter(|c| c.did != my_id)
            .collect::<Vec<_>>();
        chats.sort_by(|a, b| a.last_time.partial_cmp(&b.last_time).unwrap());
        chats.reverse();
        let page = chats.chunks(page_size as usize).nth(page as usize);
        page.map(|ls| ls.into_iter().map(|s| s.clone()).collect::<Vec<_>>())
    }

    fn update_session(
        db: Arc<Db>,
        did: u64,
        tag: Option<String>,
        read: Option<u64>,
        reach: Option<u64>,
        msg: Option<String>,
        status: Option<u8>,
        event_time: u64,
    ) -> bool {
        if did == 0 {
            return false;
        }
        let tree = Self::open_session_tree(db.clone()).unwrap();
        // assert!(did > 0,"update_session:{msg:?} ,{tag:?}");
        let mut first_read = false;
        let n_tag = tag.clone();
        if let Err(e) = tree.fetch_and_update(did.to_be_bytes(), |old| {
            match old {
                Some(val) => {
                    let chat: ChatSession = serde_cbor::from_slice(val).unwrap();
                    let ChatSession {
                        did,
                        session_type,
                        last_time,
                        tag,
                        read_crc,
                        mut reach_crc,
                        last_msg,
                        enabled_silent,
                        last_msg_status,
                    } = chat;
                    let mut last_time = last_time;
                    let mut last_msg = last_msg;
                    if let Some(c) = reach {
                        if !reach_crc.contains(&c) {
                            reach_crc.push(c);
                        }
                        if last_time < event_time {

                            last_time = event_time;
                            last_msg = msg.clone().unwrap_or(last_msg);
                        }
                    }
                    if let Some(c) = read.as_ref() {
                        first_read = reach_crc.contains(c);
                        reach_crc.retain(|x| *x != *c);
                        // assert!(reach_crc.contains(c),"reach contain :{c}");
                        // warn!("reach_crc:{reach_crc:?}   {c}");
                    }
                    let mut last_msg_status = last_msg_status;
                    if let Some(s) = status {
                        if s > last_msg_status {
                            last_msg_status = s;
                        }
                    }
                    let upd = ChatSession {
                        did,
                        session_type,
                        last_time,
                        tag: n_tag.clone().unwrap_or(tag),
                        read_crc: read.unwrap_or(read_crc),
                        reach_crc,
                        last_msg,
                        enabled_silent,
                        last_msg_status,
                    };
                    Some(serde_cbor::to_vec(&upd).unwrap())
                }
                None => {
                    let (dft, tp) =
                        Self::get_contacts_tag(db.clone(), did).unwrap_or((format!("{did}"), 3));
                    if tp == 3 {
                        tracing::error!("update session failed:{did},{dft}");
                        return None;
                    }
                    let mut reach_crc = vec![];
                    if let Some(c) = reach {
                        reach_crc.push(c);
                    }
                    let upd = ChatSession {
                        did,
                        session_type: tp,
                        last_time: event_time,
                        tag: n_tag.clone().unwrap_or(dft),
                        read_crc: read.unwrap_or_default(),
                        reach_crc,
                        last_msg: msg.clone().unwrap_or_default(),
                        enabled_silent: false,
                        last_msg_status: status.unwrap_or_default(),
                    };
                    Some(serde_cbor::to_vec(&upd).unwrap())
                }
            }
        }) {
            tracing::info!("{e:?}");
        }
        tree.flush().unwrap();
        first_read
    }

    fn remove_session(db: Arc<Db>, did: u64) -> ClientResult<()> {
        let tree = Self::open_session_tree(db).unwrap();

        let _ = tree.remove(did.to_be_bytes())?;

        Ok(())
    }

    fn update_session_for_last_msg(
        db: Arc<Db>,
        did: u64,
        at: u64,
        msg: &str,
    ) {
        if did == 0 {return;}

        let tree = Self::open_session_tree(db.clone()).unwrap();
        if let Err(e) =
            tree.fetch_and_update(did.to_be_bytes(), |old| {
                let mut v = match
                old.map(|x| serde_cbor::from_slice::<ChatSession>(x).ok()).flatten() {
                    Some(v) => v,
                    _ => return None,
                };
                // let Some(mut v) = old.map(|x| serde_cbor::from_slice::<ChatSession>(x).ok()).flatten() else {
                //     return None;
                // };
                // let Some(mut v) = old.map(|x| serde_cbor::from_slice::<ChatSession>(x).ok()).flatten() else {
                //     return None;
                // };

                v.last_time = at;
                v.last_msg = msg.to_string();

                Some(serde_cbor::to_vec(&v).unwrap())
            })
        {
            warn!("update session for last msg failed {e:?}");
        }

        tree.flush().expect("tree flush failed");
    }


    fn enable_session_silent(
        db: Arc<Db>,
        did: u64,
    )  {
        Self::update_session_if_exists(db, did, &|chat| chat.enabled_silent = true);
    }

    fn disable_session_silent(
        db: Arc<Db>,
        did: u64,
    ) {
        Self::update_session_if_exists(db, did, &|chat| chat.enabled_silent = false);
    }

    fn update_session_if_exists(db: Arc<Db>, did: u64, callback: &dyn Fn(&mut ChatSession)) {
        if did == 0 {
            return;
        }
        let tree = Self::open_session_tree(db.clone()).unwrap();
        if let Err(e) = tree.fetch_and_update(did.to_be_bytes(), |old| {
            match old {
                Some(val) => {
                    let mut chat: ChatSession = serde_cbor::from_slice(val).unwrap();
                    callback(&mut chat);

                    Some(serde_cbor::to_vec(&chat).unwrap())
                }
                None => {
                    warn!("session is not found {did}");
                    None
                }
            }
        }) {
            tracing::info!("{e:?}");
        }
        tree.flush().unwrap();
    }
}
