use luffa_rpc_types::ContactsEvent;
use sled::Db;
use std::sync::Arc;
use tracing::error;

use crate::{api::P2pClient, sled_db::group_members::GroupMembersDb};

#[async_trait::async_trait]
pub trait EventGroup: GroupMembersDb {
    async fn group_sync(
        db_t: Arc<Db>,
        client_t: P2pClient,
        secret_key: Vec<u8>,
        did: u64,
        my_id: u64,
    ) {
        let group_nickname = Self::get_group_member_nickname(db_t.clone(), did, my_id).unwrap();
        let join = luffa_rpc_types::Message::ContactsExchange {
            exchange: ContactsEvent::Sync {
                u_id: my_id,
                g_id: did,
                group_nickname,
            },
        };
        let event = luffa_rpc_types::Event::new(did, &join, Some(secret_key), my_id,None);
        let event = event.encode().unwrap();
        tracing::error!("send sync to group {did}");
        if let Err(e) = client_t.chat_request(bytes::Bytes::from(event)).await {
            error!("SendJoin1 {did} {e:?}");
        }
    }

    async fn group_join_member(db_t: Arc<Db>, group_id: u64, u_id: u64, group_nickname: &str) {
        Self::group_member_insert(db_t.clone(), group_id, vec![u_id]).unwrap();
        Self::set_group_member_nickname(db_t.clone(), group_id, u_id, group_nickname).unwrap();
        Self::set_member_to_join_status(db_t, group_id, u_id).unwrap();
    }

    async fn send_group_join(
        db_t: Arc<Db>,
        client_t: P2pClient,
        secret_key: Vec<u8>,
        group_id: u64,
        u_id: u64,
        offer_crc: u64,
    ) {
        let group_nickname = Self::get_group_member_nickname(db_t.clone(), group_id, u_id).unwrap();
        let join = luffa_rpc_types::Message::ContactsExchange {
            exchange: ContactsEvent::Join {
                offer_crc,
                group_nickname: group_nickname.to_owned(),
            },
        };
        let event = luffa_rpc_types::Event::new(group_id, &join, Some(secret_key), u_id,None);
        let event = event.encode().unwrap();
        tracing::error!("send join to group {group_id}");
        if let Err(e) = client_t.chat_request(bytes::Bytes::from(event)).await {
            error!("SendJoin1 {group_id} {e:?}");
        }

        Self::group_join_member(db_t.clone(), group_id, u_id, &group_nickname).await;
    }
}
