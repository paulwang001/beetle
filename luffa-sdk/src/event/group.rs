use std::sync::Arc;
use luffa_rpc_types::ContactsEvent;
use sled::Db;
use tracing::error;

use crate::{api::P2pClient, sled_db::nickname::Nickname};

#[async_trait::async_trait]
pub trait EventGroup: Nickname {
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
        let event = luffa_rpc_types::Event::new(did, &join, Some(secret_key), my_id);
        let event = event.encode().unwrap();
        tracing::error!("send sync to group {did}");
        if let Err(e) = client_t.chat_request(bytes::Bytes::from(event)).await {
            error!("SendJoin1 {did} {e:?}");
        }
    }

    async fn group_join(
        db_t: Arc<Db>,
        client_t: P2pClient,
        secret_key: Vec<u8>,
        did: u64,
        my_id: u64,
        offer_crc: u64,
    ) {
        let group_nickname = Self::get_group_member_nickname(db_t.clone(), did, my_id).unwrap();
        let join = luffa_rpc_types::Message::ContactsExchange {
            exchange: ContactsEvent::Join {
                offer_crc,
                group_nickname,
            },
        };
        let event = luffa_rpc_types::Event::new(did, &join, Some(secret_key), my_id);
        let event = event.encode().unwrap();
        tracing::error!("send join to group {did}");
        if let Err(e) = client_t.chat_request(bytes::Bytes::from(event)).await {
            error!("SendJoin1 {did} {e:?}");
        }
    }
}
