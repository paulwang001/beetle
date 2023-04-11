use crate::api::P2pClient;
use crate::{ClientResult, OfferStatus};
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use luffa_rpc_types::{ChatContent, ContactsToken, ContactsTypes};
use sled::{Db, Tree};
use std::sync::Arc;
use tantivy::schema::Schema;
use tantivy::{doc, IndexWriter};
use tokio::sync::RwLock;
use tracing::error;

pub const KVDB_CONTACTS_TREE: &str = "luffa_contacts";

pub trait ContactsDb {
    fn open_contact_tree(db: Arc<Db>) -> ClientResult<Tree> {
        let table = format!("{}", KVDB_CONTACTS_TREE);
        let tree = db.open_tree(table)?;
        Ok(tree)
    }

    fn save_contacts(
        db: Arc<Db>,
        to: u64,
        secret_key: Vec<u8>,
        public_key: Vec<u8>,
        c_type: ContactsTypes,
        sign: Vec<u8>,
        comment: Option<String>,
        g_keypair: Option<Vec<u8>>,
    ) {
        let tree = Self::open_contact_tree(db).unwrap();
        tracing::info!("save_contacts----->{to}");
        let s_key = format!("SKEY-{}", to);
        let p_key = format!("PKEY-{}", to);
        let sig_key = format!("SIG-{}", to);
        let tag_key = format!("TAG-{}", to);
        let type_key = format!("TYPE-{}", to);
        tree.insert(s_key.as_bytes(), secret_key).unwrap();
        tree.insert(p_key.as_bytes(), public_key).unwrap();
        tree.insert(sig_key.as_bytes(), sign).unwrap();
        tree.insert(type_key.as_bytes(), vec![c_type as u8])
            .unwrap();
        tree.insert(
            tag_key.as_bytes(),
            comment.unwrap_or(format!("{to}")).as_bytes(),
        )
        .unwrap();
        if let Some(keypair) = g_keypair {
            let group_keypair = format!("GROUPKEYPAIR-{}", to);
            tree.insert(group_keypair, keypair).unwrap();
        }
        tree.flush().unwrap();
    }

    fn remove_contacts(db: Arc<Db>, id: u64) -> ClientResult<()> {
        let tree = Self::open_contact_tree(db).unwrap();
        tracing::info!("remove_contacts----->{id}");
        let s_key = format!("SKEY-{}", id);
        let p_key = format!("PKEY-{}", id);
        let sig_key = format!("SIG-{}", id);
        let tag_key = format!("TAG-{}", id);
        let type_key = format!("TYPE-{}", id);
        let group_keypair = format!("GROUPKEYPAIR-{}", id);

        tree.remove(s_key.as_bytes()).unwrap();
        tree.remove(p_key.as_bytes()).unwrap();
        tree.remove(sig_key.as_bytes()).unwrap();
        tree.remove(tag_key.as_bytes()).unwrap();
        tree.remove(type_key.as_bytes()).unwrap();
        tree.remove(group_keypair.as_bytes()).unwrap();

        Ok(())
    }

    fn get_key(db: Arc<Db>, key: &str) -> Option<Vec<u8>> {
        let tree = Self::open_contact_tree(db).unwrap();
        if let Ok(data) = tree.get(key.as_bytes()) {
            data.map(|x| x.to_vec())
        } else {
            None
        }
    }

    fn save_to_tree(db: Arc<Db>, crc: u64, table: &str, data: Vec<u8>, event_time: u64) {
        let tree = db.open_tree(&table).unwrap();

        match tree.insert(crc.to_be_bytes(), data) {
            Ok(None) => {
                let tree_time = db.open_tree(&format!("{table}_time")).unwrap();
                tree_time
                    .insert(event_time.to_be_bytes(), crc.to_be_bytes().to_vec())
                    .unwrap();
                tree_time.flush().unwrap();
            }
            _ => {}
        }

        tree.flush().unwrap();
    }
    fn save_offer_to_tree(
        db: Arc<Db>,
        did: u64,
        crc: u64,
        offer_id: u64,
        offer_key: Vec<u8>,
        status: OfferStatus,
        c_type: ContactsTypes,
        tag: String,
        event_time: u64,
    ) {
        let table = format!("offer");
        let tree = db.open_tree(&table).unwrap();

        match tree.insert(format!("SK_{crc}"), offer_key) {
            Ok(None) => {
                tree.insert(format!("ST_{crc}"), vec![status as u8])
                    .unwrap();
                tree.insert(format!("TP_{crc}"), vec![c_type as u8])
                    .unwrap();
                tree.insert(format!("OF_{crc}"), offer_id.to_be_bytes().to_vec())
                    .unwrap();
                tree.insert(format!("DID_{crc}"), did.to_be_bytes().to_vec())
                    .unwrap();
                tree.insert(format!("TAG_{crc}"), tag.as_bytes()).unwrap();
                let tree_time = db.open_tree(&format!("{table}_time")).unwrap();
                tree_time
                    .insert(event_time.to_be_bytes(), crc.to_be_bytes().to_vec())
                    .unwrap();
                tree_time.flush().unwrap();
            }
            _ => {}
        }

        tree.flush().unwrap();
    }

    fn remove_offer_in_tree(db: Arc<Db>,crc: u64) {
        let tree = db.open_tree("offer").unwrap();
        tree.remove(format!("SK_{crc}")).unwrap();
        tree.remove(format!("ST_{crc}")).unwrap();
        tree.remove(format!("TP_{crc}")).unwrap();
        tree.remove(format!("OF_{crc}")).unwrap();
        tree.remove(format!("DID_{crc}")).unwrap();
        tree.remove(format!("TAG_{crc}")).unwrap();

        // offer_time 表中数据删除，做在读取消息时删除
    }

    fn if_exists_offer_in_tree(db: Arc<Db>, crc: u64) -> bool {
        let tree = db.open_tree("offer").unwrap();

        tree.contains_key(format!("SK_{crc}")).unwrap_or(false)
    }

    fn get_answer_from_tree(db: Arc<Db>, crc: u64) -> Option<(u64, Vec<u8>, u8)> {
        let table = format!("offer");
        let tree = db.open_tree(&table).unwrap();
        if let Ok(Some(key)) = tree.get(&format!("SK_{crc}")) {
            if let Some(offer_id) = Self::get_u64_from_tree(&tree,&format!("OF_{crc}")) {
                if let Some(status) = Self::get_u8_from_tree(&tree,&format!("ST_{crc}")) {
                    return Some((offer_id,key.to_vec(),status))    
                }
            }
        }
        None
    }
    fn get_u64_from_tree(tree: &Tree, key: &str) -> Option<u64> {
        if let Ok(Some(v)) = tree.get(key) {
            let mut val = [0u8;8];
            val.copy_from_slice(&v);
            let val = u64::from_be_bytes(val);
            Some(val)    
        }
        else{
            None
        }
    }
    fn get_u8_from_tree(tree: &Tree, key: &str) -> Option<u8> {
        if let Ok(Some(v)) = tree.get(key) {
            let mut val = [0u8;1];
            val.copy_from_slice(&v);
            
            Some(val[0])    
        }
        else{
            None
        }
    }
    fn update_offer_status(db: Arc<Db>, crc: u64, status: OfferStatus) {
        let table = format!("offer");
        let tree = db.open_tree(&table).unwrap();

        tree.insert(format!("ST_{crc}"), vec![status as u8])
            .unwrap();

        tree.flush().unwrap();
    }
    fn get_offer_status(db: Arc<Db>, crc: u64) -> OfferStatus {
        let table = format!("offer");
        let tree = db.open_tree(&table).unwrap();
        if let Ok(Some(val)) = tree.get(&format!("ST_{crc}")) {
            let st = val[0];
            OfferStatus::from(st)
        } else {
            OfferStatus::Offer
        }
    }
    fn save_to_tree_status(db: Arc<Db>, crc: u64, table: &str, status: u8) {
        let tree_status = db.open_tree(&format!("{table}_status")).unwrap();
        if let Err(e) = tree_status.fetch_and_update(crc.to_be_bytes(), |old| {
            match old {
            Some(o) => {
                if o[0] < status {
                    Some(vec![status])
                } else {
                    let old = o[0];
                    Some(vec![old])
                }
            }
            None => Some(vec![status]),
            }
        }) {
            tracing::error!("{e:?}")
        }
        // match tree_status.insert(crc.to_be_bytes(), vec![status]) {
        //     Ok(None) => {

        //     }
        //     _ => {

        //     }
        // }
        tree_status.flush().unwrap();
    }
    fn get_crc_tree_status(db: Arc<Db>, crc: u64, table: &str) -> u8 {
        let tree_status = db.open_tree(&format!("{table}_status")).unwrap();

        match tree_status.get(crc.to_be_bytes()) {
            Ok(Some(val)) => {
                val.to_vec()[0]
            }
            _ => {
                0
            }
        }
    }

    fn have_in_tree(db: Arc<Db>, crc: u64, table: &str) -> bool {
        let tree = db.open_tree(table).unwrap();
        if let Ok(r) = tree.contains_key(crc.to_be_bytes()) {
            r
        } else {
            false
        }
    }

    fn burn_from_tree(db: Arc<Db>, crc: u64, table: String) {
        let tree = db.open_tree(&table).unwrap();

        tree.remove(crc.to_be_bytes()).unwrap();
        tree.flush().unwrap();
    }

    fn get_offer_by_offer_id(db: Arc<Db>, offer_id: u64) -> Option<Vec<u8>> {
        let tree = Self::open_contact_tree(db).unwrap();
        let offer_key = format!("OFFER-{}", offer_id);
        match tree.get(offer_key) {
            Ok(Some(data)) => Some(data.to_vec()),
            _ => None,
        }
    }
    
    fn get_contacts_skey(db: Arc<Db>, did: u64) -> Option<Vec<u8>> {
        let tree = Self::open_contact_tree(db).unwrap();
        let s_key = format!("SKEY-{}", did);
        match tree.get(s_key) {
            Ok(Some(data)) => Some(data.to_vec()),
            _ => None,
        }
    }

    fn get_contacts_tag(db: Arc<Db>, did: u64) -> Option<(String, u8)> {
        let tree = Self::open_contact_tree(db.clone()).unwrap();
        let tag_key = format!("TAG-{}", did);
        match tree.get(&tag_key) {
            Ok(Some(v)) => {
                let tp = Self::get_contacts_type(db, did).unwrap_or(ContactsTypes::Private);
                Some((String::from_utf8(v.to_vec()).unwrap(), tp as u8))
            }
            _ => None,
        }
    }
    
    fn get_contacts_type(db: Arc<Db>, did: u64) -> Option<ContactsTypes> {
        let tree = Self::open_contact_tree(db).unwrap();
        let type_key = format!("TYPE-{}", did);
        match tree.get(&type_key) {
            Ok(Some(v)) => {
                let tp = v[0];
                let tp = if tp == 0 {
                    ContactsTypes::Private
                } else {
                    ContactsTypes::Group
                };
                Some(tp)
            }
            _ => None,
        }
    }

    fn set_contacts_tag(db: Arc<Db>, did: u64, tag: String) {
        let tree = Self::open_contact_tree(db).unwrap();
        let tag_key = format!("TAG-{}", did);
        println!("set tag:{did} ==> {}", tag);
        tree.insert(tag_key.as_bytes(), tag.as_bytes()).unwrap();
        tree.flush().unwrap();
    }

    fn get_contacts_have_time(db: Arc<Db>, did: u64) -> u64 {
        let tree = Self::open_contact_tree(db).unwrap();
        let tag_key = format!("H-TIME-{}", did);
        if let Ok(Some(x)) = tree.get(tag_key.as_bytes()) {
            let mut val = [0u8; 8];
            val.clone_from_slice(&x);
            u64::from_be_bytes(val)
        } else {
            0
        }
    }
    fn set_contacts_have_time(db: Arc<Db>, did: u64, now: u64) {
        let tree = Self::open_contact_tree(db).unwrap();
        let tag_key = format!("H-TIME-{}", did);
        tree.fetch_and_update(tag_key.as_bytes(), |old| match old {
            Some(old) => {
                let mut val = [0u8; 8];
                val.clone_from_slice(&old);
                let old = u64::from_be_bytes(val);
                if old < now {
                    Some(now.to_be_bytes().to_vec())
                } else {
                    Some(old.to_be_bytes().to_vec())
                }
            }
            None => Some(now.to_be_bytes().to_vec()),
        })
            .unwrap();
        tree.flush().unwrap();
    }

    fn get_aes_key_from_contacts(db: Arc<Db>, did: u64) -> Option<Vec<u8>> {
        let tree = Self::open_contact_tree(db).unwrap();
        let s_key = format!("SKEY-{}", did);
        match tree.get(&s_key) {
            Ok(Some(d)) => Some(d.to_vec()),
            _ => None,
        }
    }
}
