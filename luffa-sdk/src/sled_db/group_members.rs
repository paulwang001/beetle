use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use image::EncodableLayout;
use sled::{Db, Tree};
use serde::{Deserialize, Serialize};
use bytecheck::CheckBytes;
use crate::ClientError::CustomError;

use crate::ClientResult;
use crate::sled_db::local_config::read_local_id;

pub const KVDB_GROUP_MEMBERS_TREE: &str = "luffa_group_members";


#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Members {
    pub group_id: u64,
    pub members: BTreeSet<u64>,
}

impl Members {
    pub fn new(group_id: u64, members: Vec<u64>) -> Self {
        let mut list = BTreeSet::new();
        for member in members {
            list.insert(member);
        }
        Self { group_id, members: list }
    }

    pub fn to_bytes(&self) -> ClientResult<Vec<u8>> {
        Ok(serde_json::to_vec(&self)?)
    }

    pub fn deserialize(data: &[u8]) -> ClientResult<Self> {
         Ok(serde_json::from_slice(data)?)
    }
}


pub trait GroupMembersDb {
    fn open_group_member_tree(db: Arc<Db>) -> ClientResult<Tree> {
        let table = format!("{}-{}", KVDB_GROUP_MEMBERS_TREE, read_local_id());
        let tree = db.open_tree(table)?;
        Ok(tree)
    }

    fn group_member_insert(db: Arc<Db>, group_id: u64, member_ids: Vec<u64>) -> ClientResult<()> {
        let key = &format!("{}-{}", KVDB_GROUP_MEMBERS_TREE, group_id);
        let tree = Self::open_group_member_tree(db)?;
        let data = if let Some(data) = tree.get(key)? {
            let data = data.as_bytes();
            let mut members = Members::deserialize(data)?;
            let _ = member_ids.iter().map(|member_id| {
                members.members.insert(*member_id);
            });
            members.to_bytes()?
        } else {
            Members::new(group_id, member_ids).to_bytes()?
        };
        tree.insert(key, data)?;
        tree.flush()?;
        Ok(())
    }

    fn group_member_get(db: Arc<Db>, group_id: u64) -> ClientResult<Members> {
        let key = format!("{}-{}", KVDB_GROUP_MEMBERS_TREE, group_id);
        let tree = Self::open_group_member_tree(db)?;
        let members = if let Some(data) = tree.get(key)? {
            Members::deserialize(data.as_bytes())?
        } else {
            Members::new(group_id, vec![])
        };
        Ok(members)
    }
}