use crate::ClientError::CustomError;
use bytecheck::CheckBytes;
use image::EncodableLayout;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::sled_db::contacts::ContactsDb;
use crate::sled_db::nickname::Nickname;
use crate::ClientResult;

pub const KVDB_GROUP_MEMBERS_TREE: &str = "luffa_group_members";

#[derive(Debug)]
pub struct GroupMemberNickname {
    pub u_id: u64,
    pub nickname: String,
}

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
        Self {
            group_id,
            members: list,
        }
    }

    pub fn to_bytes(&self) -> ClientResult<Vec<u8>> {
        Ok(serde_json::to_vec(&self)?)
    }

    pub fn deserialize(data: &[u8]) -> ClientResult<Self> {
        Ok(serde_json::from_slice(data)?)
    }
}

pub trait GroupMembersDb: Nickname {
    fn open_group_member_tree(db: Arc<Db>) -> ClientResult<Tree> {
        let table = format!("{}", KVDB_GROUP_MEMBERS_TREE);
        let tree = db.open_tree(table)?;
        Ok(tree)
    }

    fn group_member_key(group_id: u64) -> String {
        format!("{}", group_id)
    }

    fn group_manager_key(group_id: u64) -> String {
        format!("manager-{}", group_id)
    }

    fn group_member_insert(db: Arc<Db>, group_id: u64, member_ids: Vec<u64>) -> ClientResult<()> {
        tracing::error!(
            "group_id: {}, member_ids: {:?}",
            group_id,
            member_ids.clone()
        );
        let key = &Self::group_member_key(group_id);
        let tree = Self::open_group_member_tree(db)?;
        let mut res = vec![];
        if let Some(data) = tree.get(key)? {
            let data = data.as_bytes();
            let mut members = Members::deserialize(data)?;
            let _: Vec<_> = member_ids
                .iter()
                .map(|member_id| {
                    members.members.insert(*member_id);
                })
                .collect();
            res = members.to_bytes()?;
        } else {
            res = Members::new(group_id, member_ids).to_bytes()?
        };
        tree.insert(key, res)?;
        tree.flush()?;
        Ok(())
    }

    fn group_members_ids(db: Arc<Db>, group_id: u64) -> ClientResult<Vec<u64>> {
        let key = Self::group_member_key(group_id);
        let tree = Self::open_group_member_tree(db.clone())?;
        if let Some(data) = tree.get(key)? {
            let members = Members::deserialize(data.as_bytes())?;
            let members: Vec<u64> = members.members.iter().map(|a| *a).collect();
            Ok(members)
        } else {
            Ok(vec![])
        }
    }

    fn group_members_get(
        db: Arc<Db>,
        group_id: u64,
        page_no: u64,
        page_size: u64,
    ) -> ClientResult<Vec<GroupMemberNickname>> {
        let key = Self::group_member_key(group_id);
        let tree = Self::open_group_member_tree(db.clone())?;
        let mut list = vec![];
        let contact_tree = Self::open_contact_tree(db)?;
        if let Some(data) = tree.get(key)? {
            let members = Members::deserialize(data.as_bytes())?;
            let members: Vec<u64> = members.members.iter().map(|a| *a).collect();
            let left = ((page_no - 1) * page_size) as usize;
            let mut right = (page_size * page_size) as usize;
            if left > members.len() {
                return Ok(vec![]);
            }
            if right > members.len() {
                right = members.len();
            }
            for member in members.get(left..right).unwrap() {
                let mut nickname = String::new();
                let key = Self::get_group_member_nickname_key(group_id, *member);
                if let Some(data) = contact_tree.get(key.as_bytes())? {
                    let data = data.to_vec();
                    nickname = String::from_utf8(data)?;
                } else {
                    let key = format!("TAG-{}", *member);
                    if let Some(data) = contact_tree.get(key.as_bytes())? {
                        let data = data.to_vec();
                        nickname = String::from_utf8(data)?;
                    }
                }
                list.push(GroupMemberNickname {
                    u_id: *member,
                    nickname,
                });
            }
        };
        Ok(list)
    }

    fn get_member_count(db: Arc<Db>, group_id: u64) -> ClientResult<u64> {
        let key = Self::group_member_key(group_id);
        let tree = Self::open_group_member_tree(db.clone())?;
        let count = if let Some(data) = tree.get(key)? {
            let members = Members::deserialize(data.as_bytes())?;
            members.members.len() as u64
        } else {
            0
        };
        Ok(count)
    }

    fn set_is_group_manager(db: Arc<Db>, group_id: u64, u_id: u64) -> ClientResult<()> {
        let key = &Self::group_manager_key(group_id);
        let tree = Self::open_group_member_tree(db.clone())?;
        let mut res: HashMap<u64, bool> = HashMap::new();
        if let Some(data) = tree.get(key)? {
            let data = data.as_bytes();
            res = serde_json::from_slice(data)?;
            res.insert(u_id, true);
        } else {
            res.insert(u_id, true);
        }

        let data = serde_json::to_string(&res)?;
        tree.insert(key, data.as_str())?;
        tree.flush()?;
        Ok(())
    }

    fn get_is_group_manager(db: Arc<Db>, group_id: u64, u_id: u64) -> ClientResult<bool> {
        let key = &Self::group_manager_key(group_id);
        let tree = Self::open_group_member_tree(db.clone())?;
        if let Some(data) = tree.get(key)? {
            let data = data.as_bytes();
            let res: HashMap<u64, bool> = serde_json::from_slice(data)?;
            Ok(*res.get(&u_id).unwrap_or(&false))
        } else {
            Ok(false)
        }
    }

    fn group_member_remove(db: Arc<Db>, group_id: u64, u_id: u64) -> ClientResult<()> {
        let key = &Self::group_member_key(group_id);
        let tree = Self::open_group_member_tree(db)?;
        let data = if let Some(data) = tree.get(key)? {
            let data = data.as_bytes();
            let mut members = Members::deserialize(data)?;
            members.members.remove(&u_id);

            members.to_bytes()?
        } else {
            return Ok(());
        };
        tree.insert(key, data)?;
        tree.flush()?;
        Ok(())
    }

    fn group_remove(db: Arc<Db>, group_id: u64) -> ClientResult<()> {
        let key = &Self::group_member_key(group_id);
        let tree = Self::open_group_member_tree(db)?;

        let _ = tree.remove(key)?;

        Ok(())
    }
}
