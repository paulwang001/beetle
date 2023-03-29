use std::sync::Arc;
use async_trait::async_trait;
use image::EncodableLayout;
use sled::Db;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::ser::Serializer;
use rkyv::ser::serializers::AllocSerializer;
use bytecheck::CheckBytes;
use crate::ClientError::CustomError;

use crate::ClientResult;

const KVDB_GROUP_MEMBERS_TREE: &str = "luffa_group_members";


#[derive(Debug, Default, Archive, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct Members{
    pub group_id: u64,
    pub members: Vec<u64>,
}

impl Members {
    pub fn new(group_id: u64, members: Vec<u64>) -> Self {
        Self{ group_id, members}
    }

    pub fn to_bytes(&self) -> ClientResult<Vec<u8>> {
        let mut serializer = AllocSerializer::<0>::default();
        if let Some(_) = serializer.serialize_value(self).ok() {
            Ok(serializer.into_serializer().into_inner().into_vec())
        } else {
            Err(CustomError("Members to_bytes fail".to_string()))
        }
    }

    pub fn deserialize(data: &[u8]) -> ClientResult<Self> {
        if let Some(data) = rkyv::check_archived_root::<Self>(data).ok() {
            Ok(Self{ group_id: data.group_id, members: data.members.to_vec() })
        } else {
            Err(CustomError("Members deserialize fail".to_string()))
        }
    }
}


pub trait GroupMembers {
    fn group_member_insert(db: Arc<Db>, group_id: u64, member_ids: Vec<u64>) -> ClientResult<()> {
        let key = &format!("{}-{}", KVDB_GROUP_MEMBERS_TREE, group_id);
        let tree = db.open_tree(KVDB_GROUP_MEMBERS_TREE)?;
        let data = if let Some(data) = tree.get(key)? {
            let data = data.as_bytes();
            let mut members = Members::deserialize(data)?;
            let _ = member_ids.iter().map(|member_id| {
                members.members.push(*member_id);
            });
            members.to_bytes()?
        } else {
            Members::new(group_id, member_ids).to_bytes()?
        };
        tree.insert(key, data)?;
        Ok(())
    }

    fn group_member_get(db: Arc<Db>, group_id: u64) -> ClientResult<Members> {
        let key = format!("{}-{}", KVDB_GROUP_MEMBERS_TREE, group_id);
        let tree = db.open_tree(KVDB_GROUP_MEMBERS_TREE)?;
        let members = if let Some(data) = tree.get(key)? {
            Members::deserialize(data.as_bytes())?
        } else {
            Members::new(group_id, vec![])
        };
        Ok(members)
    }
}