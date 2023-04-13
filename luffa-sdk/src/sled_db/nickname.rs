use crate::sled_db::contacts::ContactsDb;
use crate::ClientResult;
use image::EncodableLayout;
use luffa_rpc_types::Member;
use sled::Db;
use std::sync::Arc;

pub trait Nickname: ContactsDb {
    fn set_contacts_nickname(db: Arc<Db>, u_id: u64, nickname: &str) {
        Self::set_contacts_tag(db, u_id, nickname.to_string());
    }

    fn set_group_member_nickname(
        db: Arc<Db>,
        group_id: u64,
        u_id: u64,
        nickname: &str,
    ) -> ClientResult<()> {
        let tree = Self::open_contact_tree(db)?;
        let tag_key = Self::get_group_member_nickname_key(group_id, u_id);
        println!("set group member nickname:{tag_key} ==> {}", nickname);
        tree.insert(tag_key.as_bytes(), nickname)?;
        tree.flush()?;
        Ok(())
    }

    fn get_contacts_nickname(db: Arc<Db>, u_id: u64) -> ClientResult<Option<(String, u8)>> {
        Ok(Self::get_contacts_tag(db, u_id))
    }

    fn get_group_member_nickname_key(group_id: u64, u_id: u64) -> String {
        format!("GROUP-{}-{}", group_id, u_id)
    }

    fn get_group_member_nickname(db: Arc<Db>, group_id: u64, u_id: u64) -> ClientResult<String> {
        let tree = Self::open_contact_tree(db.clone())?;
        let tag_key = Self::get_group_member_nickname_key(group_id, u_id);
        if let Some(data) = tree.get(tag_key.as_bytes())? {
            let data = data.as_bytes();
            let nickname = String::from_utf8(data.to_vec())?;
            Ok(nickname)
        } else {
            let (nickname, _) = Self::get_contacts_nickname(db, u_id)?.unwrap();
            Ok(nickname)
        }
    }

    fn set_group_members_nickname(
        db: Arc<Db>,
        group_id: u64,
        members: Vec<Member>,
    ) -> ClientResult<()> {
        let tree = Self::open_contact_tree(db)?;
        for member in members {
            let tag_key = Self::get_group_member_nickname_key(group_id, member.u_id);
            tree.insert(tag_key.as_bytes(), member.group_nickname.as_bytes())?;
        }

        tree.flush()?;
        Ok(())
    }
}
