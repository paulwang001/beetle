use crate::sled_db::contacts::ContactsDb;
use crate::sled_db::group_members::GroupMembersDb;
use crate::sled_db::session::SessionDb;

pub mod group_members;
pub mod contacts;
pub mod session;
pub mod local_config;

pub trait SledDb: ContactsDb + SessionDb + GroupMembersDb{}