use crate::sled_db::contacts::ContactsDb;
use crate::sled_db::group_members::GroupMembersDb;
use crate::sled_db::mnemonic::Mnemonic;
use crate::sled_db::session::SessionDb;
use crate::sled_db::global_db::GlobalDb;

pub mod group_members;
pub mod contacts;
pub mod session;
pub mod mnemonic;
pub mod global_db;

pub trait SledDbAll: ContactsDb + SessionDb + GroupMembersDb + Mnemonic{}


