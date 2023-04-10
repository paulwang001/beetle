use crate::sled_db::contacts::ContactsDb;
use crate::sled_db::group_members::GroupMembersDb;
use crate::sled_db::mnemonic::Mnemonic;
use crate::sled_db::nickname::Nickname;
use crate::sled_db::session::SessionDb;

pub mod group_members;
pub mod contacts;
pub mod session;
pub mod mnemonic;
pub mod nickname;

pub trait SledDbAll: ContactsDb + SessionDb + GroupMembersDb + Mnemonic + Nickname{}


