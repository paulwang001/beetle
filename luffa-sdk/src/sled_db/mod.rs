use crate::sled_db::contacts::ContactsDb;
use crate::sled_db::group_members::GroupMembersDb;
use crate::sled_db::mnemonic::Mnemonic;
use crate::sled_db::nickname::Nickname;
use crate::sled_db::session::SessionDb;

pub mod contacts;
pub mod group_members;
pub mod mnemonic;
pub mod nickname;
pub mod process_event;
pub mod run;
pub mod session;

pub trait SledDbAll: ContactsDb + SessionDb + GroupMembersDb + Mnemonic + Nickname {}
