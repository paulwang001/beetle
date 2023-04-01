use std::path::PathBuf;
use std::sync::Arc;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

pub static LOCAL_ID: Lazy<Arc<RwLock<u64>>> = Lazy::new(||{
    Arc::new(RwLock::new(0))
});

pub static KVDB_CONTACTS_FILE: Lazy<Arc<RwLock<String>>> =Lazy::new(||{
    Arc::new(RwLock::new(String::from("contacts")))
});

pub static LUFFA_CONTENT:Lazy<Arc<RwLock<String>>> = Lazy::new(||{
    Arc::new(RwLock::new(String::from("index_data")))
});

pub fn write_local_id(local_id: u64) {
    let mut w = LOCAL_ID.write();
    *w = local_id;
    let mut contacts = KVDB_CONTACTS_FILE.write();
    *contacts = format!("contacts-{local_id}");

    let mut content = LUFFA_CONTENT.write();
    *content = format!("index_data-{local_id}");
}

pub fn read_local_id() -> u64 {
    LOCAL_ID.read().clone()
}