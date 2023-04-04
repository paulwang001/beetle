use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::sync::Arc;


pub static KVDB_CONTACTS_FILE: Lazy<Arc<RwLock<String>>> =Lazy::new(||{
    Arc::new(RwLock::new(String::from("contacts")))
});

pub static LUFFA_CONTENT:Lazy<Arc<RwLock<String>>> = Lazy::new(||{
    Arc::new(RwLock::new(String::from("index_data")))
});
