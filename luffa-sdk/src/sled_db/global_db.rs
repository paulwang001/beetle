use std::sync::Arc;
use sled::Db;

const GLOBAL_DB_FILE: &str = "global_db";


pub trait GlobalDb {
    fn get_global_db() -> Arc<Db> {
        let path = luffa_util::luffa_data_path(GLOBAL_DB_FILE).unwrap();
        Arc::new(sled::open(path).expect("open db failed"))
    }
}