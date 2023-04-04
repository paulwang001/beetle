use std::fs;
use std::sync::Arc;
use sled::Db;
use tantivy::{Index, IndexWriter, TantivyError};
use tantivy::schema::Schema;
use tokio::sync::RwLock;
use tracing::{info, warn};
use crate::{content_index, KVDB_CONTACTS_FILE, LUFFA_CONTENT};

pub struct SledDb {
    pub db: Arc<Db>,
    pub idx: Arc<Index>,
    pub schema: Schema,
    pub writer: Arc<RwLock<IndexWriter>>,
}

impl SledDb {
    pub fn new(local_id: u64) -> Self {
        let path = luffa_util::luffa_data_path(&format!("{}/{}", KVDB_CONTACTS_FILE, local_id)).unwrap();
        let idx_path = luffa_util::luffa_data_path(&format!("{}/{}", LUFFA_CONTENT, local_id)).unwrap();
        info!("path >>>> {:?}", &path);
        info!("idx_path >>>> {:?}", &idx_path);

        let db = Arc::new(sled::open(path).expect("open db failed"));
        let (idx, schema) = content_index(idx_path.as_path());
        let writer = idx.writer_with_num_threads(1, 12 * 1024 * 1024).or_else(|e| {
            match e {
                TantivyError::LockFailure(_, _) => {
                    // 清除文件锁，再次尝试打开 indexWriter
                    warn!("first open indexWriter failed because of acquire file lock failed");
                    fs::remove_file(&idx_path.join(".tantivy-writer.lock")).expect("release indexWriter lock failed");

                    idx.writer_with_num_threads(1, 12 * 1024 * 1024)
                }
                _ => panic!("acquire indexWriter failed: {:?}", e)
            }
        })
            .expect("acquire indexWriter failed");

        Self {
            db,
            idx: Arc::new(idx),
            schema,
            writer: Arc::new(RwLock::new(writer)),
        }
    }


}


pub trait SledDbTrait {
    fn get_db(sled_db: Arc<parking_lot::RwLock<Option<SledDb>>>) -> Arc<Db> {
        sled_db.read().as_ref().unwrap().db.clone()
    }


    fn get_idx(sled_db: Arc<parking_lot::RwLock<Option<SledDb>>>) -> Arc<Index> {
        sled_db.read().as_ref().unwrap().idx.clone()
    }

    fn get_writer(sled_db: Arc<parking_lot::RwLock<Option<SledDb>>>) -> Arc<RwLock<IndexWriter>> {
        sled_db.read().as_ref().unwrap().writer.clone()
    }

    fn get_schema(sled_db: Arc<parking_lot::RwLock<Option<SledDb>>>) -> Schema {
        sled_db.read().as_ref().unwrap().schema.clone()
    }
}