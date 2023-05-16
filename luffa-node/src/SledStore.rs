use std::{iter, collections::{hash_map, hash_set}, borrow::Cow, path::{Path, PathBuf}};

use libp2p::{kad::{store::RecordStore, ProviderRecord, Record}, PeerId};
use luffa_rpc_types::p2p::Key;

pub struct SledStore {
    /// The identity of the peer owning the store.
    local_key: Option<PeerId>,
    config: SledStoreConfig,
    db:sled::Db,
}

/// Configuration for a `MemoryStore`.
#[derive(Debug, Clone)]
pub struct SledStoreConfig {
    /// The maximum number of records.
    pub max_records: usize,
    /// The maximum size of record values, in bytes.
    pub max_value_bytes: usize,
    /// The maximum number of providers stored for a key.
    ///
    /// This should match up with the chosen replication factor.
    pub max_providers_per_key: usize,
    /// The maximum number of provider records for which the
    /// local node is the provider.
    pub max_provided_keys: usize,

    pub db_path:PathBuf,
}

impl Default for SledStoreConfig {
    fn default() -> Self {
        let db_path = PathBuf::from("./store");
        Self {
            max_records: 1024,
            max_value_bytes: 65 * 1024,
            max_provided_keys: 1024,
            max_providers_per_key: 1024,
            db_path,
        }
    }
}

pub struct InnerRecord{

}

// impl Iterator<Item=u64> for InnerRecord {
    
// }

impl SledStore {
    pub fn new(local_id: PeerId) -> Self {
        Self::with_config(local_id, Default::default())
    }

    /// Creates a new `MemoryRecordStore` with the given configuration.
    pub fn with_config(local_id: PeerId, config: SledStoreConfig) -> Self {
        std::fs::create_dir_all(config.db_path.as_path()).unwrap();
        let db = sled::open(config.db_path.as_path()).unwrap();
        SledStore {
            local_key: Some(local_id),
            config,
            db,
        }
    }
}


impl RecordStore for SledStore {
    type RecordsIter<'a> = 
    iter::Map<hash_map::Values<'a, Key, Record>, fn(&'a Record) -> Cow<'a, Record>>
    where
        Self: 'a;

    type ProvidedIter<'a> = iter::Map<
    hash_set::Iter<'a, ProviderRecord>,
    fn(&'a ProviderRecord) -> Cow<'a, ProviderRecord>,>
    where
        Self: 'a;
        

    fn get(&self, key: &libp2p::kad::RecordKey) -> Option<std::borrow::Cow<'_, libp2p::kad::Record>> {
        match self.db.get(key) {
            Ok(Some(val))=>{
                let val = Record::new(key.clone(), val.to_vec());
                Some(Cow::Owned(val))
            }
            Ok(None)=> None,
            Err(_e)=>{
                None
            }
        }
    }

    fn put(&mut self, r: libp2p::kad::Record) -> libp2p::kad::store::Result<()> {
        
        // self.db.fetch_and_update(r.key.clone(), |x|{
            
        // });
        self.db.insert(r.key.to_vec(), r.value.to_vec()).unwrap();
        self.db.flush().unwrap();
        Ok(())
    }
    
    fn remove(&mut self, k: &libp2p::kad::RecordKey) {
        self.db.remove(&k.to_vec()).unwrap();
        self.db.flush().unwrap();
        
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        // self.db.iter().map(|x|{
        //     let (k,v) = x.unwrap();
        //     let key = libp2p::kad::RecordKey::new(&k.to_vec());
        //     let val = Record::new(key,v.to_vec());
        //     let mahash_map::HashMap::new();
        // }).map(Cow::Owned)
        todo!()
    }

    fn add_provider(
        &mut self,
        record: libp2p::kad::ProviderRecord,
    ) -> libp2p::kad::store::Result<()> {
        todo!()
    }

    fn providers(&self, key: &libp2p::kad::RecordKey) -> Vec<libp2p::kad::ProviderRecord> {
        todo!()
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        todo!()
    }

    fn remove_provider(&mut self, k: &libp2p::kad::RecordKey, p: &PeerId) {
        todo!()
    }
}
