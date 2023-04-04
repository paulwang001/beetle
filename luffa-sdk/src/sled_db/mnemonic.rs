use std::sync::Arc;
use image::EncodableLayout;
use sled::{Db, IVec, Tree};
use luffa_node::Keypair;
use crate::ClientError::CustomError;
use crate::ClientResult;
use crate::sled_db::global_db::GlobalDb;

pub trait Mnemonic: GlobalDb {
    fn open_mnemonic_tree() -> ClientResult<Tree> {
        Ok(Self::get_global_db().open_tree("bip39_keys")?)
    }

    fn mnemonic_key(name: &str) -> String {
        format!("phrase-{}", name)
    }

    fn keypair_key(name: &str) -> String {
        format!("pair-{}", name)
    }

    fn save_mnemonic(name: &str, mnemonic: &str) -> ClientResult<()> {
        let mut tree = Self::open_mnemonic_tree()?;
        let key = Self::mnemonic_key(name);
        tree.insert(key, mnemonic)?;
        tree.flush()?;
        Ok(())
    }

    fn save_mnemonic_keypair(mnemonic: &str, keypair: Keypair) -> ClientResult<()> {
        let name = &keypair.name();
        if let Keypair::Ed25519(v) = keypair {
            let data = v.to_bytes();
            let mut tree = Self::open_mnemonic_tree()?;
            let mnemonic_key = Self::mnemonic_key(name);
            let keypair_key = Self::keypair_key(name);
            tree.insert(mnemonic_key, mnemonic)?;
            tree.insert(keypair_key, data.to_vec())?;
            tree.flush()?;
        }
        Ok(())
    }

    fn get_mnemonic(name: &str) -> ClientResult<Option<String>> {
        let mut tree = Self::open_mnemonic_tree()?;
        let key = Self::mnemonic_key(name);
        let data = if let Some(data) = tree.get(key)? {
            Some(String::from_utf8(data.to_vec())?)
        } else {
            None
        };
        Ok(data)
    }

    fn get_mnemonic_vec(name: &str) -> ClientResult<Option<IVec>> {
        let mut tree = Self::open_mnemonic_tree()?;
        let key = Self::mnemonic_key(name);
        Ok(tree.get(key)?)
    }

    fn get_mnemonic_keypair(name: &str) -> ClientResult<Option<IVec>> {
        let mut tree = Self::open_mnemonic_tree()?;
        let key = Self::keypair_key(name);
        let data = tree.get(key)?;
        Ok(data)
    }

    fn remove_mnemonic_keypair(name: &str) -> ClientResult<Option<IVec>> {
        let mut tree = Self::open_mnemonic_tree()?;
        let key = Self::keypair_key(name);
        let data = tree.remove(key)?;
        Ok(data)
    }
}
