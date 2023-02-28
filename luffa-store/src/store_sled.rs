use std::{fmt, sync::Arc};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use cid::Cid;

use sled::Tree;
use smallvec::SmallVec;

use crate::Config;

#[derive(Clone)]
pub struct Store {
    blocks: Arc<Tree>,
    links: Arc<Tree>,
    block_sizes: Arc<Tree>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InnerStore")
            .field("blocks", &self.blocks)
            .field("links", &self.links)
            .field("block_sizes", &self.block_sizes)
            .finish()
    }
}

fn id_key(cid: &Cid) -> SmallVec<[u8; 64]> {
    let mut key = SmallVec::new();
    cid.hash().write(&mut key).unwrap();
    key.extend_from_slice(&cid.codec().to_be_bytes());
    key
}

impl Store {
    /// Creates a new database.
    #[tracing::instrument]
    pub async fn open(config: Config) -> Result<Self> {
        let path = config.path.clone();
        let db = sled::open(path)?;
        let blocks = Arc::new(db.open_tree("BLOCKS")?);
        let block_sizes = Arc::new(db.open_tree("BLOCK_SIZES")?);
        let links = Arc::new(db.open_tree("LINKS")?);
        Ok(Store {
            blocks,
            links,
            block_sizes,
        })
    }

    #[tracing::instrument(skip(self, links, blob))]
    pub fn put<T: AsRef<[u8]>, L>(&self, cid: Cid, blob: T, links: L) -> Result<()>
    where
        L: IntoIterator<Item = Cid>,
    {
        let key = id_key(&cid);
        let links_t = self.links.clone();
        let blocks_t = self.blocks.clone();
        let links = links.into_iter().map(|c| c.to_bytes()).collect::<Vec<_>>();
        if !links.is_empty() {
            let links = serde_cbor::to_vec(&links)?;
            links_t.insert(key.clone(), links)?;
            links_t.flush()?;
        }
        let size = blob.as_ref().len() as u64;
        self.block_sizes
            .insert(key.clone(), size.to_be_bytes().to_vec())?;
        self.block_sizes.flush()?;
        blocks_t.insert(key, blob.as_ref())?;
        blocks_t.flush().map_err(|e| anyhow!("{e:?}"))?;
        Ok(())
    }

    #[tracing::instrument(skip(self, blocks))]
    pub fn put_many(&self, blocks: impl IntoIterator<Item = (Cid, Bytes, Vec<Cid>)>) -> Result<()> {
        let links_t = self.links.clone();
        let blocks_t = self.blocks.clone();
        let mut links_count = 0_usize;
        for (cid, blob, links) in blocks {
            let key = id_key(&cid);
            let links = links.into_iter().map(|c| c.to_bytes()).collect::<Vec<_>>();
            if !links.is_empty() {
                links_count += links.len();
                let links = serde_cbor::to_vec(&links)?;
                links_t.insert(key.clone(), links)?;
            }
            blocks_t.insert(key.clone(), blob.as_ref())?;
            let size = blob.as_ref().len() as u64;
            self.block_sizes.insert(key, size.to_be_bytes().to_vec())?;
            self.block_sizes.flush()?;
        }
        if links_count > 0 {
            links_t.flush()?;
        }
        self.block_sizes.flush()?;
        blocks_t.flush().map_err(|e| anyhow!("{e:?}"))?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub fn get(&self, cid: &Cid) -> Result<Option<Bytes>> {
        let key = id_key(&cid);
        let val = self.blocks.get(&key)?;
        Ok(val.map(|v| Bytes::from(v.to_vec())))
    }

    #[tracing::instrument(skip(self))]
    pub fn get_size(&self, cid: &Cid) -> Result<Option<usize>> {
        let key = id_key(&cid);
        let size = self.block_sizes.get(key)?;
        Ok(size.map(|v| {
            let mut val = [0u8; 8];
            val.clone_from_slice(&v[..8]);
            u64::from_be_bytes(val) as usize
        }))
    }

    #[tracing::instrument(skip(self))]
    pub fn has(&self, cid: &Cid) -> Result<bool> {
        let key = id_key(&cid);
        self.block_sizes
            .contains_key(key)
            .map_err(|e| anyhow!("{e:?}"))
    }
    #[tracing::instrument(skip(self))]
    pub fn get_links(&self, cid: &Cid) -> Result<Option<Vec<Cid>>> {
        let key = id_key(cid);

        let val = self.links.get(&key)?;
        Ok(val.map(|v| {
            let links: Vec<Vec<u8>> = serde_cbor::from_slice(&v).unwrap();
            links
                .into_iter()
                .map(|c| Cid::try_from(&c[..]).unwrap())
                .collect::<Vec<_>>()
        }))
    }

    pub(crate) async fn spawn_blocking<T: Send + Sync + 'static>(
        &self,
        f: impl FnOnce(Self) -> anyhow::Result<T> + Send + Sync + 'static,
    ) -> anyhow::Result<T> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || f(this)).await?
    }
}
