/// CLI arguments support.
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short, long)]
    cache: Option<bool>,
    #[clap(long)]
    metrics: bool,
    #[clap(long)]
    tracing: bool,
    #[clap(long)]
    denylist: bool,
    /// Path to the store
    #[clap(long = "store-path")]
    pub store_path: Option<PathBuf>,
    #[clap(long)]
    pub cfg: Option<PathBuf>,
}

impl Args {
    pub fn make_overrides_map(&self) -> HashMap<&str, String> {
        let mut map: HashMap<&str, String> = HashMap::new();

        map.insert("metrics.collect", self.metrics.to_string());
        map.insert("metrics.tracing", self.tracing.to_string());
        if let Some(path) = self.store_path.clone() {
            map.insert("store.path", path.to_str().unwrap_or("").to_string());
        }
        map
    }
}
