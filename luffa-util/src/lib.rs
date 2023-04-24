use anyhow::{anyhow, Result};
use cid::{
    multihash::{Code, MultihashDigest},
    Cid,
};
use config::{Config, ConfigError, Environment, File, Map, Source, Value, ValueKind};
use std::{
    cell::RefCell,
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub mod exitcodes;
pub mod human;
pub mod lock;

const LUFFA_DIR: &str = "luffa";
#[cfg(unix)]
const DEFAULT_NOFILE_LIMIT: u64 = 65536;
#[cfg(unix)]
const MIN_NOFILE_LIMIT: u64 = 2048;

/// Blocks current thread until ctrl-c is received
pub async fn block_until_sigint() {
    let (ctrlc_send, ctrlc_oneshot) = futures::channel::oneshot::channel();
    let ctrlc_send_c = RefCell::new(Some(ctrlc_send));

    let running = Arc::new(AtomicUsize::new(0));
    ctrlc::set_handler(move || {
        let prev = running.fetch_add(1, Ordering::SeqCst);
        if prev == 0 {
            tracing::info!("Got interrupt, shutting down...");
            // Send sig int in channel to blocking task
            if let Some(ctrlc_send) = ctrlc_send_c.try_borrow_mut().unwrap().take() {
                ctrlc_send.send(()).expect("Error sending ctrl-c message");
            }
        } else {
            std::process::exit(0);
        }
    })
    .expect("Error setting Ctrl-C handler");

    ctrlc_oneshot.await.unwrap();
}

/// Returns the path to the user's luffa config directory.
///
/// If the `LUFFA_CONFIG_DIR` environment variable is set it will be used unconditionally.
/// Otherwise the returned value depends on the operating system according to the following
/// table.
///
/// | Platform | Value                                 | Example                          |
/// | -------- | ------------------------------------- | -------------------------------- |
/// | Linux    | `$XDG_CONFIG_HOME` or `$HOME`/.config/luffa | /home/alice/.config/luffa              |
/// | macOS    | `$HOME`/Library/Application Support/luffa   | /Users/Alice/Library/Application Support/luffa |
/// | Windows  | `{FOLDERID_RoamingAppData}`/luffa           | C:\Users\Alice\AppData\Roaming\luffa   |
pub fn luffa_config_root() -> Result<PathBuf> {
    if let Some(val) = env::var_os("LUFFA_CONFIG_DIR") {
        return Ok(PathBuf::from(val));
    }
    let cfg = dirs_next::config_dir().unwrap_or("/data/user/0/com.meta.luffa/files/config".into());
    Ok(cfg.join(LUFFA_DIR))
}

// Path that leads to a file in the luffa config directory.
pub fn luffa_config_path(file_name: &str) -> Result<PathBuf> {
    let path = luffa_config_root()?.join(file_name);
    Ok(path)
}

/// Returns the path to the user's luffa data directory.
///
/// If the `LUFFA_DATA_DIR` environment variable is set it will be used unconditionally.
/// Otherwise the returned value depends on the operating system according to the following
/// table.
///
/// | Platform | Value                                         | Example                                  |
/// | -------- | --------------------------------------------- | ---------------------------------------- |
/// | Linux    | `$XDG_DATA_HOME`/luffa or `$HOME`/.local/share/luffa | /home/alice/.local/share/luffa                 |
/// | macOS    | `$HOME`/Library/Application Support/luffa      | /Users/Alice/Library/Application Support/luffa |
/// | Windows  | `{FOLDERID_RoamingAppData}/luffa`              | C:\Users\Alice\AppData\Roaming\luffa           |
pub fn luffa_data_root() -> Result<PathBuf> {
    if let Some(val) = env::var_os("LUFFA_DATA_DIR") {
        return Ok(PathBuf::from(val));
    }
    let path = dirs_next::cache_dir().unwrap_or("/data/user/0/com.meta.luffa/files/data".into());
    Ok(path.join(LUFFA_DIR))
}

/// Path that leads to a file in the luffa data directory.
pub fn luffa_data_path(file_name: &str) -> Result<PathBuf> {
    let path = luffa_data_root()?.join(file_name);
    Ok(path)
}

/// Returns the path to the user's luffa cache directory.
///
/// If the `LUFFA_CACHE_DIR` environment variable is set it will be used unconditionally.
/// Otherwise the returned value depends on the operating system according to the following
/// table.
///
/// | Platform | Value                                         | Example                                  |
/// | -------- | --------------------------------------------- | ---------------------------------------- |
/// | Linux    | `$XDG_CACHE_HOME`/luffa or `$HOME`/.cache/luffa | /home/.cache/luffa                        |
/// | macOS    | `$HOME`/Library/Caches/luffa                   | /Users/Alice/Library/Caches/luffa         |
/// | Windows  | `{FOLDERID_LocalAppData}/luffa`                | C:\Users\Alice\AppData\Roaming\luffa      |
pub fn luffa_cache_root() -> Result<PathBuf> {
    if let Some(val) = env::var_os("LUFFA_CACHE_DIR") {
        return Ok(PathBuf::from(val));
    }
    let path = dirs_next::cache_dir().unwrap_or("/data/user/0/com.meta.luffa/cache".into());
    Ok(path.join(LUFFA_DIR))
}

/// Path that leads to a file in the luffa cache directory.
pub fn luffa_cache_path(file_name: &str) -> Result<PathBuf> {
    let path = luffa_cache_root()?.join(file_name);
    Ok(path)
}

/// insert a value into a `config::Map`
pub fn insert_into_config_map<I: Into<String>, V: Into<ValueKind>>(
    map: &mut Map<String, Value>,
    field: I,
    val: V,
) {
    map.insert(field.into(), Value::new(None, val));
}

// struct made to shoe-horn in the ability to use the `LUFFA_METRICS` env var prefix
#[derive(Debug, Clone)]
struct MetricsSource {
    metrics: Config,
}

impl Source for MetricsSource {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }
    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let metrics = self.metrics.collect()?;
        let mut map = Map::new();
        insert_into_config_map(&mut map, "metrics", metrics);
        Ok(map)
    }
}

/// Make a config using a default, files, environment variables, and commandline flags.
///
/// Later items in the *file_paths* slice will have a higher priority than earlier ones.
///
/// Environment variables are expected to start with the *env_prefix*. Nested fields can be
/// accessed using `.`, if your environment allows env vars with `.`
///
/// Note: For the metrics configuration env vars, it is recommended to use the metrics
/// specific prefix `LUFFA_METRICS` to set a field in the metrics config. You can use the
/// above dot notation to set a metrics field, eg, `LUFFA_CONFIG_METRICS.SERVICE_NAME`, but
/// only if your environment allows it
pub fn make_config<T, S, V>(
    default: T,
    file_paths: &[Option<&Path>],
    env_prefix: &str,
    flag_overrides: HashMap<S, V>,
) -> Result<T>
where
    T: serde::de::DeserializeOwned + Source + Send + Sync + 'static,
    S: AsRef<str>,
    V: Into<Value>,
{
    // create config builder and add default as first source
    let mut builder = Config::builder().add_source(default);

    // layer on config options from files
    for path in file_paths.iter().flatten() {
        if path.exists() {
            let p = path.to_str().ok_or_else(|| anyhow::anyhow!("empty path"))?;
            builder = builder.add_source(File::with_name(p));
        }
    }

    // next, add any environment variables
    builder = builder.add_source(
        Environment::with_prefix(env_prefix)
            .separator("__")
            .try_parsing(true),
    );

    // pull metrics config from env variables
    // nesting into this odd `MetricsSource` struct, gives us the option of
    // using the more convienient prefix `LUFFA_METRICS` to set metrics env vars
    let mut metrics = Config::builder().add_source(
        Environment::with_prefix("LUFFA_METRICS")
            .separator("__")
            .try_parsing(true),
    );

    // allow custom `LUFFA_INSTANCE_ID` env var
    if let Ok(instance_id) = env::var("LUFFA_INSTANCE_ID") {
        metrics = metrics.set_override("instance_id", instance_id)?;
    }
    // allow custom `LUFFA_ENV` env var
    if let Ok(service_env) = env::var("LUFFA_ENV") {
        metrics = metrics.set_override("service_env", service_env)?;
    }
    let metrics = metrics.build().unwrap();

    builder = builder.add_source(MetricsSource { metrics });

    // finally, override any values
    for (flag, val) in flag_overrides.into_iter() {
        builder = builder.set_override(flag, val)?;
    }

    let cfg = builder.build()?;
    println!("metrics make_config:\n{:#?}\n", cfg);
    let cfg: T = cfg.try_deserialize()?;
    Ok(cfg)
}

/// Verifies that the provided bytes hash to the given multihash.
pub fn verify_hash(cid: &Cid, bytes: &[u8]) -> Option<bool> {
    Code::try_from(cid.hash().code()).ok().map(|code| {
        let calculated_hash = code.digest(bytes);
        &calculated_hash == cid.hash()
    })
}

/// If supported sets a preffered limit for file descriptors.
#[cfg(unix)]
pub fn increase_fd_limit() -> std::io::Result<u64> {
    let (_, hard) = rlimit::Resource::NOFILE.get()?;
    let target = std::cmp::min(hard, DEFAULT_NOFILE_LIMIT);
    rlimit::Resource::NOFILE.set(target, hard)?;
    let (soft, _) = rlimit::Resource::NOFILE.get()?;
    if soft < MIN_NOFILE_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("NOFILE limit too low: {soft}"),
        ));
    }
    Ok(soft)
}
