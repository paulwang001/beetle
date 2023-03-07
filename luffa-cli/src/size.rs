use std::path::{Path, PathBuf};

use async_stream::stream;
use futures::stream::Stream;
use tokio::fs;

#[derive(Debug, PartialEq, Eq)]
pub struct FileInfo {
    pub path: PathBuf,
    pub size: u64,
}

pub fn size_stream(path: &Path) -> impl Stream<Item = FileInfo> + '_ {
    stream! {
        let mut stack = vec![path.to_path_buf()];
        while let Some(path) = stack.pop() {
            if path.is_dir() {
                let mut read_dir = fs::read_dir(&path).await.unwrap();
                while let Some(entry) = read_dir.next_entry().await.unwrap() {
                    stack.push(entry.path());
                }
            } else if path.is_symlink() {
                continue;
            } else {
                let size = fs::metadata(&path).await.unwrap().len();
                let path = path.clone();
                yield FileInfo { path, size };
            }
        }
    }
}
