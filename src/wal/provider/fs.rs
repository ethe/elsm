use std::{
    fs,
    fs::{DirEntry, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use async_stream::stream;
use futures::Stream;
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use tokio::fs::File;

use super::WalProvider;
use crate::wal::FileId;

static WAL_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}\.wal$").unwrap());

pub struct Fs {
    path: PathBuf,
}

impl Fs {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::create_dir_all(path.as_ref())?;
        Ok(Self {
            path: path.as_ref().to_owned(),
        })
    }
}

impl WalProvider for Fs {
    type File = File;

    async fn open(&self, fid: FileId) -> io::Result<Self::File> {
        Ok(OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(self.path.join(format!("{}.wal", fid)))?
            .into())
    }

    fn remove(&self, fid: FileId) -> io::Result<()> {
        let _ = fs::remove_file(self.path.join(format!("{}.wal", fid)));
        Ok(())
    }

    fn list(&self) -> io::Result<impl Stream<Item = io::Result<(Self::File, FileId)>>> {
        let mut entries: Vec<DirEntry> = fs::read_dir(&self.path)?.try_collect()?;
        entries.sort_by_key(|entry| entry.file_name());

        Ok(stream! {
            for entry in entries {
                let path = entry.path();
                if path.is_file() {
                    if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                        if WAL_REGEX.is_match(filename) {
                            // SAFETY: Checked on WAL_REGEX
                            let file_id = FileId::from_string(filename
                                .split('.')
                                .next()
                                .unwrap()).unwrap();
                            yield Ok((OpenOptions::new()
                                .create(true)
                                .write(true)
                                .read(true)
                                .open(self.path.join(filename))?.into(), file_id))
                        }
                    }
                }
            }
        })
    }
}
