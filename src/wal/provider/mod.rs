pub mod fs;
pub mod in_mem;

use std::{future::Future, io};

use futures::Stream;

use crate::wal::FileId;

pub trait WalProvider: Send + Sync + 'static {
    type File: Unpin + Send + Sync + 'static;

    fn open(&self, fid: FileId) -> impl Future<Output = io::Result<Self::File>>;

    // FIXME: async
    fn remove(&self, fid: FileId) -> io::Result<()>;

    fn list(&self) -> io::Result<impl Stream<Item = io::Result<(Self::File, FileId)>>>;
}
