mod checksum;
pub mod provider;

use std::{error::Error, future::Future, io, marker::PhantomData};

use async_stream::stream;
use checksum::{HashReader, HashWriter};
use futures::Stream;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use ulid::Ulid;

use self::provider::WalProvider;
use crate::{
    record::Record,
    serdes::{Decode, Encode},
};

pub(crate) type FileId = Ulid;

#[derive(Debug)]
pub(crate) struct WalManager<WP> {
    pub(crate) wal_provider: WP,
}

impl<WP> WalManager<WP>
where
    WP: WalProvider,
{
    pub(crate) fn new(wal_provider: WP) -> Self {
        Self { wal_provider }
    }

    pub(crate) async fn create_wal_file<K, V>(&self) -> io::Result<WalFile<WP::File, K, V>> {
        let file_id = Ulid::new();
        let file = self.wal_provider.open(file_id).await?;

        self.pack_wal_file(file, file_id).await
    }

    pub(crate) fn remove_wal_file(&self, file_id: FileId) -> io::Result<()> {
        self.wal_provider.remove(file_id)
    }

    pub(crate) async fn pack_wal_file<K, V>(
        &self,
        file: WP::File,
        file_id: FileId,
    ) -> io::Result<WalFile<WP::File, K, V>> {
        Ok(WalFile::new(file, file_id))
    }
}

pub trait WalWrite<K, V>
where
    K: Encode,
    V: Encode,
{
    fn write(
        &mut self,
        record: Record<&K, &V>,
    ) -> impl Future<Output = Result<(), WriteError<<Record<&K, &V> as Encode>::Error>>>;

    fn flush(&mut self) -> impl Future<Output = io::Result<()>>;
}

pub trait WalRecover<K, V> {
    type Error: std::error::Error + Send + Sync + 'static;

    fn recover(&mut self) -> impl Stream<Item = Result<Record<K, V>, Self::Error>>;
}

#[derive(Debug)]
pub(crate) struct WalFile<F, K, V> {
    file: F,
    file_id: FileId,
    _marker: PhantomData<(K, V)>,
}

impl<F, K, V> WalFile<F, K, V> {
    pub(crate) fn new(file: F, file_id: FileId) -> Self {
        Self {
            file,
            file_id,
            _marker: PhantomData,
        }
    }

    pub(crate) fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl<F, K, V> WalWrite<K, V> for WalFile<F, K, V>
where
    F: AsyncWrite + Unpin + Send + Sync,
    K: Encode,
    V: Encode,
{
    async fn write(
        &mut self,
        record: Record<&K, &V>,
    ) -> Result<(), WriteError<<Record<&K, &V> as Encode>::Error>> {
        let mut writer = HashWriter::new(&mut self.file);
        record.encode(&mut writer).await?;
        writer.eol().await.map_err(WriteError::Io)?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.file.flush().await
    }
}

impl<F, K, V> WalRecover<K, V> for WalFile<F, K, V>
where
    F: AsyncRead + Unpin,
    K: Decode,
    V: Decode,
{
    type Error = RecoverError<<Record<K, V> as Decode>::Error>;

    fn recover(&mut self) -> impl Stream<Item = Result<Record<K, V>, Self::Error>> {
        stream! {
            // Safety: https://github.com/rust-lang/futures-rs/pull/2848 fix this, waiting for release
            let mut file = BufReader::new(&mut self.file);

            loop {
                if file.buffer().is_empty() && file.fill_buf().await.map_err(RecoverError::Io)?.is_empty() {
                    return;
                }

                let mut reader = HashReader::new(&mut file);

                let record = Record::decode(&mut reader).await?;

                if !reader.checksum().await.map_err(RecoverError::Io)? {
                    yield Err(RecoverError::Checksum);
                    return;
                }

                yield Ok(record);
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum WriteError<E: std::error::Error> {
    #[error("wal write encode error: {0}")]
    Encode(#[from] E),
    #[error("wal write io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("wal write max size exceeded")]
    MaxSizeExceeded,
    #[error("wal write arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("wal write internal error: {0}")]
    Internal(#[source] Box<dyn Error + Send + Sync + 'static>),
}

#[derive(Debug, Error)]
pub(crate) enum RecoverError<E: std::error::Error> {
    #[error("wal recover decode error: {0}")]
    Decode(#[from] E),
    #[error("wal recover checksum error")]
    Checksum,
    #[error("wal recover io error")]
    Io(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, pin::pin};

    use futures::{executor::block_on, StreamExt};

    use super::{FileId, Record, WalFile, WalRecover, WalWrite};
    use crate::record::RecordType;

    #[test]
    fn write_and_recover() {
        let mut file = Vec::new();
        block_on(async {
            {
                let mut wal = WalFile::new(Cursor::new(&mut file), FileId::new());
                wal.write(Record::new(
                    RecordType::Full,
                    &"key".to_string(),
                    0_u64,
                    Some(&"value".to_string()),
                ))
                .await
                .unwrap();
                wal.flush().await.unwrap();
            }
            {
                let mut wal = WalFile::new(Cursor::new(&mut file), FileId::new());

                {
                    let mut stream = pin!(wal.recover());
                    assert_eq!(
                        stream.next().await.unwrap().unwrap().value,
                        Some("value".to_string())
                    );
                }

                wal.write(Record::new(
                    RecordType::Full,
                    &"key".to_string(),
                    0_u64,
                    Some(&"value".to_string()),
                ))
                .await
                .unwrap();
                wal.flush().await.unwrap();
            }

            {
                let mut wal = WalFile::new(Cursor::new(&mut file), FileId::new());

                {
                    let mut stream = pin!(wal.recover());
                    let record: Record<String, _> = stream.next().await.unwrap().unwrap();
                    assert_eq!(record.key, "key".to_string());
                    assert_eq!(record.value, Some("value".to_string()));
                    let record: Record<String, _> = stream.next().await.unwrap().unwrap();
                    assert_eq!(record.key, "key".to_string());
                    assert_eq!(record.value, Some("value".to_string()));
                }
            }
        });
    }
}
