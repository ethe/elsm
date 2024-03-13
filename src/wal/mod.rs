mod checksum;

use std::{future::Future, hash::Hasher, io, marker::PhantomData};

use async_lock::Mutex;
use async_stream::stream;
use checksum::{HashReader, HashWriter};
use futures::{
    io::{BufReader, BufWriter},
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, Stream,
};
use thiserror::Error;

use crate::{
    record::Record,
    serdes::{Decode, Encode},
};

pub trait WalWrite<K, V, T> {
    type Error: std::error::Error + 'static;

    fn write(&self, record: &Record<&K, &V, &T>) -> impl Future<Output = Result<(), Self::Error>>;

    fn flush(&self) -> impl Future<Output = io::Result<()>>;
}

pub trait WalRecover<K, V, T> {
    type Error: std::error::Error + 'static;

    fn recover(&self) -> impl Stream<Item = Result<Record<K, V, T>, Self::Error>>;
}

#[derive(Debug)]
pub(crate) struct WalFile<H, F, K, V, T> {
    file: Mutex<BufWriter<F>>,
    hasher: fn() -> H,
    _marker: PhantomData<(K, V, T)>,
}

impl<H, F, K, V, T> WalFile<H, F, K, V, T>
where
    F: AsyncWrite,
{
    pub(crate) fn new(file: F, hasher: fn() -> H) -> Self {
        Self {
            file: Mutex::new(BufWriter::new(file)),
            hasher,
            _marker: PhantomData,
        }
    }
}

impl<H, F, K, V, T> WalWrite<K, V, T> for WalFile<H, F, K, V, T>
where
    H: Hasher,
    F: AsyncWrite + Unpin,
    K: Encode,
    V: Encode,
    T: Encode,
{
    type Error = WriteError<<Record<K, V, T> as Encode>::Error>;

    async fn write(&self, record: &Record<&K, &V, &T>) -> Result<(), Self::Error> {
        let mut file = self.file.lock().await;
        let mut writer = HashWriter::new((self.hasher)(), &mut *file);
        record.encode(&mut writer).await?;
        writer.eol().await.map_err(WriteError::Io)?;
        Ok(())
    }

    async fn flush(&self) -> io::Result<()> {
        self.file.lock().await.flush().await
    }
}

impl<H, F, K, V, T> WalRecover<K, V, T> for WalFile<H, F, K, V, T>
where
    H: Hasher,
    F: AsyncRead + AsyncWrite + Unpin,
    K: Decode,
    V: Decode,
    T: Decode,
{
    type Error = RecoverError<<Record<K, V, T> as Decode>::Error>;

    fn recover(&self) -> impl Stream<Item = Result<Record<K, V, T>, Self::Error>> {
        stream! {
            let mut file = self.file.lock().await;
            let mut file = BufReader::new(file.get_mut());

            loop {
                if file.buffer().is_empty() && file.fill_buf().await.map_err(RecoverError::Io)?.is_empty() {
                    return;
                }

                let mut reader = HashReader::new((self.hasher)(), &mut file);

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
pub(crate) enum WriteError<E: std::error::Error> {
    #[error("wal write encode error: {0}")]
    Encode(#[from] E),
    #[error("wal write io error")]
    Io(#[source] std::io::Error),
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
    use std::pin::pin;

    use futures::{executor::block_on, io::Cursor, StreamExt};

    use super::{Record, WalFile, WalRecover, WalWrite};
    use crate::record::RecordType;

    #[test]
    fn write_and_recover() {
        let mut file = Vec::new();
        block_on(async {
            {
                let wal = WalFile::new(Cursor::new(&mut file), crc32fast::Hasher::new);
                wal.write(&Record::new(
                    RecordType::Full,
                    &"key".to_string(),
                    &0_u64,
                    Some(&"value".to_string()),
                ))
                .await
                .unwrap();
                wal.flush().await.unwrap();
            }
            {
                let wal = WalFile::new(Cursor::new(&mut file), crc32fast::Hasher::new);

                {
                    let mut stream = pin!(wal.recover());
                    assert_eq!(
                        stream.next().await.unwrap().unwrap().value,
                        Some("value".to_string())
                    );
                }

                wal.write(&Record::new(
                    RecordType::Full,
                    &"key".to_string(),
                    &0_u64,
                    Some(&"value".to_string()),
                ))
                .await
                .unwrap();
                wal.flush().await.unwrap();
            }

            {
                let wal = WalFile::new(Cursor::new(&mut file), crc32fast::Hasher::new);

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
