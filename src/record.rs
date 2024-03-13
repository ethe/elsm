use std::io;

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use thiserror::Error;

use crate::serdes::{Decode, Encode};

#[derive(Debug)]
pub struct Record<K, V, T = u64> {
    pub(crate) record_type: RecordType,
    pub(crate) key: K,
    pub(crate) ts: T,
    pub(crate) value: Option<V>,
}

impl<K, V, T> Record<K, V, T> {
    pub fn new(record_type: RecordType, key: K, ts: T, value: Option<V>) -> Self {
        Self {
            record_type,
            key,
            ts,
            value,
        }
    }

    pub fn as_ref(&self) -> Record<&K, &V, &T> {
        Record::new(self.record_type, &self.key, &self.ts, self.value.as_ref())
    }
}

impl<K, V, T> Encode for Record<K, V, T>
where
    K: Encode,
    V: Encode,
    T: Encode,
{
    type Error = EncodeError<K::Error, T::Error, <Option<V> as Encode>::Error>;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&[self.record_type as u8]).await?;
        self.key.encode(writer).await.map_err(EncodeError::Key)?;
        self.ts
            .encode(writer)
            .await
            .map_err(EncodeError::Timsetamp)?;
        self.value.encode(writer).await.map_err(EncodeError::Value)
    }
}

impl<K, V, T> Decode for Record<K, V, T>
where
    K: Decode,
    V: Decode,
    T: Decode,
{
    type Error = DecodeError<K::Error, T::Error, <Option<V> as Decode>::Error>;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut record_type = [0];
        reader.read_exact(&mut record_type).await?;
        let record_type = RecordType::from(record_type[0]);

        let key = K::decode(reader).await.map_err(DecodeError::Key)?;
        let ts = T::decode(reader).await.map_err(DecodeError::Timetamp)?;
        let value = Option::decode(reader).await.map_err(DecodeError::Value)?;

        Ok(Self {
            key,
            ts,
            value,
            record_type,
        })
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum RecordType {
    Full,
    First,
    Middle,
    Last,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Full,
            1 => Self::First,
            2 => Self::Middle,
            3 => Self::Last,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Error)]
pub enum EncodeError<K, T, V>
where
    K: std::error::Error,
    T: std::error::Error,
    V: std::error::Error,
{
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("key error: {0}")]
    Key(#[source] K),
    #[error("timestamp error: {0}")]
    Timsetamp(#[source] T),
    #[error("value error: {0}")]
    Value(#[source] V),
}

#[derive(Debug, Error)]
pub enum DecodeError<K, T, V>
where
    K: std::error::Error,
    T: std::error::Error,
    V: std::error::Error,
{
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("key error: {0}")]
    Key(#[source] K),
    #[error("timestamp error: {0}")]
    Timetamp(#[source] T),
    #[error("value error: {0}")]
    Value(#[source] V),
}
