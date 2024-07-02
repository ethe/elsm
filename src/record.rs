use std::{io, mem::size_of};

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    oracle::TimeStamp,
    serdes::{Decode, Encode},
};

#[derive(Debug)]
pub struct Record<K, V> {
    pub record_type: RecordType,
    pub key: K,
    pub ts: TimeStamp,
    pub value: Option<V>,
}

impl<K, V> Record<K, V> {
    pub fn new(record_type: RecordType, key: K, ts: TimeStamp, value: Option<V>) -> Self {
        Self {
            record_type,
            key,
            ts,
            value,
        }
    }

    pub fn as_ref(&self) -> Record<&K, &V> {
        Record::new(self.record_type, &self.key, self.ts, self.value.as_ref())
    }
}

impl<K, V> Encode for Record<K, V>
where
    K: Encode,
    V: Encode,
{
    type Error = EncodeError<K::Error, <TimeStamp as Encode>::Error, <Option<V> as Encode>::Error>;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send + Sync,
    {
        writer.write_all(&[self.record_type as u8]).await?;
        self.key.encode(writer).await.map_err(EncodeError::Key)?;
        self.ts
            .encode(writer)
            .await
            .map_err(EncodeError::Timsetamp)?;
        self.value.encode(writer).await.map_err(EncodeError::Value)
    }

    fn size(&self) -> usize {
        size_of::<u8>() + self.key.size() + self.ts.size() + self.value.size()
    }
}

impl<K, V> Decode for Record<K, V>
where
    K: Decode,
    V: Decode,
{
    type Error = DecodeError<K::Error, <Option<V> as Decode>::Error>;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut record_type = [0];
        reader.read_exact(&mut record_type).await?;
        let record_type = RecordType::from(record_type[0]);

        let key = K::decode(reader).await.map_err(DecodeError::Key)?;
        let ts = TimeStamp::decode(reader)
            .await
            .map_err(DecodeError::Timetamp)?;
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
pub enum DecodeError<K, V>
where
    K: std::error::Error,
    V: std::error::Error,
{
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("key error: {0}")]
    Key(#[source] K),
    #[error("timestamp error: {0}")]
    Timetamp(#[source] <TimeStamp as Decode>::Error),
    #[error("value error: {0}")]
    Value(#[source] V),
}
