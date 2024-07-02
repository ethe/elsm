use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project::pin_project;
use thiserror::Error;

use crate::{
    index_batch::stream::IndexBatchStream,
    mem_table::stream::MemTableStream,
    schema::Schema,
    serdes::{Decode, Encode},
    stream::{buf_stream::BufStream, level_stream::LevelStream, table_stream::TableStream},
    transaction::TransactionStream,
};

pub(crate) mod batch_stream;
pub(crate) mod buf_stream;
pub(crate) mod level_stream;
pub(crate) mod merge_stream;
pub(crate) mod table_stream;

#[pin_project(project = EStreamImplProj)]
pub(crate) enum EStreamImpl<'a, S>
where
    S: Schema,
{
    Buf(#[pin] BufStream<'a, S::PrimaryKey, S, StreamError<S::PrimaryKey, S>>),
    IndexBatch(#[pin] IndexBatchStream<'a, S>),
    MemTable(#[pin] MemTableStream<'a, S>),
    TransactionInner(#[pin] TransactionStream<'a, S, StreamError<S::PrimaryKey, S>>),
    Table(#[pin] TableStream<'a, S>),
    Level(#[pin] LevelStream<'a, S>),
}

impl<'a, S> Stream for EStreamImpl<'a, S>
where
    S: Schema,
{
    type Item = Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EStreamImplProj::Buf(stream) => stream.poll_next(cx),
            EStreamImplProj::IndexBatch(stream) => stream.poll_next(cx),
            EStreamImplProj::MemTable(stream) => stream.poll_next(cx),
            EStreamImplProj::TransactionInner(stream) => stream.poll_next(cx),
            EStreamImplProj::Table(stream) => stream.poll_next(cx),
            EStreamImplProj::Level(stream) => stream.poll_next(cx),
        }
    }
}

impl<S> std::fmt::Display for EStreamImpl<'_, S>
where
    S: Schema,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EStreamImpl::Buf(_) => write!(f, "BufStream"),
            EStreamImpl::IndexBatch(_) => write!(f, "IndexBatchStream"),
            EStreamImpl::MemTable(_) => write!(f, "MemTableStream"),
            EStreamImpl::TransactionInner(_) => write!(f, "TransactionStream"),
            EStreamImpl::Table(_) => write!(f, "TableStream"),
            EStreamImpl::Level(_) => write!(f, "LevelStream"),
        }
    }
}

#[derive(Debug, Error)]
pub enum StreamError<K, V>
where
    K: Encode + Decode,
    V: Decode,
{
    #[error("compaction key encode error: {0}")]
    KeyEncode(#[source] <K as Encode>::Error),
    #[error("compaction key decode error: {0}")]
    KeyDecode(#[source] <K as Decode>::Error),
    #[error("compaction value decode error: {0}")]
    ValueDecode(#[source] <V as Decode>::Error),
    #[error("compaction io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("compaction arrow error: {0}")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("compaction parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
}
