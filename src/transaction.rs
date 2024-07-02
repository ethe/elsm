use std::{
    collections::{btree_map, btree_map::Entry, BTreeMap},
    fmt::Debug,
    marker::PhantomData,
    ops::Bound,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project::pin_project;
use thiserror::Error;

use crate::{
    oracle::{TimeStamp, WriteConflict},
    schema::Schema,
    stream::{merge_stream::MergeStream, EStreamImpl, StreamError},
    GetWrite,
};

#[derive(Debug)]
pub struct Transaction<S, DB>
where
    S: Schema,
    DB: GetWrite<S>,
{
    pub(crate) read_at: TimeStamp,
    pub(crate) local: BTreeMap<S::PrimaryKey, Option<S>>,
    share: Arc<DB>,
}

impl<S, DB> Transaction<S, DB>
where
    S: Schema,
    DB: GetWrite<S>,
{
    pub(crate) fn new(share: Arc<DB>) -> Self {
        let read_at = share.start_read();
        Self {
            read_at,
            local: BTreeMap::new(),
            share,
        }
    }

    pub async fn get(&self, key: &S::PrimaryKey) -> Option<S> {
        match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some(v.clone()),
            None => self.share.get(key, &self.read_at).await,
        }
    }

    pub fn set(&mut self, key: S::PrimaryKey, value: S) {
        self.entry(key, Some(value))
    }

    pub fn remove(&mut self, key: S::PrimaryKey) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: S::PrimaryKey, value: Option<S>) {
        match self.local.entry(key) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    pub async fn commit(self) -> Result<(), CommitError<S::PrimaryKey>> {
        self.share.read_commit(self.read_at);
        if self.local.is_empty() {
            return Ok(());
        }
        let write_at = self.share.start_write();
        self.share
            .write_commit(self.read_at, write_at, self.local.keys().cloned().collect())?;
        self.share
            .write_batch(self.local.into_iter().map(|(k, v)| (k, write_at, v)))
            .await?;
        Ok(())
    }

    pub async fn range(
        &self,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
    ) -> Result<MergeStream<S>, StreamError<S::PrimaryKey, S>> {
        let mut iters = self.share.inner_range(lower, upper, &self.read_at).await?;
        let range = self
            .local
            .range::<S::PrimaryKey, (Bound<&S::PrimaryKey>, Bound<&S::PrimaryKey>)>((
                lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
                upper.map(Bound::Included).unwrap_or(Bound::Unbounded),
            ));
        let iter = TransactionStream {
            range,
            _p: Default::default(),
        };
        iters.insert(0, EStreamImpl::TransactionInner(iter));

        MergeStream::new(iters).await
    }
}

#[pin_project]
pub(crate) struct TransactionStream<'a, S, E>
where
    S: Schema,
{
    #[pin]
    range: btree_map::Range<'a, S::PrimaryKey, Option<S>>,
    _p: PhantomData<E>,
}

impl<'a, S, E> Stream for TransactionStream<'a, S, E>
where
    S: Schema,
{
    type Item = Result<(S::PrimaryKey, Option<S>), E>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        Poll::Ready(
            this.range
                .next()
                .map(|(key, value)| (key.clone(), value.clone()))
                .map(Ok),
        )
    }
}

#[derive(Debug, Error)]
pub enum CommitError<K> {
    WriteConflict(Vec<K>),
    WriteError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<K> From<WriteConflict<K>> for CommitError<K> {
    fn from(e: WriteConflict<K>) -> Self {
        CommitError::WriteConflict(e.to_keys())
    }
}
