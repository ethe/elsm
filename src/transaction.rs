use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use thiserror::Error;

use crate::{oracle::Oracle, wal::WalWrite, LsmTree};

#[derive(Debug)]
pub struct Transaction<K, V, W, O>
where
    K: Ord,
    O: Oracle<K>,
{
    pub(crate) read_at: O::Timestamp,
    pub(crate) write_at: Option<O::Timestamp>,
    pub(crate) local: BTreeMap<Arc<K>, Option<V>>,
    share: Arc<LsmTree<K, V, W, O>>,
}

impl<K, V, W, O> Transaction<K, V, W, O>
where
    O: Oracle<K>,
    O::Timestamp: Send + 'static,
    K: Ord + Send + 'static,
    V: Sync + Send + 'static,
    W: WalWrite<Arc<K>, V, O::Timestamp>,
{
    pub(crate) async fn new(share: Arc<LsmTree<K, V, W, O>>) -> Self {
        let read_at = share.oracle.read();
        Self {
            read_at,
            write_at: None,
            local: BTreeMap::new(),
            share,
        }
    }

    pub async fn get<G, F, Q>(&self, key: &Q, f: F) -> Option<G>
    where
        Q: ?Sized + Ord,
        F: FnOnce(&V) -> G,
        Arc<K>: Borrow<Q>,
    {
        match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some((f)(v)),
            None => self.share.get_inner(key, &self.read_at, f).await,
        }
    }

    pub fn set(&mut self, key: K, value: V) {
        self.entry(key, Some(value))
    }

    pub fn remove(&mut self, key: K) {
        self.entry(key, None)
    }

    pub(crate) fn entry(&mut self, key: K, value: Option<V>) {
        match self.local.entry(Arc::from(key)) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    pub async fn commit(mut self) -> Result<(), CommitError<K, W::Error>> {
        self.share.oracle.read_commit(self.read_at);
        if !self.local.is_empty() {
            let write_at = self.share.oracle.tick();
            self.write_at = Some(write_at);
            self.share
                .oracle
                .write_commit(&self)
                .map_err(|e| CommitError::WriteConflict(e.to_keys()))?;
            self.share
                .put_batch_inner(self.local.into_iter().map(|(k, v)| (k, write_at, v)))
                .await
                .map_err(CommitError::WriteError)?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CommitError<K, E: std::error::Error> {
    WriteConflict(Vec<Arc<K>>),
    WriteError(#[source] E),
}
