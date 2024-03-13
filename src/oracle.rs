use std::ops::Bound;
use std::{
    collections::{btree_map::Entry, BTreeMap, HashSet},
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use thiserror::Error;

use crate::transaction::Transaction;

pub trait Oracle<K>: Sized
where
    K: Ord,
{
    type Timestamp: Ord + Copy;

    fn read(&self) -> Self::Timestamp;

    fn read_commit(&self, ts: Self::Timestamp);

    fn tick(&self) -> Self::Timestamp;

    fn write_commit<V, W>(&self, txn: &Transaction<K, V, W, Self>) -> Result<(), WriteConflict<K>>;
}

#[derive(Debug, Error)]
#[error("transaction write conflict: {keys:?}")]
pub struct WriteConflict<K> {
    keys: Vec<Arc<K>>,
}

impl<K> WriteConflict<K> {
    pub fn to_keys(self) -> Vec<Arc<K>> {
        self.keys
    }
}

#[derive(Debug)]
pub(crate) struct LocalOracle<K>
where
    K: Ord,
{
    now: AtomicU64,
    in_read: Mutex<BTreeMap<u64, usize>>,
    committed_txns: Mutex<BTreeMap<u64, HashSet<Arc<K>>>>,
}

impl<K> Default for LocalOracle<K>
where
    K: Ord,
{
    fn default() -> Self {
        Self {
            now: Default::default(),
            in_read: Default::default(),
            committed_txns: Default::default(),
        }
    }
}

impl<K> Oracle<K> for LocalOracle<K>
where
    K: Ord + Hash,
{
    type Timestamp = u64;

    fn read(&self) -> Self::Timestamp {
        let mut in_read = self.in_read.lock().unwrap();
        let now = self.now.load(Ordering::Relaxed);
        match in_read.entry(now) {
            Entry::Vacant(v) => {
                v.insert(1);
            }
            Entry::Occupied(mut o) => {
                *o.get_mut() += 1;
            }
        }
        now
    }

    fn read_commit(&self, ts: Self::Timestamp) {
        match self.in_read.lock().unwrap().entry(ts) {
            Entry::Vacant(_) => panic!("commit non-existing read"),
            Entry::Occupied(mut o) => match o.get_mut() {
                1 => {
                    o.remove();
                }
                n => {
                    *n -= 1;
                }
            },
        }
    }

    fn tick(&self) -> Self::Timestamp {
        self.now.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn write_commit<V, W>(&self, txn: &Transaction<K, V, W, Self>) -> Result<(), WriteConflict<K>> {
        let in_write: HashSet<Arc<K>> = txn.local.keys().cloned().collect();

        let write_at = txn.write_at.unwrap();
        let mut committed_txns = self.committed_txns.lock().unwrap();
        let conflicts: Vec<_> = committed_txns
            .range((Bound::Excluded(txn.read_at), Bound::Excluded(write_at)))
            .flat_map(|(_, txn)| txn.intersection(&in_write))
            .cloned()
            .collect();

        if !conflicts.is_empty() {
            committed_txns.insert(write_at, in_write);
            return Err(WriteConflict { keys: conflicts });
        }

        committed_txns.insert(write_at, txn.local.keys().cloned().collect());
        Ok(())
    }
}
