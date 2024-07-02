use std::{
    collections::{btree_map::Entry, BTreeMap, HashSet},
    fmt::Debug,
    hash::Hash,
    ops::Bound,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use thiserror::Error;

pub(crate) type TimeStamp = u64;

pub trait Oracle<K>: Sized
where
    K: Ord,
{
    fn start_read(&self) -> TimeStamp;

    fn read_commit(&self, ts: TimeStamp);

    fn start_write(&self) -> TimeStamp;

    fn write_commit(
        &self,
        read_at: TimeStamp,
        write_at: TimeStamp,
        in_write: HashSet<K>,
    ) -> Result<(), WriteConflict<K>>;
}

#[derive(Debug, Error)]
#[error("transaction write conflict: {keys:?}")]
pub struct WriteConflict<K> {
    keys: Vec<K>,
}

impl<K> WriteConflict<K> {
    pub fn to_keys(self) -> Vec<K> {
        self.keys
    }
}

#[derive(Debug)]
pub struct LocalOracle<K>
where
    K: Ord,
{
    now: AtomicU64,
    in_read: Mutex<BTreeMap<u64, usize>>,
    committed_txns: Mutex<BTreeMap<u64, HashSet<K>>>,
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
    K: Ord + Hash + Clone,
{
    fn start_read(&self) -> TimeStamp {
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

    fn read_commit(&self, ts: TimeStamp) {
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

    fn start_write(&self) -> TimeStamp {
        self.now.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn write_commit(
        &self,
        read_at: TimeStamp,
        write_at: TimeStamp,
        in_write: HashSet<K>,
    ) -> Result<(), WriteConflict<K>> {
        let mut committed_txns = self.committed_txns.lock().unwrap();
        let conflicts: Vec<_> = committed_txns
            .range((Bound::Excluded(read_at), Bound::Excluded(write_at)))
            .flat_map(|(_, txn)| txn.intersection(&in_write))
            .cloned()
            .collect();

        if !conflicts.is_empty() {
            return Err(WriteConflict { keys: conflicts });
        }
        committed_txns.insert(write_at, in_write);
        Ok(())
    }
}
