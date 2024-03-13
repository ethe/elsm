pub(crate) mod mem_table;
pub(crate) mod oracle;
pub mod record;
pub mod serdes;
pub mod transaction;
pub mod utils;
pub mod wal;

use std::{borrow::Borrow, sync::Arc};

use mem_table::MemTable;
use oracle::Oracle;
use transaction::{CommitError, Transaction};
use wal::WalWrite;

#[derive(Debug)]
pub struct LsmTree<K, V, W, O>
where
    K: Ord,
    O: Oracle<K>,
{
    pub(crate) mutable: MemTable<K, V, W, O::Timestamp>,
    pub(crate) oracle: O,
}

impl<K, V, W, O> LsmTree<K, V, W, O>
where
    O: Oracle<K>,
    O::Timestamp: Send + 'static,
    K: Ord + Send + 'static,
    V: Sync + Send + 'static,
    W: WalWrite<Arc<K>, V, O::Timestamp>,
{
    pub(crate) async fn get_inner<G, Q>(
        &self,
        key: &Q,
        ts: &O::Timestamp,
        f: impl FnOnce(&V) -> G,
    ) -> Option<G>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>,
    {
        self.mutable.get(key, ts, f)
    }

    pub(crate) async fn put_batch_inner(
        &self,
        kvs: impl ExactSizeIterator<Item = (Arc<K>, O::Timestamp, Option<V>)>,
    ) -> Result<(), W::Error> {
        self.mutable.put_batch(kvs).await
    }

    pub async fn put_batch(
        self: &Arc<Self>,
        kvs: impl ExactSizeIterator<Item = (K, Option<V>)>,
    ) -> Result<(), CommitError<K, W::Error>> {
        let mut txn = self.new_txn().await;
        for (k, v) in kvs {
            txn.entry(k, v);
        }
        txn.commit().await
    }

    pub async fn get<G, Q>(&self, k: &Q, f: impl FnOnce(&V) -> G) -> Option<G>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>,
    {
        self.get_inner(k, &self.oracle.read(), f).await
    }

    pub async fn new_txn(self: &Arc<Self>) -> Transaction<K, V, W, O> {
        Transaction::new(self.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{executor::block_on, io::Cursor};

    use crate::{
        mem_table::MemTable, oracle::LocalOracle, transaction::CommitError, wal::WalFile, LsmTree,
    };

    #[test]
    fn read_committed() {
        let file = Vec::new();
        let wal = WalFile::new(Cursor::new(file), crc32fast::Hasher::new);

        block_on(async {
            let lsm_tree = Arc::new(LsmTree {
                mutable: MemTable::new(wal).await.unwrap(),
                oracle: LocalOracle::default(),
            });

            lsm_tree
                .put_batch(
                    [("key0".to_string(), Some(0)), ("key1".to_string(), Some(1))].into_iter(),
                )
                .await
                .unwrap();

            let mut t0 = lsm_tree.new_txn().await;
            let mut t1 = lsm_tree.new_txn().await;

            t0.set(
                "key0".into(),
                t0.get(&"key1".to_owned(), |v| *v).await.unwrap(),
            );
            t1.set(
                "key1".into(),
                t1.get(&"key0".to_owned(), |v| *v).await.unwrap(),
            );

            t0.commit().await.unwrap();
            t1.commit().await.unwrap();

            assert_eq!(
                lsm_tree.get(&Arc::from("key0".to_string()), |v| *v).await,
                Some(1)
            );
            assert_eq!(
                lsm_tree.get(&Arc::from("key1".to_string()), |v| *v).await,
                Some(0)
            );
        });
    }

    #[test]
    fn write_conflicts() {
        let file = Vec::new();
        let wal = WalFile::new(Cursor::new(file), crc32fast::Hasher::new);

        block_on(async {
            let lsm_tree = Arc::new(LsmTree {
                mutable: MemTable::new(wal).await.unwrap(),
                oracle: LocalOracle::default(),
            });

            lsm_tree
                .put_batch(
                    [("key0".to_string(), Some(0)), ("key1".to_string(), Some(1))].into_iter(),
                )
                .await
                .unwrap();

            let mut t0 = lsm_tree.new_txn().await;
            let mut t1 = lsm_tree.new_txn().await;

            t0.set(
                "key0".into(),
                t0.get(&"key1".to_owned(), |v| *v).await.unwrap(),
            );
            t1.set(
                "key0".into(),
                t1.get(&"key0".to_owned(), |v| *v).await.unwrap(),
            );

            t0.commit().await.unwrap();

            let commit = t1.commit().await;
            assert!(commit.is_err());
            if let Err(CommitError::WriteConflict(keys)) = commit {
                assert_eq!(lsm_tree.get(&keys[0], |v| *v).await, Some(1));
                return;
            }
            unreachable!();
        });
    }
}
