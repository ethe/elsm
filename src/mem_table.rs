use std::{borrow::Borrow, io, ops::Bound, pin::pin, sync::Arc};

use crossbeam_skiplist::SkipMap;
use futures::StreamExt;

use crate::{
    record::{Record, RecordType},
    wal::{WalRecover, WalWrite},
};

#[derive(Debug)]
pub(crate) struct MemTable<K, V, W, T = u64>
where
    K: Ord,
    T: Ord,
{
    data: SkipMap<Arc<K>, SkipMap<T, Option<V>>>,
    wal: W,
}

impl<K, V, W, T> MemTable<K, V, W, T>
where
    K: Ord + Send + 'static,
    T: Ord + Send + 'static,
    V: Send + 'static,
    W: WalRecover<Arc<K>, V, T>,
{
    pub(crate) async fn new(wal: W) -> Result<Self, W::Error> {
        let this = Self {
            data: SkipMap::new(),
            wal,
        };

        this.recover().await?;

        Ok(this)
    }

    pub(crate) async fn recover(&self) -> Result<(), W::Error> {
        let mut stream = pin!(self.wal.recover());
        let mut batch = None;
        while let Some(record) = stream.next().await {
            let record = record?;
            match record.record_type {
                RecordType::Full => {
                    self.data
                        .get_or_insert_with(record.key, || SkipMap::new())
                        .value()
                        .insert(record.ts, record.value);
                }
                RecordType::First => {
                    if batch.is_none() {
                        batch = Some(vec![record]);
                        continue;
                    }
                    panic!("batch should be committed before next first record");
                }
                RecordType::Middle => {
                    if let Some(batch) = &mut batch {
                        batch.push(record);
                        continue;
                    }
                    panic!("middle record should in a batch");
                }
                RecordType::Last => {
                    if let Some(b) = &mut batch {
                        b.push(record);
                        let batch = batch.take().unwrap();
                        for record in batch {
                            self.data
                                .get_or_insert_with(record.key, || SkipMap::new())
                                .value()
                                .insert(record.ts, record.value);
                        }
                        continue;
                    }
                    panic!("last record should in a batch");
                }
            }
        }
        Ok(())
    }
}

impl<K, V, W, T> MemTable<K, V, W, T>
where
    K: Ord + 'static,
    T: Ord + Send + 'static,
    V: Sync + Send + 'static,
    W: WalWrite<Arc<K>, V, T>,
{
    pub(crate) async fn insert(&self, record: Record<Arc<K>, V, T>) -> Result<(), W::Error> {
        self.wal.write(&record.as_ref()).await?;
        self.data
            .get_or_insert_with(record.key, || SkipMap::new())
            .value()
            .insert(record.ts, record.value);
        Ok(())
    }

    pub(crate) async fn put_batch(
        &self,
        mut kvs: impl ExactSizeIterator<Item = (Arc<K>, T, Option<V>)>,
    ) -> Result<(), W::Error> {
        match kvs.len() {
            0 => Ok(()),
            1 => {
                let (key, ts, value) = kvs.next().unwrap();
                self.insert(Record::new(RecordType::Full, key, ts, value))
                    .await
            }
            len => {
                let (key, ts, value) = kvs.next().unwrap();
                self.insert(Record::new(RecordType::First, key, ts, value))
                    .await?;

                for (key, ts, value) in (&mut kvs).take(len - 2) {
                    self.insert(Record::new(RecordType::First, key, ts, value))
                        .await?;
                }

                let (key, ts, value) = kvs.next().unwrap();
                self.insert(Record::new(RecordType::First, key, ts, value))
                    .await
            }
        }
    }

    pub(crate) fn get<G, Q>(&self, key: &Q, ts: &T, f: impl FnOnce(&V) -> G) -> Option<G>
    where
        Q: ?Sized + Ord,
        Arc<K>: Borrow<Q>,
    {
        if let Some(entry) = self.data.get(key) {
            if let Some(entry) = entry
                .value()
                .range((Bound::Unbounded, Bound::Included(ts)))
                .next_back()
            {
                return entry.value().as_ref().map(f);
            }
        }
        None
    }

    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.wal.flush().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{executor::block_on, io::Cursor};

    use super::MemTable;
    use crate::{record::Record, wal::WalFile};

    #[test]
    fn insert_recover_and_get() {
        let mut file = Vec::new();
        let key = Arc::new("key".to_owned());
        let value = "value".to_owned();
        block_on(async {
            {
                let wal = WalFile::new(Cursor::new(&mut file), crc32fast::Hasher::new);
                let mut mem_table = MemTable::new(wal).await.unwrap();
                mem_table
                    .insert(Record::new(
                        crate::record::RecordType::Full,
                        key.clone(),
                        0,
                        Some(value.clone()),
                    ))
                    .await
                    .unwrap();

                mem_table.flush().await.unwrap();
            }
            {
                let wal = WalFile::new(Cursor::new(&mut file), crc32fast::Hasher::new);
                let mem_table: MemTable<String, String, _> = MemTable::new(wal).await.unwrap();

                assert_eq!(
                    mem_table.get(&"key".to_owned(), &0_u64, |v| v.clone()),
                    Some(value)
                );
            }
        });
    }
}
