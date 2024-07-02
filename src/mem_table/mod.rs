pub(crate) mod stream;

use std::{cmp, cmp::Ordering, collections::BTreeMap, ops::Bound, pin::pin};

use futures::StreamExt;

use crate::{
    oracle::TimeStamp, record::RecordType, schema::Schema, serdes::Encode, wal::WalRecover,
};

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct InternalKey<K> {
    pub(crate) key: K,
    pub(crate) ts: TimeStamp,
}

impl<K> PartialOrd<Self> for InternalKey<K>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for InternalKey<K>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| other.ts.cmp(&self.ts))
    }
}

#[derive(Debug)]
pub(crate) struct MemTable<S>
where
    S: Schema,
{
    pub(crate) data: BTreeMap<InternalKey<S::PrimaryKey>, Option<S>>,
    max_ts: TimeStamp,
    written_size: usize,
}

impl<S> Default for MemTable<S>
where
    S: Schema,
{
    fn default() -> Self {
        Self {
            data: BTreeMap::default(),
            max_ts: TimeStamp::default(),
            written_size: 0,
        }
    }
}

impl<S> MemTable<S>
where
    S: Schema,
{
    pub(crate) async fn from_wal<W>(wal: &mut W) -> Result<Self, W::Error>
    where
        W: WalRecover<S::PrimaryKey, S>,
    {
        let mut mem_table = Self::default();

        mem_table.recover(wal).await?;

        Ok(mem_table)
    }

    pub(crate) async fn recover<W>(&mut self, wal: &mut W) -> Result<(), W::Error>
    where
        W: WalRecover<S::PrimaryKey, S>,
    {
        let mut stream = pin!(wal.recover());
        let mut batch = None;
        while let Some(record) = stream.next().await {
            let record = record?;
            match record.record_type {
                RecordType::Full => self.insert(record.key, record.ts, record.value),
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
                    if let Some(b) = batch.take() {
                        for r in b {
                            self.insert(r.key, r.ts, r.value);
                        }
                        self.insert(record.key, record.ts, record.value);
                        continue;
                    }
                    panic!("last record should in a batch");
                }
            }
        }
        Ok(())
    }
}

impl<S> MemTable<S>
where
    S: Schema,
{
    pub(crate) fn is_excess(&self, max_size: usize) -> bool {
        self.written_size > max_size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn insert(&mut self, key: S::PrimaryKey, ts: TimeStamp, value: Option<S>) {
        self.max_ts = cmp::max(self.max_ts, ts);
        self.written_size = key.size() + ts.size() + value.as_ref().map(Encode::size).unwrap_or(0);

        let _ = self.data.insert(InternalKey { key, ts }, value);
    }

    pub(crate) fn get(&self, key: &S::PrimaryKey, ts: &TimeStamp) -> Option<Option<&S>> {
        let internal_key = InternalKey {
            key: key.clone(),
            ts: *ts,
        };

        self.data
            .range((Bound::Included(&internal_key), Bound::Unbounded))
            .next()
            .and_then(|(InternalKey { key: item_key, .. }, value)| {
                (item_key == key).then_some(value.as_ref())
            })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use futures::executor::block_on;

    use super::MemTable;
    use crate::{
        record::{Record, RecordType},
        tests::UserInner,
        wal::{FileId, WalFile, WalWrite},
    };

    #[test]
    fn crud() {
        block_on(async {
            let mut mem_table = MemTable::default();

            mem_table.insert(
                1,
                0,
                Some(UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                )),
            );
            mem_table.insert(
                1,
                1,
                Some(UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                )),
            );
            mem_table.insert(
                1,
                2,
                Some(UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                )),
            );

            mem_table.insert(
                3,
                0,
                Some(UserInner::new(
                    3,
                    "3".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                )),
            );

            assert_eq!(
                mem_table.get(&1, &0),
                Some(Some(&UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                )))
            );
            assert_eq!(
                mem_table.get(&1, &1),
                Some(Some(&UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                )))
            );
            assert_eq!(
                mem_table.get(&1, &2),
                Some(Some(&UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                )))
            );

            assert_eq!(
                mem_table.get(&3, &0),
                Some(Some(&UserInner::new(
                    3,
                    "3".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                )))
            );

            assert_eq!(mem_table.get(&2, &0), None);
            assert_eq!(mem_table.get(&4, &0), None);
            assert_eq!(
                mem_table.get(&1, &3),
                Some(Some(&UserInner::new(
                    1,
                    "1".to_string(),
                    false,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0
                )))
            );
        });
    }

    #[test]
    fn recover_from_wal() {
        let mut file = Vec::new();
        let key = 0;
        let value = UserInner::new(0, "v".to_string(), false, 0, 0, 0, 0, 0, 0, 0, 0);
        block_on(async {
            {
                let mut wal = WalFile::new(Cursor::new(&mut file), FileId::new());
                wal.write(Record::new(RecordType::Full, &key, 0, Some(&value)))
                    .await
                    .unwrap();
                wal.flush().await.unwrap();
            }
            {
                let mut wal = WalFile::new(Cursor::new(&mut file), FileId::new());
                let mem_table: MemTable<UserInner> = MemTable::from_wal(&mut wal).await.unwrap();
                assert_eq!(mem_table.get(&key, &0), Some(Some(&value)));
            }
        });
    }
}
