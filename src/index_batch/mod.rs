pub(crate) mod stream;

use std::{
    collections::{BTreeMap, Bound},
    fmt::Debug,
    iter::Iterator,
};

use arrow::array::RecordBatch;

use crate::{mem_table::InternalKey, oracle::TimeStamp, schema::Schema};

#[derive(Debug)]
pub(crate) struct IndexBatch<S>
where
    S: Schema,
{
    pub(crate) batch: RecordBatch,
    pub(crate) index: BTreeMap<InternalKey<S::PrimaryKey>, u32>,
}

impl<S> IndexBatch<S>
where
    S: Schema,
{
    pub(crate) async fn find(&self, key: &S::PrimaryKey, ts: &TimeStamp) -> Option<Option<S>> {
        let internal_key = InternalKey {
            key: key.clone(),
            ts: *ts,
        };
        if let Some((InternalKey { key: item_key, .. }, offset)) = self
            .index
            .range((Bound::Included(&internal_key), Bound::Unbounded))
            .next()
        {
            if item_key == key {
                let (_, item) = S::from_batch(&self.batch, *offset as usize);

                return Some(item);
            }
        }
        None
    }

    pub(crate) fn scope(&self) -> Option<(&S::PrimaryKey, &S::PrimaryKey)> {
        if let (Some((min, _)), Some((max, _))) =
            (self.index.first_key_value(), self.index.last_key_value())
        {
            return Some((&min.key, &max.key));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        mem_table::MemTable, oracle::LocalOracle, tests::UserInner,
        wal::provider::in_mem::InMemProvider, Db,
    };

    #[tokio::test]
    async fn find() {
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
        mem_table.insert(1, 1, None);
        mem_table.insert(
            2,
            0,
            Some(UserInner::new(
                2,
                "2".to_string(),
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
        mem_table.insert(3, 0, None);

        let batch = Db::<UserInner, LocalOracle<u64>, InMemProvider>::freeze(mem_table)
            .await
            .unwrap();

        assert_eq!(
            batch.find(&1, &0).await,
            Some(Some(UserInner::new(
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
        assert_eq!(batch.find(&1, &1).await, Some(None));

        assert_eq!(
            batch.find(&2, &0).await,
            Some(Some(UserInner::new(
                2,
                "2".to_string(),
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
        assert_eq!(batch.find(&3, &0).await, Some(None));
    }
}
