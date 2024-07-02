use std::{
    collections::{btree_map::Range, Bound},
    fmt::Debug,
    pin::{pin, Pin},
    task::{Context, Poll},
};

use arrow::array::RecordBatch;
use futures::{Stream, StreamExt};
use pin_project::pin_project;

use crate::{
    index_batch::IndexBatch, mem_table::InternalKey, oracle::TimeStamp, schema::Schema,
    stream::StreamError,
};

#[pin_project]
#[derive(Debug)]
pub(crate) struct IndexBatchStream<'a, S>
where
    S: Schema,
{
    batch: &'a RecordBatch,
    item_buf: Option<(S::PrimaryKey, Option<S>)>,
    inner: Range<'a, InternalKey<S::PrimaryKey>, u32>,
    ts: TimeStamp,
}

impl<'a, S> Stream for IndexBatchStream<'a, S>
where
    S: Schema,
{
    type Item = Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        for (InternalKey { key, ts }, offset) in this.inner.by_ref() {
            if ts <= this.ts
                && matches!(
                    this.item_buf.as_ref().map(|(k, _)| k != key),
                    Some(true) | None
                )
            {
                return Poll::Ready(
                    this.item_buf
                        .replace((key.clone(), S::from_batch(this.batch, *offset as usize).1))
                        .map(Ok),
                );
            }
        }
        Poll::Ready(this.item_buf.take().map(Ok))
    }
}

impl<S> IndexBatch<S>
where
    S: Schema,
{
    pub(crate) async fn range(
        &self,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
        ts: &TimeStamp,
    ) -> Result<IndexBatchStream<S>, StreamError<S::PrimaryKey, S>> {
        let mut iterator = IndexBatchStream {
            batch: &self.batch,
            inner: self.index.range((
                lower
                    .map(|k| {
                        Bound::Included(InternalKey {
                            key: k.clone(),
                            ts: *ts,
                        })
                    })
                    .unwrap_or(Bound::Unbounded),
                upper
                    .map(|k| {
                        Bound::Included(InternalKey {
                            key: k.clone(),
                            ts: TimeStamp::default(),
                        })
                    })
                    .unwrap_or(Bound::Unbounded),
            )),
            item_buf: None,
            ts: *ts,
        };

        {
            let mut iterator = pin!(&mut iterator);
            // filling first item
            let _ = iterator.next().await;
        }

        Ok(iterator)
    }
}

#[cfg(test)]
mod tests {
    use futures::{executor::block_on, StreamExt};

    use crate::{
        mem_table::MemTable, oracle::LocalOracle, tests::UserInner,
        wal::provider::in_mem::InMemProvider, Db,
    };

    #[test]
    fn range() {
        block_on(async {
            let mut mem_table = MemTable::<UserInner>::default();

            mem_table.insert(0, 0, None);
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

            let mut iterator = batch.range(Some(&1), Some(&2), &1).await.unwrap();

            assert_eq!(iterator.next().await.unwrap().unwrap(), (1, None));
            assert_eq!(
                iterator.next().await.unwrap().unwrap(),
                (
                    2,
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
                        0
                    ))
                )
            );
            assert!(iterator.next().await.is_none())
        })
    }
}
