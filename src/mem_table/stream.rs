use std::{
    collections::{btree_map, Bound},
    pin::{pin, Pin},
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use pin_project::pin_project;

use crate::{
    mem_table::{InternalKey, MemTable},
    oracle::TimeStamp,
    schema::Schema,
    stream::StreamError,
};

#[pin_project]
pub(crate) struct MemTableStream<'a, S>
where
    S: Schema,
{
    inner: btree_map::Range<'a, InternalKey<S::PrimaryKey>, Option<S>>,
    item_buf: Option<(S::PrimaryKey, Option<S>)>,
    ts: TimeStamp,
}

impl<'a, S> Stream for MemTableStream<'a, S>
where
    S: Schema,
{
    type Item = Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        for (InternalKey { key, ts }, value) in this.inner.by_ref() {
            if ts <= this.ts
                && matches!(
                    this.item_buf.as_ref().map(|(k, _)| k != key),
                    Some(true) | None
                )
            {
                return Poll::Ready(this.item_buf.replace((key.clone(), value.clone())).map(Ok));
            }
        }
        Poll::Ready(this.item_buf.take().map(Ok))
    }
}

impl<S> MemTable<S>
where
    S: Schema,
{
    pub(crate) async fn iter(&self) -> Result<MemTableStream<S>, StreamError<S::PrimaryKey, S>> {
        let mut iterator = MemTableStream {
            inner: self.data.range::<InternalKey<S::PrimaryKey>, (
                Bound<InternalKey<S::PrimaryKey>>,
                Bound<InternalKey<S::PrimaryKey>>,
            )>((Bound::Unbounded, Bound::Unbounded)),
            item_buf: None,
            ts: self.max_ts,
        };
        {
            let mut iterator = pin!(&mut iterator);
            // filling first item
            let _ = iterator.next().await;
        }
        Ok(iterator)
    }

    pub(crate) async fn range(
        &self,
        lower: Option<&S::PrimaryKey>,
        upper: Option<&S::PrimaryKey>,
        ts: &TimeStamp,
    ) -> Result<MemTableStream<S>, StreamError<S::PrimaryKey, S>> {
        let mut iterator = MemTableStream {
            inner: self.data.range((
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
    use futures::StreamExt;

    use crate::{mem_table::MemTable, tests::UserInner};

    #[tokio::test]
    async fn iterator() {
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

        mem_table.insert(
            2,
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

        let mut iterator = mem_table.iter().await.unwrap();

        assert_eq!(
            iterator.next().await.unwrap().unwrap(),
            (
                1,
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
        assert_eq!(
            iterator.next().await.unwrap().unwrap(),
            (
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
                    0
                ))
            )
        );

        drop(iterator);
        mem_table.insert(1, 3, None);

        let mut iterator = mem_table.iter().await.unwrap();

        assert_eq!(iterator.next().await.unwrap().unwrap(), (1, None));
    }

    #[tokio::test]
    async fn range() {
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
        mem_table.insert(
            2,
            1,
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
        mem_table.insert(
            4,
            0,
            Some(UserInner::new(
                4,
                "4".to_string(),
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

        let mut iterator = mem_table.iter().await.unwrap();

        assert_eq!(
            iterator.next().await.unwrap().unwrap(),
            (
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
                    0
                ))
            )
        );
        assert_eq!(
            iterator.next().await.unwrap().unwrap(),
            (
                2,
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
                    0
                ))
            )
        );
        assert_eq!(
            iterator.next().await.unwrap().unwrap(),
            (
                3,
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
                    0
                ))
            )
        );
        assert_eq!(
            iterator.next().await.unwrap().unwrap(),
            (
                4,
                Some(UserInner::new(
                    4,
                    "4".to_string(),
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
        assert!(iterator.next().await.is_none());

        let mut iterator = mem_table.range(Some(&2), Some(&3), &0).await.unwrap();

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
        assert_eq!(
            iterator.next().await.unwrap().unwrap(),
            (
                3,
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
                    0
                ))
            )
        );
        assert!(iterator.next().await.is_none())
    }
}
