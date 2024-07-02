use std::{
    marker::PhantomData,
    pin::{pin, Pin},
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use futures::{Future, Stream};
use pin_project::pin_project;

use crate::{schema::Schema, stream::StreamError};

#[pin_project]
pub(crate) struct BatchStream<S>
where
    S: Schema,
{
    pos: usize,
    inner: RecordBatch,
    _p: PhantomData<S>,
}

impl<S> BatchStream<S>
where
    S: Schema,
{
    pub(crate) fn new(batch: RecordBatch) -> Self {
        BatchStream {
            pos: 0,
            inner: batch,
            _p: Default::default(),
        }
    }

    async fn decode_item(
        &mut self,
    ) -> Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>> {
        // Safety: already check offset
        let (id, item) = S::from_batch(&self.inner, self.pos);

        self.pos += 1;
        Ok((id, item))
    }
}

impl<S> Stream for BatchStream<S>
where
    S: Schema,
{
    type Item = Result<(S::PrimaryKey, Option<S>), StreamError<S::PrimaryKey, S>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.pos < self.inner.num_rows() {
            let mut future = pin!(self.decode_item());

            return match future.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(Some(result)),
                Poll::Pending => Poll::Pending,
            };
        }
        Poll::Ready(None)
    }
}
