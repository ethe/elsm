use std::{
    marker::PhantomData,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use futures::Stream;

unsafe impl<K, V, E> Send for BufStream<'_, K, V, E>
where
    K: Ord + Clone + Sync,
    V: Sync,
    E: Sync,
{
}
unsafe impl<K, V, E> Sync for BufStream<'_, K, V, E>
where
    K: Ord + Clone + Sync,
    V: Sync,
    E: Sync,
{
}

pub(crate) struct BufStream<'a, K, V, E>
where
    K: Ord + Clone,
{
    inner: NonNull<Vec<(K, Option<V>)>>,
    pos: usize,
    _p: PhantomData<&'a E>,
}

impl<'a, K, V, E> BufStream<'a, K, V, E>
where
    K: Ord + Clone,
    V: 'a,
{
    pub(crate) fn new(items: Vec<(K, Option<V>)>) -> Self {
        BufStream {
            inner: Box::leak(Box::new(items)).into(),
            pos: 0,
            _p: Default::default(),
        }
    }

    unsafe fn inner(&self) -> &'a [(K, Option<V>)] {
        self.inner.as_ref()
    }

    unsafe fn take_item(&mut self) -> (K, Option<V>) {
        let value = self.inner.as_mut()[self.pos].1.take();
        let key = self.inner.as_ref()[self.pos].0.clone();

        (key, value)
    }
}

impl<'a, K, V, E> Stream for BufStream<'a, K, V, E>
where
    K: Ord + Clone + 'a,
    V: 'a,
{
    type Item = Result<(K, Option<V>), E>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(unsafe { self.pos < self.inner().len() }.then(|| {
            let result = unsafe { self.take_item() };
            self.pos += 1;
            Ok(result)
        }))
    }
}

impl<K, V, E> Drop for BufStream<'_, K, V, E>
where
    K: Ord + Clone,
{
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.inner.as_ptr())) }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        pin::{pin, Pin},
        sync::Arc,
    };

    use futures::{executor::block_on, StreamExt};

    use crate::stream::buf_stream::BufStream;

    #[test]
    fn iter() {
        block_on(async {
            let key_1 = Arc::new("key_1".to_owned());
            let key_2 = Arc::new("key_2".to_owned());
            let value_1 = "value_1".to_owned();

            let mut iter: Pin<&mut BufStream<_, _, Infallible>> = pin!(BufStream::new(vec![
                (key_1.clone(), Some(value_1.clone())),
                (key_2.clone(), None),
            ]));

            assert_eq!(iter.next().await.unwrap(), Ok((key_1, Some(value_1))));
            assert_eq!(iter.next().await.unwrap(), Ok((key_2, None)));
            assert_eq!(iter.next().await, None);
        });
    }
}
