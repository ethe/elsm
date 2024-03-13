use std::{
    hash::Hasher,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use pin_project_lite::pin_project;

pin_project! {
    pub(crate) struct HashWriter<H: Hasher, W: AsyncWrite> {
        hasher: H,
        #[pin]
        writer: W,
    }
}

impl<H: Hasher, W: AsyncWrite + Unpin> HashWriter<H, W> {
    pub(crate) fn new(hasher: H, writer: W) -> Self {
        Self { hasher, writer }
    }

    pub(crate) async fn eol(mut self) -> io::Result<usize> {
        self.writer.write(&self.hasher.finish().to_le_bytes()).await
    }
}

impl<H: Hasher, W: AsyncWrite> AsyncWrite for HashWriter<H, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();

        Poll::Ready(match ready!(this.writer.poll_write(cx, buf)) {
            Ok(n) => {
                this.hasher.write(&buf[..n]);
                Ok(n)
            }
            e => e,
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().writer.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().writer.poll_close(cx)
    }
}

pin_project! {
    pub(crate) struct HashReader<H: Hasher, R: AsyncRead> {
        hasher: H,
        #[pin]
        reader: R,
    }
}

impl<H: Hasher, R: AsyncRead + Unpin> HashReader<H, R> {
    pub(crate) fn new(hasher: H, reader: R) -> Self {
        Self { hasher, reader }
    }

    pub(crate) async fn checksum(mut self) -> io::Result<bool> {
        let mut hash = [0; 8];
        self.reader.read_exact(&mut hash).await?;
        let checksum = u64::from_le_bytes(hash);

        Ok(self.hasher.finish() == checksum)
    }
}

impl<H: Hasher, R: AsyncRead> AsyncRead for HashReader<H, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        match this.reader.poll_read(cx, buf) {
            Poll::Ready(ready) => Poll::Ready(match ready {
                Ok(n) => {
                    this.hasher.write(&buf[..n]);
                    Ok(n)
                }
                e => e,
            }),
            Poll::Pending => todo!(),
        }
    }
}
