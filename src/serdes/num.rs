use std::{io, mem::size_of};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Decode, Encode};

impl Encode for u64 {
    type Error = io::Error;

    async fn encode<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        writer.write_all(&self.to_le_bytes()).await
    }
}

impl Decode for u64 {
    type Error = io::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let buf = {
            let mut buf = [0; size_of::<Self>()];
            reader.read_exact(&mut buf).await?;
            buf
        };

        Ok(Self::from_le_bytes(buf))
    }
}
