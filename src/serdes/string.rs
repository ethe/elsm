use std::io;

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Decode, Encode};

impl Encode for String {
    type Error = io::Error;

    async fn encode<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        writer.write_all(&(self.len() as u16).to_le_bytes()).await?;
        writer.write_all(self.as_bytes()).await
    }
}

impl Decode for String {
    type Error = io::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let len = {
            let mut len = [0; 2];
            reader.read_exact(&mut len).await?;
            u16::from_le_bytes(len) as usize
        };

        let vec = {
            let mut vec = vec![0; len];
            reader.read_exact(&mut vec).await?;
            vec
        };

        Ok(unsafe { String::from_utf8_unchecked(vec) })
    }
}
