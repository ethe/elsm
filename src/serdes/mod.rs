mod arc;
mod num;
mod option;
mod string;

use std::{future::Future, io};

use futures::{AsyncRead, AsyncWrite};

pub trait Encode {
    type Error: From<io::Error> + std::error::Error + 'static;

    fn encode<W>(&self, writer: &mut W) -> impl Future<Output = Result<(), Self::Error>>
    where
        W: AsyncWrite + Unpin;
}

impl<T: Encode> Encode for &T {
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin,
    {
        Encode::encode(*self, writer).await
    }
}

pub trait Decode: Sized {
    type Error: From<io::Error> + std::error::Error + 'static;

    fn decode<R>(reader: &mut R) -> impl Future<Output = Result<Self, Self::Error>>
    where
        R: AsyncRead + Unpin;
}
