use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

pub enum Either<Lhs, Rhs> {
    Left(Lhs),
    Right(Rhs),
}

impl<Lhs, Rhs> Debug for Either<Lhs, Rhs>
where
    Lhs: Debug,
    Rhs: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Either::Left(a) => a.fmt(f),
            Either::Right(b) => b.fmt(f),
        }
    }
}

impl<Lhs, Rhs> Display for Either<Lhs, Rhs>
where
    Lhs: Display,
    Rhs: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Either::Left(a) => a.fmt(f),
            Either::Right(b) => b.fmt(f),
        }
    }
}

impl<Lhs, Rhs> Error for Either<Lhs, Rhs>
where
    Lhs: Debug + Display + Error,
    Rhs: Debug + Display + Error,
{
}
