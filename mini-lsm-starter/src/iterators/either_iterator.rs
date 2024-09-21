use anyhow::Result;

use super::StorageIterator;

pub enum EitherIterator<A: StorageIterator, B: StorageIterator> {
    A(A),
    B(B),
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for EitherIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self {
            Self::A(a) => a.key(),
            Self::B(b) => b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            Self::A(a) => a.value(),
            Self::B(b) => b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            Self::A(a) => a.is_valid(),
            Self::B(b) => b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            Self::A(a) => a.next(),
            Self::B(b) => b.next(),
        }
    }

    fn num_active_iterators(&self) -> usize {
        match self {
            Self::A(a) => a.num_active_iterators(),
            Self::B(b) => b.num_active_iterators(),
        }
    }
}
