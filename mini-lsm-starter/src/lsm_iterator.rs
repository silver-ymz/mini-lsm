// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    MergeIterator<MemTableIterator>,
    TwoMergeIterator<MergeIterator<SsTableIterator>, MergeIterator<SstConcatIterator>>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    prev_key: Vec<u8>,
    upper_bound: Bound<Bytes>,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        upper_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut this = Self {
            inner: iter,
            prev_key: Vec::new(),
            upper_bound,
            read_ts,
        };

        this.skip()?;
        if this.inner.is_valid() {
            this.prev_key.extend_from_slice(this.inner.key().key_ref());
            if this.inner.value().is_empty() {
                this.next()?;
            }
        }

        Ok(this)
    }

    fn skip(&mut self) -> Result<()> {
        let key = self.prev_key.as_slice();
        while self.inner.is_valid()
            && (self.inner.key().key_ref() == key || self.inner.key().ts() > self.read_ts)
        {
            self.inner.next()?
        }

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
            && match &self.upper_bound {
                Bound::Included(key) => self.inner.key().key_ref() <= key,
                Bound::Excluded(key) => self.inner.key().key_ref() < key,
                Bound::Unbounded => true,
            }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.skip()?;

        if self.inner.is_valid() {
            self.prev_key.clear();
            self.prev_key.extend_from_slice(self.inner.key().key_ref());
        }

        if self.inner.is_valid() && self.inner.value().is_empty() {
            self.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Iterator has errored");
        }
        if !self.iter.is_valid() {
            return Ok(());
        }
        if let Err(e) = self.iter.next() {
            self.has_errored = true;
            return Err(e);
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
