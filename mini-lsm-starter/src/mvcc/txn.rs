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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let commited = self.committed.load(Ordering::Acquire);
        if commited {
            return Err(anyhow::anyhow!("Transaction has been committed"));
        }

        if let Some(local_value) = self.local_storage.get(key) {
            let local_value = local_value.value();
            if !local_value.is_empty() {
                return Ok(Some(local_value.clone()));
            } else {
                return Ok(None);
            }
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let commited = self.committed.load(Ordering::Acquire);
        if commited {
            return Err(anyhow::anyhow!("Transaction has been committed"));
        }

        let local_iter = TxnLocalIterator::create(
            self.local_storage.clone(),
            lower.map(Bytes::copy_from_slice),
            upper.map(Bytes::copy_from_slice),
        );
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let iter = TwoMergeIterator::create(local_iter, lsm_iter)?;

        TxnIterator::create(self.clone(), iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let commited = self.committed.load(Ordering::Acquire);
        if commited {
            panic!("Transaction has been committed");
        }

        let key = Bytes::copy_from_slice(key);
        let value = Bytes::copy_from_slice(value);
        self.local_storage.insert(key, value);
    }

    pub fn delete(&self, key: &[u8]) {
        let commited = self.committed.load(Ordering::Acquire);
        if commited {
            panic!("Transaction has been committed");
        }

        let key = Bytes::copy_from_slice(key);
        self.local_storage.insert(key, Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        self.committed.store(true, Ordering::Release);

        let mut batch = Vec::new();
        for entry in self.local_storage.iter() {
            let key = entry.key().clone();
            let value = entry.value().clone();
            if value.is_empty() {
                batch.push(WriteBatchRecord::Del(key));
            } else {
                batch.push(WriteBatchRecord::Put(key, value));
            }
        }
        self.inner.write_batch(&batch)?;

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let mut guard = self.inner.mvcc.ts.lock();
        guard.1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn create(map: Arc<SkipMap<Bytes, Bytes>>, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Self {
        let mut iter = TxnLocalIterator::new(
            map,
            |map| map.range((lower, upper)),
            (Bytes::new(), Bytes::new()),
        );
        iter.next().unwrap();
        iter
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|this| {
            if let Some(entry) = this.iter.next() {
                *this.item = (entry.key().clone(), entry.value().clone());
            } else {
                *this.item = Default::default();
            }
        });
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(TxnIterator { _txn: txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
