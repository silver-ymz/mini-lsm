use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = sstables
            .first()
            .map(|t| SsTableIterator::create_and_seek_to_first(t.clone()))
            .transpose()?;
        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = sstables.partition_point(|t| t.last_key().as_key_slice() < key);
        let current = sstables
            .get(idx)
            .map(|t| SsTableIterator::create_and_seek_to_key(t.clone(), key))
            .transpose()?;
        Ok(Self {
            current,
            next_sst_idx: idx + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let cur_iter = self
            .current
            .as_mut()
            .ok_or(anyhow::anyhow!("No more elements"))?;
        cur_iter.next()?;
        if cur_iter.is_valid() {
            return Ok(());
        }

        if self.next_sst_idx >= self.sstables.len() {
            self.current = None;
            return Ok(());
        }

        let next_iter =
            SsTableIterator::create_and_seek_to_first(self.sstables[self.next_sst_idx].clone())?;
        self.next_sst_idx += 1;
        self.current = Some(next_iter);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
