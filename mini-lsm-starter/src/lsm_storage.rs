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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    apply_full_compaction, CompactionController, CompactionOptions, CompactionTask,
    LeveledCompactionController, LeveledCompactionOptions, SimpleLeveledCompactionController,
    SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{Key, TS_RANGE_BEGIN};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Manifest,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.compaction_notifier.send(())?;
        if let Some(h) = self.compaction_thread.lock().take() {
            h.join()
                .map_err(|_| anyhow::anyhow!("Compaction thread panicked"))?;
        }
        self.flush_notifier.send(())?;
        if let Some(h) = self.flush_thread.lock().take() {
            h.join()
                .map_err(|_| anyhow::anyhow!("Flush thread panicked"))?;
        }

        self.sync()?;
        if !self.inner.options.enable_wal {
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            }

            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest;
        let mut records = None;
        if !path.exists() || !path.join("MANIFEST").exists() {
            std::fs::create_dir_all(path)?;
            manifest = Manifest::create(path.join("MANIFEST"))?;
        } else {
            let (manifest_, records_) = Manifest::recover(path.join("MANIFEST"))?;
            manifest = manifest_;
            records = Some(records_);
        };

        let state = LsmStorageState::create(&options);

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        if let Some(records) = records {
            storage.recovery(&records)?;
        } else {
            storage.init()?;
        }

        Ok(storage)
    }

    fn recovery(&self, records: &[ManifestRecord]) -> Result<()> {
        let mut guard = self.state.write();
        let mut state = guard.as_ref().clone();
        let mut imm_memtables = Vec::new();
        let mut memtable_id = 0;
        for record in records {
            match record {
                ManifestRecord::Flush(sst_id) => {
                    let sst_id = *sst_id;
                    assert!(sst_id == imm_memtables.pop().unwrap());
                    if self.compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, sst_id);
                    } else {
                        state.levels.insert(0, (sst_id, vec![sst_id]));
                    }
                }
                ManifestRecord::Compaction(task, output) => {
                    let (state_, _) = if matches!(task, CompactionTask::ForceFullCompaction { .. })
                    {
                        apply_full_compaction(&state, task, output)
                    } else {
                        self.compaction_controller
                            .apply_compaction_result(&state, task, output, true)
                    };
                    state = state_;
                }
                ManifestRecord::NewMemtable(sst_id) => {
                    imm_memtables.insert(0, memtable_id);
                    memtable_id = *sst_id;
                }
            }
        }

        // recovery sst files
        let iter = state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, ssts)| ssts));
        for &sst_id in iter {
            let sst_path = self.path_of_sst(sst_id);
            let file = FileObject::open(&sst_path)?;
            let sst = SsTable::open(sst_id, Some(self.block_cache.clone()), file)?;
            state.sstables.insert(sst_id, Arc::new(sst));
        }

        // order for leveled compaction
        if matches!(self.compaction_controller, CompactionController::Leveled(_)) {
            for (_, ssts) in &mut state.levels {
                ssts.sort_unstable_by_key(|id| state.sstables[id].first_key());
            }
        }

        // calculate max sst id
        let max_sst_id = state.sstables.keys().max().copied().unwrap_or(memtable_id);
        let next_sst_id = std::cmp::max(max_sst_id, memtable_id) + 1;
        self.next_sst_id
            .store(next_sst_id, std::sync::atomic::Ordering::SeqCst);
        let memtable = if self.options.enable_wal {
            MemTable::recover_from_wal(memtable_id, self.path_of_wal(memtable_id))?
        } else {
            let memtable_id = self.next_sst_id();
            self.manifest.add_record(
                &self.state_lock.lock(),
                ManifestRecord::NewMemtable(memtable_id),
            )?;
            MemTable::create(memtable_id)
        };
        state.memtable = Arc::new(memtable);

        // recovery imm memtables
        if self.options.enable_wal {
            state.imm_memtables = imm_memtables
                .into_iter()
                .map(|id| Arc::new(MemTable::recover_from_wal(id, self.path_of_wal(id)).unwrap()))
                .collect();
        }

        *guard = Arc::new(state);
        Ok(())
    }

    fn init(&self) -> Result<()> {
        if self.options.enable_wal {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();

            let memtable = MemTable::create_with_wal(0, self.path_of_wal(0))?;
            state.memtable = Arc::new(memtable);
            *guard = Arc::new(state);
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let state = self.state.read();
        state.memtable.sync_wal()?;
        self.sync_dir()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        for memtable in &snapshot.imm_memtables {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        for &sst_id in &snapshot.l0_sstables {
            let sst = snapshot
                .sstables
                .get(&sst_id)
                .ok_or(anyhow::anyhow!("SSTable not found"))?;
            if !key_within(sst, key) {
                continue;
            }
            if let Some(bloom) = &sst.bloom {
                if !bloom.may_contain(farmhash::fingerprint32(key)) {
                    continue;
                }
            }
            let iter = SsTableIterator::create_and_seek_to_key(
                sst.clone(),
                Key::from_slice(key, TS_RANGE_BEGIN),
            )?;
            if iter.is_valid() && iter.key().key_ref() == key {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        for (_, sstables) in &snapshot.levels {
            let mut sstables_actual = Vec::new();
            for &sst_id in sstables {
                let sst = snapshot
                    .sstables
                    .get(&sst_id)
                    .ok_or(anyhow::anyhow!("SSTable not found"))?;
                sstables_actual.push(sst.clone());
            }
            let iter = SstConcatIterator::create_and_seek_to_key(
                sstables_actual,
                Key::from_slice(key, TS_RANGE_BEGIN),
            )?;
            if iter.is_valid() && iter.key().key_ref() == key {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => self.put(key.as_ref(), value.as_ref())?,
                WriteBatchRecord::Del(key) => self.delete(key.as_ref())?,
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let state = self.state.read();
        state.memtable.put(key, value)?;
        let approximate_size = state.memtable.approximate_size();
        drop(state);
        if approximate_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let approximate_size = self.state.read().memtable.approximate_size();
            if approximate_size >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            MemTable::create_with_wal(new_memtable_id, self.path_of_wal(new_memtable_id))?
        } else {
            MemTable::create(new_memtable_id)
        };
        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut state.memtable, Arc::new(new_memtable));
            old_memtable.sync_wal()?;
            state.imm_memtables.insert(0, old_memtable);
            *guard = Arc::new(state);
        }
        self.manifest.add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(new_memtable_id),
        )?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let imm_memtable = {
            let guard = self.state.read();
            guard
                .imm_memtables
                .last()
                .ok_or(anyhow::anyhow!("No imm memtable"))?
                .clone()
        };

        let sst_id = imm_memtable.id();
        let sst_path = self.path_of_sst(sst_id);
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        imm_memtable.flush(&mut sst_builder)?;
        let sstable = sst_builder.build(sst_id, Some(self.block_cache.clone()), sst_path)?;
        self.sync_dir()?;

        let record = ManifestRecord::Flush(sst_id);
        self.manifest.add_record(&state_lock, record)?;

        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state.imm_memtables.pop();
            state.sstables.insert(sst_id, Arc::new(sstable));
            if self.compaction_controller.flush_to_l0() {
                state.l0_sstables.insert(0, sst_id);
            } else {
                state.levels.insert(0, (sst_id, vec![sst_id]));
            }
            *guard = Arc::new(state);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut memtable_iters = Vec::new();
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for imm_memtable in &snapshot.imm_memtables {
            memtable_iters.push(Box::new(imm_memtable.scan(lower, upper)));
        }

        let mut l0_sstable_iters = Vec::new();
        for &sst_id in &snapshot.l0_sstables {
            let sst = snapshot
                .sstables
                .get(&sst_id)
                .ok_or(anyhow::anyhow!("SSTable not found"))?;
            if !range_overlap(sst, lower, upper) {
                continue;
            }
            let iter = match lower {
                Bound::Included(lower) => SsTableIterator::create_and_seek_to_key(
                    sst.clone(),
                    Key::from_slice(lower, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(lower) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sst.clone(),
                        Key::from_slice(lower, TS_RANGE_BEGIN),
                    )?;
                    if iter.is_valid() {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone())?,
            };
            l0_sstable_iters.push(Box::new(iter));
        }

        let mut sstables_iters = Vec::new();
        for (_, sstables) in &snapshot.levels {
            let mut sstables_actual = Vec::new();
            for &sst_id in sstables {
                let sst = snapshot
                    .sstables
                    .get(&sst_id)
                    .ok_or(anyhow::anyhow!("SSTable not found"))?;
                if !range_overlap(sst, lower, upper) {
                    continue;
                }
                sstables_actual.push(sst.clone());
            }
            if sstables_actual.is_empty() {
                continue;
            }
            let iter = match lower {
                Bound::Included(lower) => SstConcatIterator::create_and_seek_to_key(
                    sstables_actual,
                    Key::from_slice(lower, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(lower) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        sstables_actual,
                        Key::from_slice(lower, TS_RANGE_BEGIN),
                    )?;
                    if iter.is_valid() {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables_actual)?,
            };
            sstables_iters.push(Box::new(iter));
        }

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                MergeIterator::create(memtable_iters),
                TwoMergeIterator::create(
                    MergeIterator::create(l0_sstable_iters),
                    MergeIterator::create(sstables_iters),
                )?,
            )?,
            map_bound(upper),
        )?))
    }
}

fn range_overlap(sst: &SsTable, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    (match lower {
        Bound::Included(lower) => sst.last_key().key_ref() >= lower,
        Bound::Excluded(lower) => sst.last_key().key_ref() > lower,
        Bound::Unbounded => true,
    }) && (match upper {
        Bound::Included(upper) => sst.first_key().key_ref() <= upper,
        Bound::Excluded(upper) => sst.first_key().key_ref() < upper,
        Bound::Unbounded => true,
    })
}

fn key_within(sst: &SsTable, key: &[u8]) -> bool {
    sst.first_key().key_ref() <= key && sst.last_key().key_ref() >= key
}
