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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::either_iterator::EitherIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::mem_table::MemTable;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = self.state.read().clone();
        match task {
            CompactionTask::Leveled(_leveled_compaction_task) => todo!(),
            CompactionTask::Tiered(_tiered_compaction_task) => todo!(),
            CompactionTask::Simple(task) => {
                let upper_level_iter = if task.upper_level.is_none() {
                    let iter = sstable_merge_iter(&state, &task.upper_level_sst_ids)?;
                    EitherIterator::A(iter)
                } else {
                    let iter = sstable_concat_iter(&state, &task.upper_level_sst_ids)?;
                    EitherIterator::B(iter)
                };
                let lower_level_iter = sstable_concat_iter(&state, &task.lower_level_sst_ids)?;
                let iter = TwoMergeIterator::create(upper_level_iter, lower_level_iter)?;
                self.compact_iterator(iter)
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let iter = TwoMergeIterator::create(
                    sstable_merge_iter(&state, l0_sstables)?,
                    sstable_concat_iter(&state, l1_sstables)?,
                )?;
                self.compact_iterator(iter)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        };
        let new_ssts = self.compact(&task)?;
        let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = task
        else {
            unreachable!()
        };

        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state
                .l0_sstables
                .truncate(state.l0_sstables.len() - l0_sstables.len());
            let mut new_l1_sstables = Vec::new();
            for sst in new_ssts {
                new_l1_sstables.push(sst.sst_id());
                state.sstables.insert(sst.sst_id(), sst);
            }
            state.levels[0].1 = new_l1_sstables;
            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                state.sstables.remove(id);
            }
            *guard = Arc::new(state);
        };

        for &id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().as_ref().clone();
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };

        let new_sstables = self.compact(&task)?;

        let del;
        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let output = new_sstables.iter().map(|s| s.sst_id()).collect::<Vec<_>>();
            (snapshot, del) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);
            for id in &del {
                snapshot.sstables.remove(id);
            }
            for sst in new_sstables {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            *guard = Arc::new(snapshot);
        }

        for id in del {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let mem_table_len = self.state.read().imm_memtables.len() + 1;
        if mem_table_len >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn flush_memtable(&self, memtable: MemTable) -> Result<Arc<SsTable>> {
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut sst_builder)?;
        let sstable = sst_builder.build(
            memtable.id(),
            Some(self.block_cache.clone()),
            self.path_of_sst(memtable.id()),
        )?;
        Ok(Arc::new(sstable))
    }

    fn compact_iterator<I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>>(
        &self,
        mut iter: I,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_ssts = Vec::new();
        let mut memtable = MemTable::create(self.next_sst_id());
        while iter.is_valid() {
            if iter.value().is_empty() {
                iter.next()?;
                continue;
            }
            memtable.put(iter.key().raw_ref(), iter.value())?;
            iter.next()?;
            if memtable.approximate_size() >= self.options.target_sst_size {
                let sstable = self.flush_memtable(memtable)?;
                new_ssts.push(sstable);
                memtable = MemTable::create(self.next_sst_id());
            }
        }
        if memtable.approximate_size() > 0 {
            let sstable = self.flush_memtable(memtable)?;
            new_ssts.push(sstable);
        }

        Ok(new_ssts)
    }
}

fn sstable_merge_iter(
    state: &LsmStorageState,
    sstables: &[usize],
) -> Result<MergeIterator<SsTableIterator>> {
    let mut sstable_iters = Vec::new();
    for id in sstables {
        let sst = state
            .sstables
            .get(id)
            .ok_or(anyhow::anyhow!("sstable not found"))?;
        sstable_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
            sst.clone(),
        )?));
    }
    Ok(MergeIterator::create(sstable_iters))
}

fn sstable_concat_iter(state: &LsmStorageState, sstables: &[usize]) -> Result<SstConcatIterator> {
    let mut sstables_actual = Vec::new();
    for id in sstables {
        let sst = state
            .sstables
            .get(id)
            .ok_or(anyhow::anyhow!("sstable not found"))?;
        sstables_actual.push(sst.clone());
    }
    SstConcatIterator::create_and_seek_to_first(sstables_actual)
}
