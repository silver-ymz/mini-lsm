#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

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
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
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
        let state = self.state.read();
        match task {
            CompactionTask::Leveled(_leveled_compaction_task) => todo!(),
            CompactionTask::Tiered(_tiered_compaction_task) => todo!(),
            CompactionTask::Simple(_simple_leveled_compaction_task) => todo!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_sstable_iters = Vec::new();
                for id in l0_sstables {
                    let sst = state
                        .sstables
                        .get(id)
                        .ok_or(anyhow::anyhow!("sstable not found"))?;
                    l0_sstable_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        sst.clone(),
                    )?));
                }
                let mut l1_sstables_actual = Vec::new();
                for id in l1_sstables {
                    let sst = state
                        .sstables
                        .get(id)
                        .ok_or(anyhow::anyhow!("sstable not found"))?;
                    l1_sstables_actual.push(sst.clone());
                }
                let mut total_iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_sstable_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_sstables_actual)?,
                )?;

                let flush_memtable = |memtable: MemTable| -> Result<SsTable> {
                    let mut sst_builder = SsTableBuilder::new(self.options.block_size);
                    memtable.flush(&mut sst_builder)?;
                    sst_builder.build(
                        memtable.id(),
                        Some(self.block_cache.clone()),
                        self.path_of_sst(memtable.id()),
                    )
                };

                let mut new_ssts = Vec::new();
                let mut memtable = MemTable::create(self.next_sst_id());
                while total_iter.is_valid() {
                    if total_iter.value().is_empty() {
                        total_iter.next()?;
                        continue;
                    }
                    memtable.put(total_iter.key().raw_ref(), total_iter.value())?;
                    total_iter.next()?;
                    if memtable.approximate_size() >= self.options.target_sst_size {
                        let sstable = flush_memtable(memtable)?;
                        new_ssts.push(Arc::new(sstable));
                        memtable = MemTable::create(self.next_sst_id());
                    }
                }
                if memtable.approximate_size() > 0 {
                    let sstable = flush_memtable(memtable)?;
                    new_ssts.push(Arc::new(sstable));
                }

                Ok(new_ssts)
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
        unimplemented!()
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
}
