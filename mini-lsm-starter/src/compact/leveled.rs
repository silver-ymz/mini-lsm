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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let first_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].first_key())
            .min()
            .unwrap();
        let last_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].last_key())
            .max()
            .unwrap();

        let start_idx = snapshot.levels[in_level - 1]
            .1
            .partition_point(|sst_id| snapshot.sstables[sst_id].last_key() < first_key);
        let end_idx = snapshot.levels[in_level - 1]
            .1
            .partition_point(|sst_id| snapshot.sstables[sst_id].first_key() <= last_key);

        snapshot.levels[in_level - 1].1[start_idx..end_idx].to_vec()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut target_level_size = vec![0; self.options.max_levels];
        let actual_level_size = snapshot
            .levels
            .iter()
            .map(|level| {
                level
                    .1
                    .iter()
                    .map(|sst_id| snapshot.sstables[sst_id].file.size())
                    .sum::<u64>()
            })
            .collect::<Vec<_>>();

        // Compute Target Sizes
        let base_level_size = self.options.base_level_size_mb as u64 * 1024 * 1024;
        if actual_level_size[self.options.max_levels - 1] > base_level_size {
            target_level_size[self.options.max_levels - 1] =
                actual_level_size[self.options.max_levels - 1];
            for i in (0..self.options.max_levels - 1).rev() {
                target_level_size[i] =
                    target_level_size[i + 1] / self.options.level_size_multiplier as u64;
                if target_level_size[i] < base_level_size {
                    break;
                }
            }
        } else {
            target_level_size[self.options.max_levels - 1] = base_level_size;
        }

        // Decide Base Level
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let base_level = target_level_size.partition_point(|&size| size == 0) + 1;
            let base_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: base_level_sst_ids,
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        // Decide Level Priorities
        let mut upper_level = None;
        let mut max_priority = 0.0;
        for level in 1..self.options.max_levels {
            if actual_level_size[level - 1] == 0 {
                continue;
            }
            let priority =
                actual_level_size[level - 1] as f32 / target_level_size[level - 1] as f32;
            if priority <= 1.0 {
                continue;
            }
            if priority > max_priority {
                max_priority = priority;
                upper_level = Some(level);
            }
        }
        let upper_level = upper_level?;

        // Select SST to Compact
        let upper_level_sst_id = *snapshot.levels[upper_level - 1].1.iter().min().unwrap();
        let lower_level = upper_level + 1;
        let lower_level_sst_ids =
            self.find_overlapping_ssts(snapshot, &[upper_level_sst_id], lower_level);

        Some(LeveledCompactionTask {
            upper_level: Some(upper_level),
            upper_level_sst_ids: vec![upper_level_sst_id],
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level: lower_level == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        // Remove SSTs from upper level
        if let Some(upper_level) = task.upper_level {
            snapshot.levels[upper_level - 1]
                .1
                .retain(|sst_id| !task.upper_level_sst_ids.contains(sst_id));
        } else {
            snapshot
                .l0_sstables
                .retain(|sst_id| !task.upper_level_sst_ids.contains(sst_id));
        }

        // Replace SSTs from lower level
        if !in_recovery {
            let first_key = task
                .upper_level_sst_ids
                .iter()
                .map(|sst_id| snapshot.sstables[sst_id].first_key())
                .min()
                .unwrap();
            let last_key = task
                .upper_level_sst_ids
                .iter()
                .map(|sst_id| snapshot.sstables[sst_id].last_key())
                .max()
                .unwrap();
            let first_idx = snapshot.levels[task.lower_level - 1]
                .1
                .partition_point(|sst_id| snapshot.sstables[sst_id].last_key() < first_key);
            let last_idx = snapshot.levels[task.lower_level - 1]
                .1
                .partition_point(|sst_id| snapshot.sstables[sst_id].first_key() <= last_key);
            let mut new_sst_ids = Vec::with_capacity(
                snapshot.levels[task.lower_level - 1].1.len() - (last_idx - first_idx)
                    + output.len(),
            );
            new_sst_ids.extend_from_slice(&snapshot.levels[task.lower_level - 1].1[..first_idx]);
            new_sst_ids.extend_from_slice(output);
            new_sst_ids.extend_from_slice(&snapshot.levels[task.lower_level - 1].1[last_idx..]);
            snapshot.levels[task.lower_level - 1].1 = new_sst_ids;
        } else {
            snapshot.levels[task.lower_level - 1]
                .1
                .retain(|sst_id| !task.lower_level_sst_ids.contains(sst_id));
            snapshot.levels[task.lower_level - 1]
                .1
                .extend_from_slice(output);
        }

        let mut del = task.upper_level_sst_ids.clone();
        del.extend(&task.lower_level_sst_ids);

        (snapshot, del)
    }
}
