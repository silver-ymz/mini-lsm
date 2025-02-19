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

use std::{cmp::Reverse, collections::HashSet};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // Precondition
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Triggered by Space Amplification Ratio
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        let engine_size = snapshot
            .levels
            .iter()
            .map(|(_, ssts)| ssts.len())
            .sum::<usize>()
            - last_level_size;

        if engine_size * 100 / last_level_size >= self.options.max_size_amplification_percent {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Triggered by Size Ratio
        let start = self.options.min_merge_width;
        let mut previous_levels_size = snapshot
            .levels
            .iter()
            .take(start)
            .map(|(_, ssts)| ssts.len())
            .sum::<usize>();
        for i in start..snapshot.levels.len() {
            let current_level_size = snapshot.levels[i].1.len();
            if current_level_size * 100 / previous_levels_size >= 100 + self.options.size_ratio {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.iter().take(i).cloned().collect(),
                    bottom_tier_included: i == snapshot.levels.len() - 1,
                });
            }
            previous_levels_size += current_level_size;
        }

        let max_width = self.options.max_merge_width.unwrap_or(usize::MAX);
        Some(TieredCompactionTask {
            tiers: snapshot.levels.iter().take(max_width).cloned().collect(),
            bottom_tier_included: max_width >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut to_remove = Vec::new();

        // Remove the compacted levels
        let merged_levels = task.tiers.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
        snapshot.levels.retain(|(level, files)| {
            if merged_levels.contains(level) {
                to_remove.extend(files);
                false
            } else {
                true
            }
        });

        // Insert the new levels
        let pos = snapshot
            .levels
            .binary_search_by_key(&Reverse(task.tiers[0].0), |(level, _)| Reverse(*level))
            .unwrap_err();
        snapshot.levels.insert(pos, (output[0], output.to_vec()));

        (snapshot, to_remove)
    }
}
