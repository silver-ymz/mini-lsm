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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{Key, KeySlice, TS_DEFAULT},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    key_hashes: Vec<u32>,
    block_size: usize,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            key_hashes: Vec::new(),
            block_size,
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
        if self.first_key.is_empty() {
            self.first_key.extend_from_slice(key.key_ref());
            self.last_key.extend_from_slice(key.key_ref());
        }

        if !self.builder.add(key, value) {
            self.split_block();
            let res = self.builder.add(key, value);
            assert!(res);
            self.first_key.extend_from_slice(key.key_ref());
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key.key_ref());
        self.max_ts = self.max_ts.max(key.ts());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.split_block();
        }
        let mut data = self.data;
        let block_meta_offset = data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut data);
        let meta_checksum = crc32fast::hash(&data[block_meta_offset..]);
        data.extend_from_slice(&meta_checksum.to_le_bytes());
        data.extend_from_slice(&block_meta_offset.to_le_bytes());

        data.extend_from_slice(&self.max_ts.to_le_bytes());

        let bloom_offset = data.len();
        let bloom_bits = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bloom_bits);
        bloom.encode(&mut data);
        let bloom_checksum = crc32fast::hash(&data[bloom_offset..]);
        data.extend_from_slice(&bloom_checksum.to_le_bytes());
        data.extend_from_slice(&bloom_offset.to_le_bytes());

        let file = FileObject::create(path.as_ref(), data)?;
        let first_key = self
            .meta
            .first()
            .map_or(Key::<Bytes>::new(), |m| m.first_key.clone());
        let last_key = self
            .meta
            .last()
            .map_or(Key::<Bytes>::new(), |m| m.last_key.clone());

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }

    fn split_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_bytes_with_ts(Bytes::copy_from_slice(&self.first_key), TS_DEFAULT),
            last_key: Key::from_bytes_with_ts(Bytes::copy_from_slice(&self.last_key), TS_DEFAULT),
        };
        self.meta.push(meta);

        let encoded_block = block.encode();
        self.data.extend_from_slice(&encoded_block);
        let checksum = crc32fast::hash(&encoded_block);
        self.data.extend_from_slice(&checksum.to_le_bytes());

        self.first_key.clear();
        self.last_key.clear();
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
