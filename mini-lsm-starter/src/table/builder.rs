#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{Key, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
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
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.extend_from_slice(key.raw_ref());
            self.last_key.extend_from_slice(key.raw_ref());
        }

        if !self.builder.add(key, value) {
            self.split_block();
            let res = self.builder.add(key, value);
            assert!(res);
            self.first_key.extend_from_slice(key.raw_ref());
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key.raw_ref());
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
        data.extend_from_slice(&block_meta_offset.to_le_bytes());

        let file = FileObject::create(path.as_ref(), data)?;
        let first_key = self
            .meta
            .first()
            .map_or(Key::from_bytes(Bytes::new()), |m| m.first_key.clone());
        let last_key = self
            .meta
            .last()
            .map_or(Key::from_bytes(Bytes::new()), |m| m.last_key.clone());
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    fn split_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: Key::from_bytes(Bytes::copy_from_slice(&self.last_key)),
        };
        self.meta.push(meta);
        self.data.extend_from_slice(block.encode().as_ref());
        self.first_key.clear();
        self.last_key.clear();
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
