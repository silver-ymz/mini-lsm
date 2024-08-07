use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![0],
            data: vec![],
            block_size,
            first_key: KeyVec::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let len: u16 = match (key.len() + value.len() + 4).try_into() {
            Ok(len) => len,
            Err(_) => return false,
        };

        let next_offset = self.offsets.last().unwrap() + len;
        if self.offsets.len() != 1
            && (next_offset as usize + 2 * self.offsets.len() > self.block_size)
        {
            return false;
        }

        self.offsets.push(next_offset);
        self.data
            .extend_from_slice(&TryInto::<u16>::try_into(key.len()).unwrap().to_le_bytes());
        self.data.extend_from_slice(key.raw_ref());
        self.data
            .extend_from_slice(&TryInto::<u16>::try_into(value.len()).unwrap().to_le_bytes());
        self.data.extend_from_slice(value);
        if self.offsets.len() == 2 {
            self.first_key.set_from_slice(key);
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.len() == 1
    }

    /// Finalize the block.
    pub fn build(mut self) -> Block {
        self.offsets.pop();
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
