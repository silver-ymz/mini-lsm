use std::sync::Arc;

use super::Block;
use crate::key::{KeySlice, KeyVec};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 < self.block.offsets.len() {
            self.idx += 1;
            self.seek_to_index(self.idx);
        } else {
            self.key = KeyVec::new();
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut left = 0;
        let mut right = self.block.offsets.len();
        let x = key.raw_ref();
        while left < right {
            let mid = (left + right) / 2;
            let offset = self.block.offsets[mid] as usize;
            let data = &self.block.data[offset..];
            let (key_len, data) = data.split_first_chunk::<2>().unwrap();
            let key_len = u16::from_le_bytes(*key_len);
            let (key, _) = data.split_at(key_len as usize);
            if key >= x {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        if left != self.block.offsets.len() {
            self.seek_to_index(left);
        } else {
            self.key = KeyVec::new();
        }
    }

    fn seek_to_index(&mut self, idx: usize) {
        let offset = self.block.offsets[idx] as usize;
        let data = &self.block.data[offset..];

        let (key_len, data) = data.split_first_chunk::<2>().unwrap();
        let key_len = u16::from_le_bytes(*key_len);
        let (key, data) = data.split_at(key_len as usize);
        self.key.set_from_slice(KeySlice::from_slice(key));

        let (value_len, _) = data.split_first_chunk::<2>().unwrap();
        let value_len = u16::from_le_bytes(*value_len);
        let value_begin = offset + key_len as usize + 4;
        let value_end = value_begin + value_len as usize;
        self.value_range = (value_begin, value_end);

        self.idx = idx;
    }
}
