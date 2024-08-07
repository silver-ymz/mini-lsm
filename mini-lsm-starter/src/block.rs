mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let len = self.data.len() + 2 * self.offsets.len() + 2;
        let mut data = BytesMut::with_capacity(len);
        data.extend_from_slice(&self.data);
        for offset in &self.offsets {
            data.extend_from_slice(&offset.to_le_bytes());
        }
        data.extend_from_slice(
            &TryInto::<u16>::try_into(self.offsets.len())
                .unwrap()
                .to_le_bytes(),
        );
        data.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let (data, len) = data.split_last_chunk::<2>().unwrap();
        let len = u16::from_le_bytes(*len) as usize;
        let (data, offsets_slice) = data.split_at(data.len() - len * 2);
        let mut offsets = Vec::with_capacity(len);
        for offset in offsets_slice.chunks(2) {
            offsets.push(u16::from_le_bytes(offset.try_into().unwrap()));
        }
        let data = data.to_vec();
        Self { data, offsets }
    }
}
