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

use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

struct WalInner {
    file: File,
    buf: Vec<u8>,
}

pub struct Wal {
    inner: Arc<Mutex<WalInner>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(WalInner {
                file,
                buf: Vec::new(),
            })),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = File::open(&path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut read_slice = &buf[..];
        while !read_slice.is_empty() {
            let (batch_size, remain_data) = read_slice.split_first_chunk::<4>().unwrap();
            let batch_size = u32::from_le_bytes(*batch_size) as usize;
            let (body, remain_data) = remain_data.split_at(batch_size);
            let (checksum_data, remain_data) = remain_data.split_first_chunk::<4>().unwrap();
            read_slice = remain_data;

            let checksum_stored = u32::from_le_bytes(*checksum_data);
            let checksum_actual = crc32fast::hash(body);
            if checksum_stored != checksum_actual {
                return Err(anyhow::anyhow!("Checksum mismatch in WAL"));
            }

            let mut body_slice = body;
            while !body_slice.is_empty() {
                let (key_length_data, remain_data) = body_slice.split_first_chunk::<2>().unwrap();
                let key_length = u16::from_le_bytes(*key_length_data) as usize;
                assert!(key_length > 0);
                let (key_data, remain_data) = remain_data.split_at(key_length);
                let (ts, remain_data) = remain_data.split_first_chunk::<8>().unwrap();
                let ts = u64::from_le_bytes(*ts);
                let key = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key_data), ts);

                let (value_length_data, remain_data) =
                    remain_data.split_first_chunk::<2>().unwrap();
                let value_length = u16::from_le_bytes(*value_length_data) as usize;
                let (value_data, remain_data) = remain_data.split_at(value_length);
                let value = Bytes::copy_from_slice(value_data);

                skiplist.insert(key, value);
                body_slice = remain_data;
            }
        }

        let mut file = File::options().write(true).open(path)?;
        file.seek(std::io::SeekFrom::End(0))?;

        Ok(Self {
            inner: Arc::new(Mutex::new(WalInner {
                file,
                buf: Vec::new(),
            })),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut inner = self.inner.lock();
        let inner = &mut *inner;
        inner.buf.write_all(&[0; 4])?; // Placeholder for batch_size

        for (key, val) in data {
            inner.buf.write_all(&(key.key_len() as u16).to_le_bytes())?;
            inner.buf.write_all(key.key_ref())?;
            inner.buf.write_all(&key.ts().to_le_bytes())?;
            inner.buf.write_all(&(val.len() as u16).to_le_bytes())?;
            inner.buf.write_all(val)?;
        }

        let batch_size = inner.buf.len() - 4;
        inner.buf[0..4].copy_from_slice(&(batch_size as u32).to_le_bytes());
        let checksum = crc32fast::hash(&inner.buf[4..]);
        inner.buf.write_all(&checksum.to_le_bytes())?;

        inner.file.write_all(&inner.buf)?;
        inner.buf.clear();

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.file.flush()?;
        inner.file.sync_all()?;
        Ok(())
    }
}
