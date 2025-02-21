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
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);
        let mut length_buf = [0u8; std::mem::size_of::<usize>()];
        loop {
            match reader.read_exact(&mut length_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let length = usize::from_le_bytes(length_buf);
            assert!(length > 0);
            let mut key = vec![0u8; length];
            reader.read_exact(&mut key)?;
            let key = Bytes::from(key);

            reader.read_exact(&mut length_buf)?;
            let length = usize::from_le_bytes(length_buf);
            let mut value = vec![0u8; length];
            reader.read_exact(&mut value)?;
            let value = Bytes::from(value);

            skiplist.insert(key, value);
        }

        let file = File::options().append(true).open(path)?;
        let file = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self { file })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        file.write_all(&key.len().to_le_bytes())?;
        file.write_all(key)?;
        file.write_all(&value.len().to_le_bytes())?;
        file.write_all(value)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;
        Ok(())
    }
}
