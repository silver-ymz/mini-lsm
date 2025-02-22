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

use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = Arc::new(Mutex::new(File::create(path)?));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = BufReader::new(File::open(&path)?);
        let mut buf = [0; 8];
        let mut data_buf = Vec::new();
        let mut records = Vec::new();

        loop {
            match file.read_exact(&mut buf) {
                Ok(()) => (),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = usize::from_le_bytes(buf);
            assert!(len > 0);
            data_buf.resize(len, 0);
            file.read_exact(&mut data_buf)?;
            file.read_exact(&mut buf[0..4])?;
            let checksum_stored = u32::from_le_bytes(buf[0..4].try_into().unwrap());
            let checksum_actual = crc32fast::hash(&data_buf);

            if checksum_stored != checksum_actual {
                return Err(anyhow::anyhow!("Checksum mismatch in manifest file"));
            }

            let record = serde_json::from_slice(&data_buf)?;
            records.push(record);

            data_buf.clear();
        }

        let file = File::options().append(true).open(path)?;
        let file = Arc::new(Mutex::new(file));
        Ok((Self { file }, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut buf = vec![0; 8];
        serde_json::to_writer(&mut buf, &record)?;

        let len = buf.len() - 8;
        buf[0..8].copy_from_slice(&len.to_le_bytes());

        let checksum = crc32fast::hash(&buf[8..]);
        buf.extend_from_slice(&checksum.to_le_bytes());

        let mut file = self.file.lock();
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
