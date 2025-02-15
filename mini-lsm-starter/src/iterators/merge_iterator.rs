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

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use super::StorageIterator;
use crate::key::KeySlice;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }
        let current = heap.pop();
        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let mut errs = vec![];
        let mut current = self.current.take().unwrap();
        let key = current.1.key().raw_ref().to_vec();
        if let Err(e) = current.1.next() {
            errs.push(e);
        } else if current.1.is_valid() {
            self.iters.push(current);
        }

        while let Some(mut p) = self.iters.peek_mut() {
            if p.1.key().raw_ref() != key {
                break;
            }
            while p.1.key().raw_ref() == key {
                if let Err(e) = p.1.next() {
                    PeekMut::pop(p);
                    errs.push(e);
                    break;
                }
                if !p.1.is_valid() {
                    PeekMut::pop(p);
                    break;
                }
            }
        }

        self.current = self.iters.pop();
        if errs.is_empty() {
            Ok(())
        } else {
            Err(MergeIteratorError(errs).into())
        }
    }

    fn num_active_iterators(&self) -> usize {
        let mut num = 0;
        if let Some(current) = &self.current {
            num += current.1.num_active_iterators();
        }
        for iter in self.iters.iter() {
            num += iter.1.num_active_iterators();
        }
        num
    }
}

#[derive(Debug)]
struct MergeIteratorError(pub Vec<anyhow::Error>);

impl std::fmt::Display for MergeIteratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MergeIteratorError (size {}):", self.0.len())?;
        for e in &self.0 {
            e.fmt(f)?;
            writeln!(f)?;
        }
        Ok(())
    }
}

impl std::error::Error for MergeIteratorError {}
