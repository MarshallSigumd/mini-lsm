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

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;
use crate::block::SIZEOF_U16;
use bytes::Buf;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    /// 就是当前key对应的value在块中的起始位置和结束位置
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
        debug_assert!(!self.key.is_empty(), "iterator invaild!");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to_idx(self.idx)
    }

    /// Seeks to the idx-th key in the block.
    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    /// Seek to the specified position and update the current `key` and `value`
    /// Index update will be handled by caller
    /// 将迭代器定位到 Block 数据中的指定偏移量位置，解析该位置的键值对，并更新迭代器的状态（key 和 value_range）
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        // Since `get_u16()` will automatically move the ptr 2 bytes ahead here,
        // we don't need to manually advance it
        let key_len = entry.get_u16() as usize;
        let key = entry[..key_len].to_vec();
        entry.advance(key_len);
        self.key.clear();
        self.key = KeyVec::from_vec(key);
        let value_len = entry.get_u16() as usize;
        //计算值位置范围
        // offset：条目起始位置
        // + SIZEOF_U16：跳过 key_len 字段（2字节）
        // + key_len：跳过键数据
        // + SIZEOF_U16：跳过 value_len 字段（2字节）
        let value_offset_begin = offset + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut idx_low = 0;
        let mut idx_high = self.block.offsets.len();
        while idx_low < idx_high {
            let mid = idx_low + (idx_high - idx_low) / 2;
            self.seek_to_idx(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => idx_high = mid,
                std::cmp::Ordering::Less => idx_low = mid + 1,
            }
        }
        self.seek_to_idx(idx_low);
    }
}
