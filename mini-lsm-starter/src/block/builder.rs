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

use super::{Block, SIZEOF_U16};
use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;
use std::convert::AsRef;

///
///
///
/// 关键在于清楚块的结构，一个快的数段中的一个data是一个entry，entry是由键值对组成的
///----------------------------------------------------------------------------------------------------
///|             Data Section             |              Offset Section             |      Extra      |
///----------------------------------------------------------------------------------------------------
///| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
///----------------------------------------------------------------------------------------------------
///
///
/// -----------------------------------------------------------------------
/// |                           Entry #1                            | ... |
/// -----------------------------------------------------------------------
/// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
/// -----------------------------------------------------------------------
///
///
/// Builds a block.
/// 一个block由数据段，偏移量段组成
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    // /// The first key in the block
    // first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
        }
    }

    ///估算当前 BlockBuilder 构建完成后，Block 的编码大小。
    /// 返回值：预估的字节数，用于判断是否达到块大小限制。
    fn estimated_size(&self) -> usize {
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len() * 1
    }
    //SIZEOF_U16(这个字段是用来统计条目数量的，为U16那么大小就是SIZEOFU16)+self.offsets.len()*SIZEOF_U16(偏移量数组大小)+data.len()*1(前半部分是当前builder内部已记录的数据的数量，然后一个数据单位为U8那么字节数为1)

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        //key.len()+value.len()+SIZEOF_U16*3是加入部分的大小，key和value的数据单位是u8，SIZEOFU16()*3是key_len, value_len and offset的大小和
        if self.estimated_size() + key.len() * 1 + value.len() * 1 + SIZEOF_U16 * 3
            > self.block_size
            && self.is_empty() == false
        {
            return false;
        }

        // Add the offset of the data into the offset array.
        // 一个data section是由多个entry组成的，一个entry是按顺序由key_len（2B），key，value_len(2B)，value组成的
        self.offsets.push(self.data.len() as u16);
        // Encode key length.
        self.data.put_u16(key.len() as u16);
        // Encode key content.
        self.data.put(key.raw_ref());
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block shouldnt empty!");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
