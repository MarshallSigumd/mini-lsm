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

use crate::table::KeyBytes;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

// -------------------------------------------------------------------------------------------
// |         Block Section         |          Meta Section         |          Extra          |
// -------------------------------------------------------------------------------------------
// | data block | ... | data block |            metadata           | meta block offset (u32) |
// -------------------------------------------------------------------------------------------

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{Block, BlockBuilder},
    key::KeySlice,
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>, //由多个编码后的数据块（Blocks）组成
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}
//SsTableBuilder包含目前已经encoded的数据，SsTable没有

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.clear();
            self.first_key.extend(key.raw_ref());
        }

        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend(key.raw_ref());
            return;
        }

        self.froze_block();

        // add the key-value pair to the next block
        //当前键同时作为新块的第一个和最后一个键
        assert!(self.builder.add(key, value));
        self.first_key.clear();
        self.first_key.extend(key.raw_ref());
        self.last_key.clear();
        self.last_key.extend(key.raw_ref());
    }

    //SSTBuilder只有一个活跃的block支持插入键值对进行构建, 超出阈值后其将会编码为Block并写入data数组, 这个过程就是SSTBuilder::finish_block函数的功能:

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
        //hint说直接忽视metablock的大小
    }

    ///SST中只能有一块table是活跃的，当超过规定大小的时候就要froze并且创建一个新的table来代替
    /// 关键点：froze_block() 只处理单个块，不会编码所有元数据。
    fn froze_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let frozen_block = builder.build().encode();

        //将frozen_block放入SST的data和meta
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(std::mem::take(&mut self.first_key).into()),
            last_key: KeyBytes::from_bytes(std::mem::take(&mut self.last_key).into()),
        });
        self.data.extend(frozen_block);
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    /// 完成 SSTable 的构建，将内存中的数据和元数据序列化到磁盘文件，并构造 SsTable 实例。
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.froze_block();
        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;

        // Extract first and last keys before moving self.meta
        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();
        // first_key: SSTable 中第一个块的第一个键
        // last_key: SSTable 中最后一个块的最后一个键

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
