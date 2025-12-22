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

pub(crate) mod bloom;
mod builder;
mod iterator;

use anyhow::Result;
use anyhow::anyhow;
pub use builder::SsTableBuilder;
use bytes::Buf;
use bytes::BufMut;
use clap::builder::NonEmptyStringValueParser;
pub use iterator::SsTableIterator;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

//元数据：描述属性的数据单元，本身不包含具体data

#[derive(Clone, Debug, PartialEq, Eq)]
/// STT是由Block段，Meta段和Meat段的偏移量段组成
/// 块元数据 BlockMeta ，其中包括每个块的起始/结束键以及每个块的偏移量
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
    //SSTable 是有序的。因此，文件里的第一个 Key 一定是全文件最小的，最后一个 Key 一定是全文件最大的。
}

//  Blockmeta：offset+f_key_len+f_key+l_key+l_key_value

// -------------------------------------------------------------------------------------------
// |         Block Section         |          Meta Section         |          Extra          |
// -------------------------------------------------------------------------------------------
// | data block | ... | data block |            metadata           | meta block offset (u32) |
// -------------------------------------------------------------------------------------------

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// 将数据从SST中读到buf
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = 0;
        for meta in block_meta {
            // The size of offset
            estimated_size += std::mem::size_of::<u32>();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.first_key.len();
            // The size of key length
            estimated_size += std::mem::size_of::<u16>();
            // The size of actual key
            estimated_size += meta.last_key.len();
        }
        // Reserve the space to improve performance, especially when the size of incoming data is
        // large
        buf.reserve(estimated_size);
        let original_len = buf.len();
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    /// 将buf中的数据写到SST
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            block_meta.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);
//file+file_size

impl FileObject {
    //返回的是字节流，要得到数据的话还要decode
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.所有的数据都放在file中了（字节流）
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.就是meta段开始的地方即偏移量
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    // /// The maximum timestamp stored in this SST, implemented in week 3.
    // max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    ///
    /// SSTable 文件结构：|     Data Blocks   |   Block Metadata  |  Meta Offset (u32)|
    /// Data Blocks: 实际的键值对数据块
    /// Block Metadata: 每个数据块的元信息
    /// Meta Offset: 元数据部分的起始偏移量（4字节，存储在文件末尾）
    /// -----------------------------------------------------------------------------------------------------
    // |         Block Section         |                            Meta Section                           |
    // -----------------------------------------------------------------------------------------------------
    // | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset |
    // |                               |  varlen  |         u32       |    varlen    |        u32          |
    // -----------------------------------------------------------------------------------------------------

    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();

        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64; //bloom的在SST中的起始位l;'\

        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        let bloom_filter = Bloom::decode(&raw_bloom)?;

        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        //len - 4: 文件末尾前4字节存储元数据偏移量，这里的4就是1个u32等于4个byte
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        //get_u32(): 读取大端序的32位无符号整数
        let raw_meta = file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..]);

        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            bloom: Some(bloom_filter),
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            // max_ts: 0,
        }
    }

    /// Read a block from the disk.
    /// 此时block随之更新
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_data = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk, with block cache.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            //缓存不存在
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    // pub fn max_ts(&self) -> u64 {
    //     self.max_ts
    // }
}
