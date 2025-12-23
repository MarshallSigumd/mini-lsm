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

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Ok, Result};
use bytes::Bytes;
use farmhash::fingerprint32;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::{self, MergeIterator};
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mem_table::map_bound;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.在一个线程内只能有一块mmt是活跃的！
    pub memtable: Arc<MemTable>,

    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,

    /// L0 SSTs, from latest to earliest.
    /// 第L0 sst
    pub l0_sstables: Vec<usize>, //sst的id越大表示这个sst越新, 需要优先查询，所以可以用Vec

    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    /// 从level到这一层的sst_id数组, 每一个SST由一个sst_id唯一表示，
    /// 物理上一个SST由N个sstable组成，逻辑上一个是stables由N个ssts_id组成
    pub levels: Vec<(usize, Vec<usize>)>,

    /// SST objects.
    /// sst_id和SST的映射关系
    pub sstables: HashMap<usize, Arc<SsTable>>,
    //查找第 level 层的第 idx 个 SSTable：先在levels找到第level层 （由sst_id组成），然后ssts_id.get(idx)得到sst_id,最后在sstables中得到对应的sst
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: &[u8],
    table_end: &[u8],
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin => {
            return false;
        }
        Bound::Included(key) if key < table_begin => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end => {
            return false;
        }
        Bound::Included(key) if key > table_end => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(search_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= search_key && table_end.raw_ref() >= table_end.raw_ref()
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir(path)?;
        }
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // Search on the current memtable.当前正在活跃的mmt
        if let Some(value) = snapshot.memtable.get(_key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // if fail to search on the current mmt , search on immutable memtables.
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(_key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        //if fail to search on mmt and imm , search on the l0_sst，此时已经加上了bloom和L1层
        //L0层 存在重叠
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };

        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if keep_table(_key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(_key),
                )?));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        //L1层，不重叠
        let mut l1_ssts = Vec::with_capacity(snapshot.levels[0].1.len());
        for table in snapshot.levels[0].1.iter() {
            let table = snapshot.sstables[table].clone();
            if keep_table(_key, &table) {
                l1_ssts.push(table);
            }
        }
        let l1_iter =
            SstConcatIterator::create_and_seek_to_key(l1_ssts, KeySlice::from_slice(_key))?;

        let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;

        if iter.is_valid() && iter.key().raw_ref() == _key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let key = _key.as_ref();
        let value = _value.as_ref();
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(!value.is_empty(), "value cannot be empty");

        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(key, value)?;
            size = guard.memtable.approximate_size();
        }

        self.whether_freeze(size)?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let key = _key.as_ref();
        assert!(!key.is_empty(), "key cannot be empty");

        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(key, b"")?;
            size = guard.memtable.approximate_size();
        }
        self.whether_freeze(size)?;
        Ok(())
    }

    fn whether_freeze(&self, approximate_size: usize) -> Result<()> {
        if approximate_size >= self.options.target_sst_size {
            let slock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&slock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let mmt = Arc::new(MemTable::create(id));

        let old_mmt;
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            old_mmt = std::mem::replace(&mut snapshot.memtable, mmt);
            snapshot.imm_memtables.insert(0, old_mmt);
            *guard = Arc::new(snapshot)
            //逻辑：首先先给写锁，克隆出一个快照，然后对克隆出的快照进行 1.将当前mmt换成下一个mmt并且取出来 2将old_mmt插入到immt的第一个 最后更新guard----其他线程看到要么旧状态，要么新状态
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    /// 不可变 memtable 列表中的最后一个 memtable是最先被刷新的那个
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _lock = self.state_lock.lock();
        let flush_mmt;
        {
            let guard = self.state.read();
            flush_mmt = guard.imm_memtables.last().expect("no imm mmt!").clone();
        }

        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_mmt.flush(&mut builder)?;

        let sst_id = flush_mmt.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let mmt = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mmt.id(), sst_id);
            snapshot.l0_sstables.insert(0, sst_id); //建立映射
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            // Update the snapshot.
            *guard = Arc::new(snapshot);
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// 创建支持范围查询的迭代器，扫描指定键范围内的所有有效键值对
    /// 查询的键值对可能同时出现在mmt和sst中
    /// mmt+L0+L1
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        //获取快照
        //因为 SsTableIterator::create 涉及 I/O 操作且可能较慢，不希望在 state 临界区中执行此操作。因此应该首先读取 state 并克隆 LSM 状态快照的 Arc 。然后应该释放锁。之后遍历所有的 L0 SSTs 并为每个 SST 创建迭代器，再创建一个合并迭代器来获取数据。
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        //创建内存表迭代器
        // mmt层
        let mut mmt_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mmt_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for mmt in snapshot.imm_memtables.iter() {
            mmt_iters.push(Box::new(mmt.scan(lower, upper)));
        }

        let mmt_iter = MergeIterator::create(mmt_iters);

        //创建 SSTable迭代器（磁盘表）
        //L0
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for t_id in snapshot.l0_sstables.iter() {
            let t = snapshot.sstables[t_id].clone();
            if range_overlap(
                lower,
                upper,
                t.first_key().raw_ref(),
                t.last_key().raw_ref(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(t, KeySlice::from_slice(key))?
                    }

                    Bound::Excluded(key) => {
                        let mut iter =
                            SsTableIterator::create_and_seek_to_key(t, KeySlice::from_slice(key))?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(t)?,
                };
                l0_iters.push(Box::new(iter));
            }
        }

        let l0_iter = MergeIterator::create(l0_iters);

        //L1
        let mut l1_ssts = Vec::with_capacity(snapshot.levels[0].1.len());
        for table in snapshot.levels[0].1.iter() {
            let table = snapshot.sstables[table].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().raw_ref(),
                table.last_key().raw_ref(),
            ) {
                l1_ssts.push(table);
            }
        }

        let l1_iter = match lower {
            Bound::Included(key) => {
                SstConcatIterator::create_and_seek_to_key(l1_ssts, KeySlice::from_slice(key))?
            }
            Bound::Excluded(key) => {
                let mut iter =
                    SstConcatIterator::create_and_seek_to_key(l1_ssts, KeySlice::from_slice(key))?;
                if iter.is_valid() && iter.key().raw_ref() == key {
                    iter.next()?;
                }
                iter
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(l1_ssts)?,
        };

        let iter = TwoMergeIterator::create(mmt_iter, l0_iter)?;
        let iter = TwoMergeIterator::create(iter, l1_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
