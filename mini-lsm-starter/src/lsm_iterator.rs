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

use bytes::Bytes;
use std::ops::Bound;

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{StorageIterator, merge_iterator::MergeIterator},
    mem_table::MemTableIterator,
};
use anyhow::{Ok, Result, bail};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
/// LSM 存储引擎内部迭代器将是一个结合了内存表和 SST 中数据的迭代器
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

//LsmIterator 是 LSM 存储引擎的统一查询接口，整合了：
// MemTable 迭代器（内存表）
// SSTable 迭代器（磁盘表）
// 合并逻辑（处理多版本和删除）
// 通过边界检查和墓碑过滤，提供一致的键值对视图。
pub struct LsmIterator {
    inner: LsmIteratorInner, //LsmIteratorInner由mmt的迭代器和sst的迭代器合成的
    end_bound: Bound<Bytes>, //扫描的结束边界
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
        }; //此时iter可能不合法可能是墓碑
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().raw_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().raw_ref() < key.as_ref(),
        }
        Ok(())
    }

    //将迭代器向前移动，跳过所有被标记为删除的条目（墓碑记录），直到找到有效的非删除条目或到达迭代器末尾
    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
            if !self.inner.is_valid() {
                self.is_valid = false;
                return Ok(());
            }
        }
        ///在 move_to_non_delete 方法中，循环条件是 while self.is_valid() && self.inner.value().is_empty()。当遇到墓碑记录（空值）时，方法会调用 self.inner.next() 来跳过它。但是，如果 SSTable 只有一个条目（如 sst2 中的键 "4" 和空值），调用 next() 会使迭代器超出有效范围，导致 self.inner 无效。但循环会继续在现在无效的迭代器上检查 self.inner.value().is_empty()，从而引发 panic。
        ///
        ///move_to_non_delete 方法需要在使用 next() 后检查 self.inner 是否仍然有效，并相应地更新 self.is_valid
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    //用户在迭代器无效时不应调用 key 、 value 或 next 。同时，如果 next 返回错误，用户也不应再使用该迭代器。
    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
///
/// FusedIterator 是一个包装器结构体，用于为迭代器提供熔断（fused）行为，防止在迭代器无效或出错后继续使用导致未定义行为。
/// 核心特性：
///防止重复调用：在迭代器无效时调用 next() 不会产生意外行为
/// 错误状态锁定：一旦出错，迭代器永久处于错误状态
/// 安全边界：为不安全的底层迭代器提供安全包装
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored || !self.iter.is_valid() {
            panic!("iterator has errored");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.iter.is_valid() {
            panic!("iterator has errored");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // only move when the iterator is valid and not errored
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
