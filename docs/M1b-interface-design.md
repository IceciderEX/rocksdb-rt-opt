# AMTV Milestone 1b: 读路径接口设计与对账规范 (M1b-interface-design.md)

## 0. 设计目标与核心约束

在 `memtable_max_range_deletions = 0` 的前提下，M1b 旨在将 M1a 经过对账的 AMTV 增量数据结构接入活跃 MemTable 的读路径。

**核心硬约束**:
1. **禁止在 M1b 手写未经验证的扫线算法**: 范围删除的边界重叠、截断与遮挡语义极为复杂，不得重新发明区间迭代器。
2. **禁止在每个读者处重新全量合并/物化全部墓碑**: 避免退化回原生每读重新碎片化的 CPU 开销与锁争用。
3. **严格保持原生语义**: WAL、Flush、Compaction、SST 写入、Immutable MemTable 保持 100% 原生路径。
4. **全量原生真值差分对账**: 接入读路径前后，必须支持逐 Key、逐 Scan 与全量单体 `FragmentedRangeTombstoneList` 的差分校验。

---

## 1. 活跃 MemTable 单 Range Tombstone Iterator 槽位的消费者梳理

通过对 RocksDB v11.8.0 源码全量审计，活跃 MemTable（`immutable_memtable = false`）创建的范围删除迭代器仅有以下三类消费者：

### 1.1 消费者 A: 点查路径 (`MemTable::Get` 与 `MemTable::MultiGet`)
- **源码位置**:
  - [`db/memtable.cc:1624`](file:///home/wam/grad/rocksdb-v11.8.0/db/memtable.cc#L1624) (`MemTable::Get`)
  - [`db/memtable.cc:1818, 1925`](file:///home/wam/grad/rocksdb-v11.8.0/db/memtable.cc#L1818) (`MemTable::MultiGet`)
- **消费方式**:
  ```cpp
  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
      NewRangeTombstoneIterator(read_options, seq, false /* immutable_memtable */));
  SequenceNumber covering_seq = range_del_iter->MaxCoveringTombstoneSeqnum(user_key);
  ```
- **特征**: **点查只调用 `MaxCoveringTombstoneSeqnum(user_key)`**，完全不需要双向游标扫线（`Seek`, `Next`, `Prev`）。

### 1.2 消费者 B: Scan 迭代器装配路径 (`DBImpl::NewIterator`)
- **源码位置**:
  - [`db/db_impl/db_impl.cc:2607-2621`](file:///home/wam/grad/rocksdb-v11.8.0/db/db_impl/db_impl.cc#L2607-L2621)
  - [`table/merging_iterator.cc:1712, 1758-1768`](file:///home/wam/grad/rocksdb-v11.8.0/table/merging_iterator.cc#L1758-L1768)
- **消费方式**:
  - `DBImpl::NewIterator` 调用 `mem->NewRangeTombstoneIterator(...)`；
  - 包装为 `TruncatedRangeDelIterator` 后，通过 `MergeIteratorBuilder::AddPointAndTombstoneIterator(mem_iter, std::move(mem_tombstone_iter))` 挂载为 `merging_iter` 的第 0 个 child 范围墓碑迭代器。
  - `MergeIteratorBuilder::Finish` 将 `&merge_iter->range_tombstone_iters_.front()` 赋给 `ArenaWrappedDBIter::SetMemtableRangetombstoneIter(...)`。
- **特征**: 活跃 MemTable 在 `MergingIterator` 中恰好占据一个平级槽位。

### 1.3 消费者 C: Scan 迭代器刷新路径 (`ArenaWrappedDBIter::Refresh`)
- **源码位置**:
  - [`db/arena_wrapped_db_iter.cc:290-317`](file:///home/wam/grad/rocksdb-v11.8.0/db/arena_wrapped_db_iter.cc#L290-L317)
- **消费方式**:
  - 当迭代器调用 `Refresh()` 且 SuperVersion 未变时，原地更新 `*memtable_range_tombstone_iter_`：
  ```cpp
  *memtable_range_tombstone_iter_ =
      std::make_unique<TruncatedRangeDelIterator>(
          std::unique_ptr<FragmentedRangeTombstoneIterator>(t), ...);
  ```

---

## 2. 方案比较：扩展多源消费者 vs 构造复合门面

针对消费者的接入方式，存在两种架构路径：

| 维度 | 方案 A: 扩展 MergingIterator 支持多源槽位 | 方案 B: 构造分层复用门面 (Composite Facade) |
| :--- | :--- | :--- |
| **接入点修改范围** | 需修改 `MergeIteratorBuilder`、`MergingIterator` 核心头文件与内部向量结构（影响所有 SST Level） | 仅在 `MemTable` 内部提供符合现有抽象基类的派生门面，不改动 `MergingIterator` |
| **扫线正确性风险** | 需修改 `MergingIterator` 核心归并主循环，风险扩散至整库查询路径 | 保持 `MergingIterator` 原有协议，完全不触及引擎层归并调度 |
| **实现复杂度** | 高，需重新设计 Level 与 MemTable 异构 tombstone 容器 | 中，利用现成的不可变多层列表与原生切片聚合 |
| **推选结论** | **不推荐** | **推荐 (基于原生抽象的安全接入)** |

---

## 3. 推荐方案：无全量重算的分层复用门面设计

为确保在每个读者 Scan 时**绝不重新全量 Fragment 全部墓碑**，采用“不可变层复用 + 极小增量按需物化”：

### 3.1 数据分层与生命周期
1. **Base (基底墓碑层)**:
   - 包含绝大多数墓碑（如数百至数千条）。
   - 是一个只读的 `FragmentedRangeTombstoneList`，由写线程在后台合并时单次构建并原子发布。
   - 所有并发读者共享同一个 `std::shared_ptr<const FragmentedRangeTombstoneList>`，**重用率 100%，物化开销为 0**。
2. **Sealed Delta (冻结增量层)**:
   - 容纳 64 条墓碑。当 OpenDelta 满 64 条时，写线程触发一次局部碎片化生成 `sealed_delta` 并原子发布。
   - 碎片化 64 条墓碑耗时极短，且整个生命周期内只物化一次。所有读者直接共享只读指针。
3. **OpenDelta (活跃追加增量)**:
   - 容纳 $< 64$ 条墓碑。
   - 点查（Get/MultiGet）：直接遍历数组求 max seq，**零分配、零物化**。
   - 范围扫描（Scan）：
     - 若 OpenDelta 为空（即刚完成冻结），门面直接由 Base 与 Sealed Delta 组成，**读者完全零物化**；
     - 若 OpenDelta 非空（最多 63 条）：由 Scan 读者或发布端按需生成这 $\le 63$ 条墓碑的微型 `FragmentedRangeTombstoneList` 并挂载进聚合器。

### 3.2 门面对象设计 (`AMTVRangeTombstoneFacade`)
继承自抽象基类 `RangeTombstoneIterator`（与 `FragmentedRangeTombstoneIterator` 具有相同接口能力）：
```cpp
class AMTVRangeTombstoneFacade : public RangeTombstoneIterator {
 public:
  AMTVRangeTombstoneFacade(std::shared_ptr<const AMTVSnapshot> snapshot,
                           const InternalKeyComparator* icmp,
                           SequenceNumber read_seq);

  // 1. 点查核心接口：委托给经过 M1a 充分对账的 MultiSourceAdapter
  SequenceNumber MaxCoveringTombstoneSeqnum(const Slice& user_key) override;

  // 2. Scan 核心接口：由内部微型聚合器驱动
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void Next() override;
  void Prev() override;
  bool Valid() const override;
  Slice start_key() const override;
  Slice end_key() const override;
  SequenceNumber seq() const override;

 private:
  std::shared_ptr<const AMTVSnapshot> snapshot_;
  AMTVMultiSourceAdapter adapter_;
  // 利用已有的 ReadRangeDelAggregator 进行多源融合，绝不手写区间切分
  ReadRangeDelAggregator internal_agg_;
};
```

---

## 4. 各功能模块的原生语义保持规范

1. **`Get` 语义**:
   - 命中活跃 MemTable 时，调用 `amtv_state_->GetSnapshot()`。
   - 直接调用 `AMTVMultiSourceAdapter::MaxCoveringTombstoneSeqnum(user_key, read_seq)`，完全避开 `reader_mutex` 与全量物化。
2. **`Iterator Refresh` 语义**:
   - `ArenaWrappedDBIter::Refresh()` 检测到 SuperVersion 未变时，仅需获取最新 AMTV 快照重新包装门面即可。若 `publish_epoch` 相同，甚至无需替换底册。
3. **`Immutable MemTable` 语义**:
   - `immutable_memtable = true` 时，调用路径完全不变，直接使用不可变 MemTable 固化的 `fragmented_range_tombstone_list_`。
4. **`FlushJob` 语义**:
   - FlushJob 在 MemTable 冻结为 Immutable 之后执行，强制传递 `immutable_memtable = true`。
   - AMTV 完全不介入 FlushJob，WAL 与 SST 生成保持 100% 原生。
5. **`MemTableList` 语义**:
   - 历史 MemTable 列表中所有元素均为不可变 MemTable，完全使用原生逻辑。

---

## 5. 逐 Key / 逐 Scan 差分对账机制 (Differential Oracle)

在 M1b 中，每一个单元测试与压力审计均启用全量真值对照：
1. **逐 Key 对账**:
   $$\forall \text{Key } k,\quad \text{Facade::MaxCoveringTombstoneSeqnum}(k) \equiv \text{GroundTruthList::MaxCoveringTombstoneSeqnum}(k)$$
2. **逐 Scan 游标步进对账**:
   在每一次 `Seek`, `SeekForPrev`, `Next`, `Prev` 之后：
   - `Facade::Valid() == GroundTruth::Valid()`
   - `Facade::start_key() == GroundTruth::start_key()`
   - `Facade::end_key() == GroundTruth::end_key()`
   - `Facade::seq() == GroundTruth::seq()`
3. **Fail-Fast 容灾**:
   一旦出现任何非零偏差，即刻产生测试断言失败，阻止任何隐式语义漂移。
