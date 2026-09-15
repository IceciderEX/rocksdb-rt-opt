# AMTV M4-P2a: 有界局部范围墓碑视图重构与原生迭代器契约验证实施计划

## 1. 目标与定位

本阶段的**唯一目标**：
> 证明：对固定、有上下界的窗口 $[L, U)$，AMTV Sidecar 与 Open Delta 选出的原始墓碑，经深拷贝、全局排序并直接交给 RocksDB 原生 Fragmenter 与原生截断组件（`TruncatedRangeDelIterator`）后，能生成与全局真值等价的原生范围墓碑迭代器流。

### 核心约束与范围界定
1. **代码修改范围准确界定**：
   - **真实 Scan 消费路径 0 修改**：绝对不修改 `DBIter`、`MergingIterator`、`ArenaWrappedDBIter`、`MemTable::NewRangeTombstoneIteratorInternal`、`RangeDelAggregator` 等真实 Scan 消费路径；
   - **既有写入与归并路径保持原样**：既有 AMTV Run / Sidecar 写入与后台归并路径不在本阶段改动范围内；
   - **测试专用限定**：所有新结构与验证对象均定义在 `db/amtv_local_scan_reference_test.cc` 中，不侵入生产编译与链接。
2. **严禁手写 raw 墓碑窗口裁剪算法与 max-timestamp 重写**：
   - 严禁在送入 Fragmenter 前手工裁剪原始墓碑边界；
   - 严禁手工使用 `AppendKeyWithMaxTimestamp` 重构原始墓碑，严禁将 max timestamp 写入 raw tombstone；
   - 原始墓碑直接以其规范原始表示送入 `RangeTombstone::Serialize()` 并构造原生 `FragmentedRangeTombstoneList`；
   - UDT 的 max-timestamp 规范化必须由原生 `FragmentTombstones` 完成；局部视图不得自创第二套规范化逻辑；
   - 窗口边界裁剪严格复用 RocksDB 源码中原生的 `TruncatedRangeDelIterator` 机制。
3. **禁止性能宣称**：不运行任何性能矩阵、M3b、F1、F2、LongCycle，不宣称端到端 Scan 吞吐或内存收益；
4. **编译与构建隔离**：在独立 Debug (`wt-debug`) 与 Release (`wt-release`) 工作树中完成构建与双构体验证，验证 `-DNDEBUG` 消除断言且无编译警告。

---

## 2. 核心架构与对象规范设计

```
+----------------------------------------------------------------------------------------------------+
|                                    AMTV M4-P2a 修正后架构拓扑                                      |
+----------------------------------------------------------------------------------------------------+

   [WindowSpec]
     |-- lower_bound: std::optional<std::string> (严格区分 nullopt 与 "")
     |-- upper_bound: std::optional<std::string> (严格区分 nullopt 与 "")
     \-- IsBounded(): lower_bound.has_value() && upper_bound.has_value() && (L < U)

   [LocalRangeDelView]
     |
     +---> 检查 WindowSpec 是否有界且合法 (非有界或空窗口 -> 标记 fallback_to_full / is_empty_window)
     |
     +---> [Sealed Runs] ---> Run::sidecar_index().CollectIntersectingIndices(...)
     |
     +---> [Open Delta]  ---> 线性安全扫描 (start < U && end > L)
     |
     +---> 候选相交筛选：严格仅使用 CompareWithoutTimestamp
     |
     +---> 拥有型深拷贝 (OwnedRawRangeTombstone: 唯一规范表示)
     |
     +---> 全局排序 (InternalKeyComparator: 升序 start user key, 降序 seq)
     |
     +---> 构建期完成所有 string / vector 分配并冻结 (Phase 1)
     |
     +---> 原生 VectorIterator (持有已冻结的序列化 keys & values)
     |
     +---> 原生 FragmentedRangeTombstoneList (std::shared_ptr, 原生完成 UDT max-ts 规范化)
     |
     \---> 发布为不可变 State (std::shared_ptr<const State>, Phase 2)

   [LocalRangeDelIteratorHandle]
     |-- state_: std::shared_ptr<const LocalRangeDelView::State>
     |           (持有已发布的不可变 State，FragmentedList、窗口边界与底层数据全程存活)
     \-- guard_: std::unique_ptr<WindowGuard>
                 |
                 +---> 包装原生 TruncatedRangeDelIterator
                 |     (持有原生 FragmentedRangeTombstoneIterator + smallest/largest 边界)
                 +---> MVCC 过滤：设置底层 upper_bound_ = read_seq (原生 SetMaxVisibleSeqAndTimestamp)
                 +---> 正向 Seek(target): 严格要求 L <= target < U
                 +---> 反向 SeekForPrev(target): 严格要求 L <= target <= U (允许 SeekForPrev(U) 哨兵)
                 +---> 越界处理：不调用底层原生迭代器，直接返回无效状态与 Status::InvalidArgument
                 \---> 输出 fragment 严格由原生 TruncatedRangeDelIterator 截断至 L <= start < end <= U
```

### 2.1 `OwnedRawRangeTombstone`：唯一规范表示
- 唯一规范字段定义：
  ```cpp
  struct OwnedRawRangeTombstone {
    std::string start_key;  // 用户起始键（若启用 UDT，已包含原始时间戳字节）
    std::string end_key;    // 用户排他结束键（若启用 UDT，已包含原始时间戳字节）
    SequenceNumber seq = 0; // 范围墓碑序列号
  };
  ```
- **铁律约束**：
  - 不包含独立的 `InternalKey` 缓存；
  - 不把 max timestamp 写入 raw tombstone；
  - 不手工裁剪边界后重建 raw tombstone；
  - 序列化严格调用原生 `RangeTombstone(start_key, end_key, seq).Serialize()`。

### 2.2 `LocalRangeDelView::State`：不可变发布与全生命周期安全
- **阶段一（构建期 Staging）**：
  - 选出与窗口相交的原始墓碑，存入 `std::vector<OwnedRawRangeTombstone> raw_candidates`；
  - 按照 `InternalKeyComparator` 进行排序（`start user key` 升序，`seq` 降序）；
  - 逐项序列化存入 `std::vector<std::string> serialized_keys` 与 `std::vector<std::string> serialized_values`；
  - 构造 `smallest_ikey` 与 `largest_ikey`（作为 `TruncatedRangeDelIterator` 的原生截断边界，若启用 UDT 则正确追加 max timestamp）；
  - 构造原生 `VectorIterator` 并以此构造原生 `FragmentedRangeTombstoneList`；
  - 完成所有容器与字符串的分配。
- **阶段二（发布期 Immutable State）**：
  - 封装为 `std::shared_ptr<const State>`；
  - 发布后完全不可变，严禁修改或扩容；
  - `InternalKeyComparator` 作为借用指针被记录，测试中显式证明其生命周期覆盖所有 Handle。

### 2.3 复用原生 `TruncatedRangeDelIterator` 截断
- 放弃手写任何 raw 重编码或扫描线截断；
- 使用 RocksDB 原生组件 `TruncatedRangeDelIterator`（[db/range_del_aggregator.h:31-105](file:///home/wam/grad/rocksdb-v11.8.0/db/range_del_aggregator.h#L31-L105)）；
- 将原生 `FragmentedRangeTombstoneIterator` 与 `smallest_ikey` / `largest_ikey` 传入 `TruncatedRangeDelIterator`；
- 原生截断机制自动保证：
  - `start_key()` 截断至 $L$；
  - `end_key()` 截断至 $U$；
  - `Valid()` 过滤掉 $[L, U)$ 之外的切片；
  - `timestamp()` 维持正确提取。

### 2.4 MVCC 过滤与 WindowGuard 边界契约
- **MVCC 过滤**：构建期保留所有候选（包括未来墓碑）；`read_seq` 过滤由底层的 `FragmentedRangeTombstoneIterator::SetMaxVisibleSeqAndTimestamp()` 负责；
- **Forward `Seek(target)`**：仅允许 $L \le target < U$；越界则置 `valid_ = false` 并设置 `Status::InvalidArgument`，不调用底层原生迭代器；
- **Reverse `SeekForPrev(target)`**：允许 $L \le target \le U$；**必须允许 $target = U$ 作为排他上界哨兵**（与 `DBIter::SeekToLast()` 原生契约对齐）；越界则置 `valid_ = false` 并设置 `Status::InvalidArgument`；
- **`SeekToFirst()`**：委托底层 `SeekToFirst()`（`TruncatedRangeDelIterator` 自动定位于 $L$）；
- **`SeekToLast()`**：委托底层 `SeekToLast()`（`TruncatedRangeDelIterator` 自动定位于 $U$ 之前的最后一个片段）。

---

## 3. 四方等价差分测试方案（Four-Way Differential Verification）

对每个场景与窗口，比对：
- **A. 全量原始墓碑 + 原生 Fragmenter + 原生 TruncatedRangeDelIterator**（`canonical_truth`）；
- **B. AMTV Sidecar / Open Delta 候选 + 原生 Fragmenter + 原生 TruncatedRangeDelIterator**（`local_view`）；
- **C. 独立逐点 MVCC 覆盖 Oracle**：逐 key 检验 `max_covering_tombstone_seq`、`point_deleted_by_tombstone`、`final_point_visible`；
- **D. 正向与反向遍历流对称性**：验证 `Next()` 流与 `Prev()` 流的位级对称一致性。

### 测试用例拓扑全覆盖规划
1. **`P2a_WindowSpecAndBoundaryDistinction`**：`nullopt` vs `""`，单侧有界、空窗口与倒置窗口 fallback；
2. **`P2a_WindowGuardSeekRejection`**：Forward Seek 拒绝 $< L$ 与 $\ge U$，SeekForPrev 拒绝 $< L$ 与 $> U$，但**显式允许 $SeekForPrev(U)$**；
3. **`P2a_IteratorHandleLifetimeAfterViewAndSnapshotDestruction`**：硬验证 View 与 Snapshot 提前销毁后 Handle 的完全独立存活；
4. **`P2a_EqualStartDifferentEndAndSequence`**：相同 start、不同 end 与 sequence 的原生切分与截断；
5. **`P2a_LongTombstonesCrossingBounds_NestedAdjacentCrossing`**：左跨界长墓碑、右跨界、嵌套、相邻与交叉墓碑；
6. **`P2a_MultiLevelRuns_OpenDelta_L0_L1_L2_L3`**：多层 Sealed Runs 与 Open Delta 候选提取；
7. **`P2a_PutResurrectionAndFutureTombstoneMVCC`**：Put 复活与未来墓碑在 iterator 层的原生 `read_seq` 过滤；
8. **`P2a_UserDefinedTimestamp_WithAndWithoutTsUpperBound`**：UDT 环境下原生 max-ts 规范化与原生截断；
9. **`P2a_EmptyUserKeyAsValidBoundary`**：$L = ""$ 作为真实边界及 8 字节空键墓碑重构；
10. **`P2a_OldVsNewSnapshotEquivalence`**：后台 Run 归并前与归并后局部重构流位级同构性验证。

---

## 4. 验收闸门

1. **逐字段流一致性**：所有局部输出流与全量原生流的 `start_key`, `end_key`, `seq`, `timestamp` 100% 一致；
2. **四方逐点一致**：Pointwise Oracle 与局部视图结果逐点一致；
3. **双构建通过**：Debug (`wt-debug`) 与 Release (`wt-release`) 100% 编译通过且无警告；
4. **全量基线测试回归**：
   - 既有 30 项局部视图测试 100% 通过；
   - `amtv_scan_oracle_test`（15 项）100% 通过；
   - `amtv_probe_test`（3 项）100% 通过；
5. **提交与推送**：工作树干净，提交并推送至 `rt-opt/main`；
6. **停止点**：完成后立即停止，提交实施报告与代码审阅材料，绝不进入真实 Scan 接入或性能测试。
