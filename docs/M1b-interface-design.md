# AMTV Milestone 1b-GetOnly & Milestone 1c-ScanArchitecture 架构与接口设计规范

## 0. 阶段拆分与边界

按照用户指令，AMTV 后续演进严格拆分为两个阶段：
1. **`M1b-GetOnly`**：仅在活跃 MemTable 的 `Get/MultiGet` 中使用已通过 Differential Oracle 验证的 `AMTVMultiSourceAdapter`，绕过 `reader_mutex` 与重复物化；**Scan 路径 100% 保持原生**。
2. **`M1c-ScanArchitecture`**：确定范围扫描路径的终态架构设计与逐 Scan 差分对账规范，**在设计与最小探针审核通过前不编写任何生产 Scan 读路径代码**。

---

## 1. M1b-GetOnly: 活跃 MemTable 点查旁路接入

### 1.1 核心约束
1. **Scan 路径保持原生**:
   - `MemTable::NewRangeTombstoneIteratorInternal`、`MergingIterator`、`DBIter`、`ArenaWrappedDBIter::Refresh` 均保持原生路径，不做任何改动。
2. **DeleteRange 不得跳过原生缓存失效**:
   - 写线程在 `MemTable::Add` 写入 `DeleteRange` 时，必须照常执行 `AtomicSharedPtrStore(local_cache_ref_ptr, ...)` 触发 `cached_range_tombstone_` 失效，确保后续原生 Scan 读者仍能感知最新写入。
3. **定位与宣称**:
   - AMTV 在 M1b 中严格定位为“点查旁路优化”，不宣称已解决范围扫描。
4. **内存生命周期铁律**:
   - **“AMTV 影子条目必须深拷贝边界键，以避免适配器自身的 Slice 生命周期错误。”** 原生 `RangeTombstone` 仅存引用 Slice，AMTV 的 `OpenDeltaEntry` 必须深拷贝 `start_user_key` 与 `end_key`。

### 1.2 接入点设计
- **`MemTable::Get` ([`db/memtable.cc:1622`](file:///home/wam/grad/rocksdb-v11.8.0/db/memtable.cc#L1622))**:
  ```cpp
  if (!no_range_del) {
    if (enable_amtv_ && !immutable_memtable && amtv_state_ != nullptr) {
      auto snap = amtv_state_->GetSnapshot();
      AMTVMultiSourceAdapter adapter(snap, &comparator_.comparator);
      max_covering_tombstone_seq =
          adapter.MaxCoveringTombstoneSeqnum(lkey.user_key(), read_seq, &range_del_timestamp);

  #ifndef NDEBUG
      // 双轨差分断言（Test/Debug 模式）：验证 AMTV 判定与原生物化判定完全一致
      std::unique_ptr<FragmentedRangeTombstoneIterator> native_iter(
          NewRangeTombstoneIterator(read_opts, read_seq, immutable_memtable));
      SequenceNumber native_seq =
          native_iter ? native_iter->MaxCoveringTombstoneSeqnum(lkey.user_key()) : 0;
      assert(max_covering_tombstone_seq == native_seq);
  #endif
    } else {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          NewRangeTombstoneIterator(read_opts, read_seq, immutable_memtable));
      max_covering_tombstone_seq =
          range_del_iter->MaxCoveringTombstoneSeqnum(lkey.user_key());
      if (moptions.user_comparator->timestamp_size() > 0) {
        range_del_timestamp.assign(range_del_iter->timestamp().data(),
                                   range_del_iter->timestamp().size());
      }
    }
  }
  ```
- **`MemTable::MultiGet` ([`db/memtable.cc:1818, 1925`](file:///home/wam/grad/rocksdb-v11.8.0/db/memtable.cc#L1818))**:
  - 获取一次 `amtv_state_->GetSnapshot()`，对批次内的 Key 连续复用 `AMTVMultiSourceAdapter` 求覆盖序列号，消除批次内重复物化与反复进入 `reader_mutex`。

### 1.3 并发正确性测试设计
在 `db/amtv_test.cc` 中实现 `AMTVTest.Concurrent8Readers1Writer_GetOnly`：
- **1 个写线程**：持续执行 5,000 次交替的 `DeleteRange` 与 `Put`。
- **8 个并发读线程**：持续并发执行 `db->Get(ReadOptions(), key)`。
- **校验点**：
  1. 线性化：读者一旦观察到 $read\_seq \ge S_{del}$，绝不能漏掉该范围删除；
  2. 一致性：AMTV 判定与原生判定结果完全相同；
  3. 线程安全：TSAN/ASan 洁净，无 Data Race、无 UAF。

---

## 2. M1c-ScanArchitecture: 范围扫描路径架构路线选型

### 2.1 路线 A vs 路线 B 深度论证

用户要求在以下两条路线中选一条，并说明正确性证明和代价：
- **路线 A**: 扩展 `TruncatedRangeDelIterator` 或其上游消费者，使一个活跃 MemTable 可向 Scan 聚合器提供多个原生范围墓碑来源；复用已有聚合语义。
- **路线 B**: 设计可持久增量更新的数据结构，使每个 AMTV snapshot 始终产出一个全局 Fragment 后的单一视图；禁止在每次 Scan 时对 Base + Delta 做全量 Fragment。

#### 论证矩阵：

| 评估维度 | 路线 A (推荐: 扩展 TruncatedRangeDelIterator 接受多源聚合) | 路线 B (自研全局增量动态打碎数据结构) |
| :--- | :--- | :--- |
| **算法正确性保证** | **极高**。每个子层均为原生 `FragmentedRangeTombstoneIterator`，完全复用 RocksDB 官方经过十几年检验的区间切分逻辑。 | **极低/高风险**。必须重新发明带有 sequence、timestamp 与区间重叠判定的并发可持久化红黑/线段树，极易引入隐蔽 Corner-case。 |
| **写端开销** | **零惩罚**。OpenDelta 仅简单追加，Sealed Delta 批量物化一次，Base 异步合并。 | **巨大写放大**。每条 `DeleteRange` 必须在写临界区执行区间几何切割与分裂重平衡，严重压垮写性能。 |
| **读端全量重算** | **严格杜绝**。Base（数千条）与 Sealed Delta（64条）全生命周期预打碎复用指针，读者仅按需打碎 OpenDelta（$\le 63$ 条）。 | **严格杜绝**。每个快照天然是全局单体视图。 |
| **手写扫线风险** | **零风险**。不手写任何区间扫线代码，全部调用原生迭代器。 | **高风险**。直接违背“禁止在 M1b/M1c 手写未经验证区间算法”的禁令。 |

#### **路线选型结论**: 坚定选择 **路线 A**。

### 2.2 路线 A 正确性数学证明
设读者在读序列号 $S_r$ 下扫描点键 $x$。
- 全量真实墓碑集合为 $T = T_{base} \cup T_{sealed} \cup T_{open}$。
- 原生单体打碎列表判定的覆盖序列号为：
  $$seq_{native}(x) = \max \{ t.seq \mid t \in T,\ t.start \le x < t.end,\ t.seq \le S_r \}$$
- 路线 A 中，`TruncatedRangeDelIterator` 内置持有来自同一 MemTable 的 3 个原生子迭代器：$I_{base}, I_{sealed}, I_{open}$。
- 路线 A 判定的覆盖序列号为：
  $$seq_{AMTV}(x) = \max \Big( seq(I_{base}, x),\ seq(I_{sealed}, x),\ seq(I_{open}, x) \Big)$$
- 根据集合并集的最大值恒等式（$\max$ 运算符的结合律与交换律）：
  $$\max_{t \in T_1 \cup T_2}(f(t)) \equiv \max \left( \max_{t \in T_1}(f(t)),\ \max_{t \in T_2}(f(t)) \right)$$
- 恒有：
  $$\mathbf{seq_{AMTV}(x) \equiv seq_{native}(x)}$$
- **证毕**: 路线 A 的多源聚合判定在全键空间与所有序列号下与原生全局打碎单体列表严格等价。

### 2.3 路线 A 性能代价分析
1. **CPU 开销**:
   `TruncatedRangeDelIterator::ShouldDelete()` 对 $K \le 3$ 个内部迭代器取 $\max$。因为 $K$ 极小（最多 3 个），其单次比较耗时在几纳秒量级，相比于重新全量排序打碎数千条墓碑，CPU 开销微乎其微。
2. **内存开销**:
   仅需将 `TruncatedRangeDelIterator` 内部的单指针 `iter_` 扩展为 `std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>`（最多 3 个元素，占用几十字节栈/堆空间）。

### 2.4 逐 Scan Differential Oracle 规范
在 M1c 阶段，必须实现真实 `MergingIterator` 消费链的逐 Scan 对账测试：
```text
AMTV Scan 可见键序列
  ≡
原生全量 Fragment 后的 Scan 可见键序列
  ≡
原生 DB Iterator 可见键序列
```
- **覆盖用例场景**:
  1. 跨层区间重叠与完全嵌套；
  2. 首尾相邻边界开闭区间；
  3. DeleteRange 覆盖区内 Put 复活；
  4. Snapshot 隔离可见性；
  5. Iterator Refresh 活跃代际切换；
  6. MemTable 转为 Immutable 过程中的持续迭代。

---

## 3. 模块原生语义保持汇总表

| 模块 / 操作 | M1b-GetOnly 语义保持方式 | M1c-ScanArchitecture 语义保持方式 |
| :--- | :--- | :--- |
| **`Get / MultiGet`** | 使用 `AMTVMultiSourceAdapter`，零锁、零重新碎片化 | 继承 M1b 机制 |
| **`DBIter::NewIterator`** | 100% 保持原生不变 | 挂载路线 A 多源 `TruncatedRangeDelIterator` |
| **`ArenaWrappedDBIter::Refresh`** | 100% 保持原生不变 | 刷新活跃 MemTable 的多源槽位 |
| **`Immutable MemTable`** | 显式传 `immutable=true`，完全走原生打碎列表 | 显式传 `immutable=true`，完全走原生打碎列表 |
| **`FlushJob`** | 完全走原生打碎列表，AMTV 不介入 | 完全走原生打碎列表，AMTV 不介入 |
| **`MemTableList`** | 均为不可变 MemTable，100% 原生 | 均为不可变 MemTable，100% 原生 |
| **`DeleteRange 缓存失效`** | **保留原生失效** (`cached_range_tombstone_`) | 视 Scan 完全迁移状态平滑解耦 |
