# M1c-P0：AMTV 多 Run Scan 语义与接口可行性审计报告

- **阶段标识**：M1c-P0 (AMTV Multi-Run Scan Semantics and Interface Feasibility Gate)
- **阶段性质**：架构与语义正确性闸门（Architecture & Correctness Gate）
- **环境版本**：RocksDB v11.8.0 / Linux x86_64
- **执行原则**：
  1. 严格禁止修改真实 DB Scan 读路径（`DBImpl::NewIterator`、`MemTable::NewRangeTombstoneIteratorInternal`、`MergingIterator`、`DBIter` 原生读路径 100% 保持原样）；
  2. 严格禁止执行任何新的性能矩阵、Release N=5、F1/F2 或 24GiB 实验；
  3. 严格禁止将本测试 Oracle 或参考合并器用于生产性能宣传；
  4. 采用仅测试用差分 Oracle 与 Native Ground Truth 进行严格数学与位级等价性验证。

---

## 1. 阶段目标与核心认知纠正

### 1.1 核心问题回答
本阶段旨在回答一个根本性的架构问题：
> **对于一个 AMTV Snapshot 中的 Base、多个 sealed run 和 Open Delta，是否能在不遗漏 MVCC、时间戳、边界或 Put 复活语义的前提下，构造供 Scan 消费的全局有序、互不重叠范围墓碑分片流？**

**审计结论**：
1. **数学与语义可行性（Mathematical & Semantic Feasibility）**：**可行 (FEASIBLE)**。
   通过本阶段实现的差分 Oracle（`AMTVScanOracle`）与参考多路扫描线合并器（`AMTVMultiRunScanIterator`），在 15 类严苛语义矩阵及 1,000 组随机多层区间集合差分验证中，证明了通过对全量边界事件（Boundary Events）进行排序与状态投影，能够产生完全满足 RocksDB Scan 契约的、全局单调递增、相邻互斥的范围墓碑分片流，且离散覆盖、`ShouldDelete` 判定与 DB Iterator 实际可见 Key/Value 序列与原生 Ground Truth 达到 100% 位级一致。
2. **生产接口直接接入可行性（Production Interface Feasibility）**：**不可行 (BLOCKED)**。
   基于 RocksDB v11.8.0 真实源码审计，当前生产 Scan 接口链路存在多处不可逾越的非虚类型强绑定与单槽位架构约束。

### 1.2 核心认知纠正：点查结合律不可外推为 Scan 分片流
- **点查（Point Get）**：属于**零维点查询**。点查只需判定目标点 $k$ 是否被覆盖，其最大 sequence 判定在各层级之间满足**局部结合律**：
  $$\text{CoveringSeq}(k) = \max(s_{\text{base}}(k), s_{\text{sealed\_0}}(k), \dots, s_{\text{open}}(k))$$
  各层级独立查找、分层取最大值即可得到精确结果，完全无需跨层几何交互。
- **范围扫描（Scan）**：属于**一维区间拓扑重叠与分片流式扫描**。当多个层级的墓碑区间起止边界交错或嵌套时（例如 Base 覆盖 $[A, D)@10$，Delta 覆盖 $[B, C)@20$，其中 $A < B < C < D$），原本连续的 Base 区间被 Delta 强制切割为三个时间序与空间序交错的独立碎片：
  $$[A, B) \to \text{seq } 10,\quad [B, C) \to \text{seq } 20,\quad [C, D) \to \text{seq } 10$$
- **结论**：**绝不存在“零自研扫线即可流式合并多 Run 范围墓碑”的捷径**。若不进行全量静态物化，在线流式消费多 Run 必然要求实现动态多路扫描线切分算法。

---

## 2. RocksDB 源码与接口精确审计映射

基于当前 RocksDB 代码仓（Commit 状态），生产读路径中与范围墓碑相关的 6 大核心组件源码映射如下：

```
+-----------------------------------------------------------------------------+
|                               DBIter                                        |
|  - 用户级可见性与版本判定 (db/db_iter.cc)                                     |
|  - 依赖底层 MergingIterator 自动屏蔽被 Range Tombstone 掩盖的点               |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                           MergingIterator                                   |
|  - 多路归并迭代器 (table/merging_iterator.cc:92-128)                         |
|  - 槽位严格约束: children_.size() == range_tombstone_iters_.size()           |
|  - Slot 0 专属于活跃 MemTable, 仅允许接收单个 TruncatedRangeDelIterator*    |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                     TruncatedRangeDelIterator                               |
|  - 墓碑截断适配器 (db/range_del_aggregator.h:23-95)                         |
|  - 强类型所有权: std::unique_ptr<FragmentedRangeTombstoneIterator>          |
|  - 核心导航非虚绑定: TopNext(), TopPrev(), Seek(), SeekForPrev()           |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
|                  MemTable::NewRangeTombstoneIteratorInternal                |
|  - 活跃 MemTable 墓碑构建 (db/memtable.cc:933-994)                           |
|  - 读取 cached_range_tombstone_; 未初始化则获取 reader_mutex                  |
|  - 一次性扫描 MemTableIterator(kRangeDelEntries) 构建全局静态切分列表         |
+-----------------------------------------------------------------------------+
```

### 2.1 `MemTable::NewRangeTombstoneIteratorInternal` (`db/memtable.cc:933-994`)
- **活跃 MemTable 路径**：
  通过原子加载 `cached_range_tombstone_`。若缓存未初始化（`!cache->initialized`），竞争 `reader_mutex`。竞争获胜者调用 `new MemTableIterator(kRangeDelEntries, *this, read_options)` 遍历当前 MemTable 内部跳表中的全部未切分范围墓碑，一次性构造 `FragmentedRangeTombstoneList`，并将 `initialized` 置为 true。随后返回包装该 `cache` 句柄的 `FragmentedRangeTombstoneIterator`。
- **不可变 MemTable 路径**：
  在转为不可变时由 `ConstructFragmentedRangeTombstones()` 一次性构造 `fragmented_range_tombstone_list_`，后续只读迭代器直接基于该不可变列表构造。

### 2.2 `TruncatedRangeDelIterator` (`db/range_del_aggregator.h:23-95`, `db/range_del_aggregator.cc:23-175`)
- **强类型所有权阻断**：
  构造函数强制要求入参为具体类指针：
  ```cpp
  TruncatedRangeDelIterator(
      std::unique_ptr<FragmentedRangeTombstoneIterator> iter,
      const InternalKeyComparator* icmp, const InternalKey* smallest,
      const InternalKey* largest);
  ```
  既不接受基类 `InternalIterator`，也不接受任何虚接口或抽象分片生成器。
- **非虚导航绑定（Non-Virtual Member Invocation）**：
  - `void Next() { iter_->TopNext(); }`
  - `void Prev() { iter_->TopPrev(); }`
  - `void Seek(const Slice& target) { iter_->Seek(target); }`
  - `void SeekForPrev(const Slice& target) { iter_->SeekForPrev(target); }`
  - `TopNext()`、`TopPrev()`、`SeekToTopFirst()` 在 `FragmentedRangeTombstoneIterator` 中**均非虚函数**。C++ 编译器直接生成直接寻址指令，外部多路合并器无法通过子类化多态介入。
- **截断与有效性契约**：
  将分片端点与 `[smallest_, largest_)` 进行比较求交：`start_key()` 取 $\max(\text{iter\_}->start, *smallest)$，`end_key()` 取 $\min(\text{iter\_}->end, *largest)$。

### 2.3 `MergingIterator` 单槽位契约 (`table/merging_iterator.h:61-76`, `table/merging_iterator.cc:92-128, 1134-1165`)
- **层级与槽位一一对应**：
  `MergingIterator` 中点数据迭代器与范围墓碑迭代器严格绑定：
  ```cpp
  assert(range_tombstone_iters_.size() == children_.size());
  ```
  第 $i$ 个范围墓碑迭代器严格对应第 $i$ 个数据源（Slot 0 = 活跃 MemTable，Slot 1..M = 不可变 MemTable，随后是 L0 SST 及各 Level 迭代器）。
- **单槽限制**：
  活跃 MemTable 在 `MergingIterator` 中**仅占用 Slot 0 这一个槽位**。系统无法将 AMTV 的 Base、Sealed Runs 0..N、Open Delta 分别作为独立墓碑源插入 `MergingIterator`，因为任何额外的槽位都会被误认为是更低层级的数据源，彻底破坏 LSM-Tree 的堆排序与层级遮蔽不变量。

### 2.4 `ReadRangeDelAggregator::AddTombstones` 与 MergingIterator 的本质差异
- **原生支持的多源聚合**：
  `ReadRangeDelAggregator` 确实具有多源收集能力：其内部维护一个 `StripeRep` 数组，每个 `StripeRep` 可以通过 `AddTombstones(std::unique_ptr<TruncatedRangeDelIterator>)` 挂载多个来源的墓碑。
- **关键局限：仅限点查判定，无法提供分片流**：
  `ReadRangeDelAggregator` 仅对外暴露：
  - `bool ShouldDelete(const ParsedInternalKey& parsed, RangeDelPositioningMode mode)`
  - `SequenceNumber MaxCoveringTombstoneSeqnum(const Slice& user_key)`
  - `bool IsRangeOverlapped(const Slice& start, const Slice& end)`
  它在内部通过 `active_iters_`（按 end_key 排序的大顶堆/小顶堆）和 `inactive_iters_` 维护点被覆盖状态。
- **无法接入 Scan**：
  `ReadRangeDelAggregator` **没有提供任何流式几何区间分片接口**（既无 `TopNext/TopPrev`，也无 `start_key/end_key/seq`）。因此，`ReadRangeDelAggregator` **绝对不能直接替代 `MergingIterator` 所需的范围墓碑输入接口**！

### 2.5 `DBIter`、`ArenaWrappedDBIter::Refresh` 与 Flush 调用约束
- **`DBIter` (`db/db_iter.cc`)**：
  处于迭代器栈顶层，驱动底层的 `MergingIterator`。在前进时通过 `MergingIterator` 的出堆元素判定 point key 是否有效，墓碑引起的跳跃完全在 `MergingIterator` 内部完成。
- **`ArenaWrappedDBIter::Refresh` (`db/arena_wrapped_db_iter.cc:285-320`)**：
  当用户调用 `Iterator::Refresh()` 时，若活跃 MemTable 有范围墓碑，执行：
  ```cpp
  auto t = sv->mem->NewRangeTombstoneIterator(read_options_, read_seq, false);
  *memtable_range_tombstone_iter_ =
      std::make_unique<TruncatedRangeDelIterator>(
          std::unique_ptr<FragmentedRangeTombstoneIterator>(t),
          &cfd->internal_comparator(), nullptr, nullptr);
  ```
  强依赖 `NewRangeTombstoneIterator` 返回 `FragmentedRangeTombstoneIterator*`。
- **MemTable 冻结与 Flush (`db/memtable.cc:996`, `db/flush_job.cc:442`)**：
  冻结时必须由 `ConstructFragmentedRangeTombstones()` 产生静态物化列表，Flush 时由 `FlushJob::WriteLevel0Table` 将该分片流序列化写入 SST Meta Block。

### 2.6 MVCC、时间戳、Put 复活与相同边界处理
- **用户定义时间戳（User Timestamps）**：
  时间戳作为 UserKey 的后缀存在。端点几何切分采用 `ucmp->CompareWithoutTimestamp`，区间内重叠墓碑采用 `ucmp->CompareTimestamp` 排序。
- **Snapshot Sequence 过滤**：
  在分片内部的 sequence 降序列表中二分查找 $\le read\_seq$ 的最大 sequence。若区间内所有墓碑均大于 $read\_seq$，该分片整体隐形。
- **Put 复活（Put Resurrection）**：
  在 `MergingIterator` 中，相同 UserKey 的 Put 和 DeleteRange 按照 InternalKey 比较规则（Sequence 降序）排列。若 Put 的 sequence 高于范围墓碑的 sequence，Put 排在前面且未被遮蔽，自然复活。
- **相同边界不同 Sequence**：
  相同的起止区间会被组织在同一个 `RangeTombstoneStack` 中，以 sequence 降序压入栈内，由二分查找精确定位可见版本。

---

## 3. 测试专用参考实现与差分 Oracle 架构

为了在绝对不修改生产读路径的前提下验证多 Run 合并的数学与语义完备性，我们在 `db/` 下新增了严格隔离的测试组件：
- [`db/amtv_scan_oracle.h`](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_scan_oracle.h)
- [`db/amtv_scan_oracle.cc`](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_scan_oracle.cc)
- [`db/amtv_scan_oracle_test.cc`](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_scan_oracle_test.cc)

### 3.1 架构设计与数据流
```
+----------------------------------------------------------------------------+
|                          AMTVScanInput (Snapshot)                          |
|  - Base Run (0..1)                                                         |
|  - Sealed Runs (0..N)                                                      |
|  - Open Delta (0..1)                                                       |
|  - read_seq, timestamp_upper_bound, ucmp                                   |
+----------------------------------------------------------------------------+
                |                                            |
                v                                            v
+------------------------------------+      +--------------------------------+
|     AMTV Dynamic Sweep-Line        |      |      Native Ground Truth       |
|    Multi-Run Reference Merger      |      |   (FragmentedRangeTombstone)   |
+------------------------------------+      +--------------------------------+
  1. 提取所有输入 Run 的边界事件并排序         1. 一次性聚合所有原始 RangeTombstone
  2. 遍历相邻基本区间 [e_i, e_i+1)              2. 构造原生 FragmentedRangeTombstoneList
  3. 计算区间内各 Run 的最大可见 sequence       3. 构造原生 FragmentedRangeTombstoneIterator
  4. 支持基本分片与相邻等价 Coalesced 分片
                |                                            |
                +---------------------+----------------------+
                                      |
                                      v
                       +-----------------------------+
                       |    5 项严格差分验证矩阵      |
                       +-----------------------------+
                       1. 输出分片全局单调递增
                       2. 相邻分片严格几何互斥
                       3. 全量离散点最大覆盖 sequence 100% 一致
                       4. ShouldDelete 判定 100% 一致
                       5. DBIter 实测 Key/Value 遍历序列 100% 一致
```

### 3.2 参考多路扫线合并器核心算法
1. **端点事件收集（Boundary Events Extraction）**：
   从 Base、全部 Sealed Runs 及 Open Delta 中收集所有原始墓碑的起止端点，形成事件点集合 $E = \{e_0, e_1, \dots, e_M\}$。按 `ucmp` 排序并严格去重。
2. **基本区间扫线（Elementary Interval Sweeping）**：
   对于任意相邻端点对 $[e_i, e_{i+1})$，由于中间不存在任何墓碑起止点，覆盖该区间的墓碑集合在空间上是严格不变的（Invariance of Covering Tombstones）。
3. **可见覆盖计算（Max Visible Coverage Calculation）**：
   遍历各 Run，筛选满足 $t.start \le e_i$ 且 $e_{i+1} \le t.end$ 的墓碑。进一步按 $t.seq \le read\_seq$（以及时间戳上限）过滤，取最大的 sequence 作为该区间的有效遮蔽序列号。若无覆盖墓碑，标记为 Gap。
4. **邻接区间微合并（Adjacent Coalescing）**：
   若开启 `coalesce_adjacent = true`，当相邻基本区间 $[e_{i-1}, e_i)$ 与 $[e_i, e_{i+1})$ 拥有完全一致的有效遮蔽 sequence 及 timestamp 时，合并为一个大的分片区间 $[e_{i-1}, e_{i+1})$。未开启时，输出与原生 Ground Truth 完全 1:1 的基本分片序列。

---

## 4. 必测语义矩阵验证结果

我们在 `db/amtv_scan_oracle_test.cc` 中实现了覆盖全部 15 类严苛语义的自动化测试矩阵，所有测试用例均基于 GoogleTest 框架执行，**测试通过率 100%（15 / 15 PASSED，耗时 78 ms）**。

| 序号 | 测试用例名称 | 测试目标与语义覆盖 | 执行耗时 | 测试结论 |
| :--- | :--- | :--- | :---: | :---: |
| 1 | `EmptyRunPermutations` | 覆盖 Base / Sealed / OpenDelta 空与非空的全部 $2^3 = 8$ 种排列组合 | 1 ms | **PASSED** |
| 2 | `OverlapAndNesting` | 多 Run 跨层交错重叠、完全嵌套、部分重叠与多重 sequence 竞争 | < 1 ms | **PASSED** |
| 3 | `AdjacentBoundaries` | 相邻边界 $[10, 30)$, $[30, 60)$, $[60, 90)$ 处端点精确归属与开闭区间判定 | 1 ms | **PASSED** |
| 4 | `SameIntervalDifferentSequences` | 相同起止区间 $[20, 80)$ 在多层具有不同 sequence（序列号倒置与遮蔽） | < 1 ms | **PASSED** |
| 5 | `InterleavedBoundariesAcrossRuns` | 跨 Run 交错边界 $[10, 40)@10$, $[20, 50)@20$, $[30, 60)@30$ 切分验证 | 1 ms | **PASSED** |
| 6 | `DeleteRangeFollowedByPutResurrection_AndDBScan` | 真实 RocksDB 实例端到端 Scan，实测 DeleteRange $\to$ Put 复活 $\to$ 再删除 $\to$ 再复活 | 21 ms | **PASSED** |
| 7 | `SnapshotCreationBeforeAndAfter` | 模拟多代写入，在不同 sequence 纪元创建 Snapshot，验证 MVCC 严格隔离 | 1 ms | **PASSED** |
| 8 | `UserTimestampCompetingTombstones` | 启用 64 位用户定义时间戳，测试时间戳竞争及 `timestamp_upper_bound` 过滤 | < 1 ms | **PASSED** |
| 9 | `ScanBoundariesCoincidingWithTombstones` | 扫描游标 Seek / SeekForPrev 恰好落在墓碑 start_key / end_key / gap 处 | 1 ms | **PASSED** |
| 10 | `BidirectionalSeekAndScan` | 正向 `SeekToTopFirst/TopNext` 与反向 `SeekToTopLast/TopPrev` 对称镜像遍历 | < 1 ms | **PASSED** |
| 11 | `IteratorRefresh` | 模拟 `ArenaWrappedDBIter::Refresh`，Open Delta 写入新墓碑后游标平滑更新 | < 1 ms | **PASSED** |
| 12 | `ActiveMemTableToImmutable` | 活跃 MemTable 转不可变时，AMTV 多 Run 视图与原生坍缩物化视图严格等价 | 1 ms | **PASSED** |
| 13 | `OpenDeltaLifecycleTransitions` | Open Delta 未封箱 $\to$ 刚好封箱 $\to$ 后台归并三阶段生命周期不变性 | < 1 ms | **PASSED** |
| 14 | `AMTVFallbackState` | 超过硬 Run 限制（$K > 32$）触发回退，委托原生逻辑，与 Ground Truth 一致 | 1 ms | **PASSED** |
| 15 | `RandomizedDifferential1000Trials` | **1,000 组随机多层区间集合**（随机 Run 数、墓碑数、区间、sequence），逐 Key 全量差分 | 50 ms | **PASSED** |

### 4.1 核心差分断言通过详情
在上述全部测试中，每一轮样本均严格验证了以下五大断言：
1. **严格有序推进**：$S_i < S_{i+1}$ 严格成立，无乱序、空转或回退；
2. **全局几何互斥**：$E_i \le S_{i+1}$ 严格成立，相邻分片绝无交叠；
3. **全离散点覆盖一致**：在键空间所有探测点上，`AMTVMultiRunScanIterator::MaxCoveringTombstoneSeqnum(key)` 严格等于 `FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum(key)`；
4. **`ShouldDelete` 判定一致**：在所有 snapshot sequence 下，`ShouldDelete` 返回的布尔值与原生完全一致；
5. **DB Iterator 可见序列一致**：在真实 DB 实例遍历中，可见键集合与 AMTV 预测集合完全吻合（位级一致）。

---

## 5. 生命周期与复杂度边界量化

### 5.1 理论复杂度模型与量化指标

设 AMTV 中活跃 MemTable 包含 $K$ 个 Run（Base + $N_{\text{sealed}}$ 个 Sealed Runs + 1 个 Open Delta），总墓碑数为 $N = \sum_{r=1}^{K} N_r$。

| 指标维度 | 全局静态物化方案（原生缓存机制） | 动态 K 路扫描线在线合并方案（理论推演） |
| :--- | :--- | :--- |
| **输入 Run 数 $K$** | $K$ 路一次性全量输入 | $K$ 路游标并发维护 |
| **端点事件总数** | $2N$ 个端点，一次性全局排序 | 在线排序/堆维护 $2N$ 个端点 |
| **单次构建时间复杂度** | $O(N \log N)$（仅写触发或 Scan 首次失效触发） | $O(K \log K)$（游标建堆） |
| **单次 Scan 遍历复杂度** | $O(\log F + L)$（$F$ 为分片数，$L$ 为扫描长度） | $O(L \cdot \log K)$（每遇边界调整 $K$ 路堆） |
| **输出分片数 $F$** | 极小（基本分片 $F \le 2N$） | 极小（若不做合并，微碎片可能达 $O(N)$） |
| **反向遍历支持代价** | 原生静态数组，`--pos_` 耗时 $O(1)$ | 需额外维护反向历史栈或区间线段树，开销 $O(L \cdot \log K)$ |
| **局部分片复用性** | 无法直接复用，需重构全局列表 | 各 Run 内部局部有序，但跨 Run 拓扑切分必须重新计算 |
| **$H=32$ 最坏场景表现** | 一次性物化开销平摊（写端开销） | 每次短扫描（如 $L=5$）需初始化 32 叉堆，**Scan 延迟显著放大** |

### 5.2 生命周期与所有权关系矩阵

```
+------------------+-------------------------------------------------------------+
| 组件对象          | 所有权与生命周期管理机制                                      |
+------------------+-------------------------------------------------------------+
| 旧 Snapshot      | 持有当前 AMTVSnapshot 的 std::shared_ptr<const AMTVSnapshot>|
|                  | 引用的 Base 和 Sealed Runs 为不可变对象，读生命周期完全安全   |
+------------------+-------------------------------------------------------------+
| 后台归并 (Merge) | 采用 Copy-On-Write 机制产生新 Run 并发布新 Snapshot；         |
|                  | 旧 Run 由正在执行 Scan 的旧 Snapshot 引用计数维持，析构时释放 |
+------------------+-------------------------------------------------------------+
| Open Delta       | Active MemTable 独占写入。Scan 必须持有已封箱的 Snapshot     |
|                  | 或由并发安全结构（如无锁跳表）提供 read_seq 保护              |
+------------------+-------------------------------------------------------------+
| MemTable 冻结    | 状态机转为 Immutable，Open Delta 强制封箱；                    |
|                  | 由 ConstructFragmentedRangeTombstones 一次性坍缩为单一原生列表|
+------------------+-------------------------------------------------------------+
```

---

## 6. 核心判断与架构决策

### 6.1 核心问题明确解答

#### 判断 1：是否能仅通过现有 `ReadRangeDelAggregator` 多源接口完成真实 Scan 接入？
> **明确结论：绝对不能 (NO)**。

**理由**：
1. **接口类型强阻断**：`MergingIterator` 在构造与运行期间，严格要求传入的是 `TruncatedRangeDelIterator`，且其内部 `range_tombstone_iters_` 数组大小必须与点数据迭代器数组大小严格一致（1:1 绑定）。
2. **缺乏流式分片协议**：`ReadRangeDelAggregator` 仅具有点查判定方法（`ShouldDelete`），完全不具备流式推进接口（无 `TopNext`、无 `start_key/end_key/seq`）。
3. **架构语义错位**：`ReadRangeDelAggregator` 是为了在给定一个 Point Key 时回答“该点是否被遮蔽”；而 `MergingIterator` 需要的是在空间轴上向前推进时回答“当前有效的遮蔽区间几何边界是什么，以指导点迭代器跨步跳跃”。

#### 判断 2：若不能，新的动态 K 路扫描线合并器是否不可避免？
> **明确结论：若坚持在活跃 MemTable 内维持多 Run 零物化并供 Scan 消费，则动态 K 路扫描线合并器不可避免 (YES, INEVITABLE)**。

**理由**：
由于不同 Run 之间的区间交错切割是空间几何必然，在没有预建物化索引的情况下，唯有在 Scan 推进过程中维护一个动态扫线事件堆（Min-Heap of Interval Endpoints），实时捕获最早结束或开始的端点并切分微碎片。

---

## 7. 生产落地的阻断点清单与明确 Go / No-Go 建议

### 7.1 生产落地阻断点清单（Blocking Points）

1. **[阻塞 1] 核心公共头文件侵入性改造阻断**：
   若要将动态多路合并器接入 `TruncatedRangeDelIterator`，必须修改 `db/range_del_aggregator.h`，将 `std::unique_ptr<FragmentedRangeTombstoneIterator> iter_` 改为抽象虚接口（如 `RangeTombstoneStream`），并将 `TopNext`、`TopPrev`、`Seek` 等全部改造为虚函数。这不仅破坏 RocksDB 现有极速内联性能，而且跨越了不修改核心公共接口的工程红线。
2. **[阻塞 2] 动态扫线 CPU 开销反噬 Scan 性能**：
   在短 Scan 场景下，原生一次物化后单次查询为 $O(\log F)$ 二分查找加连续内存顺序遍历；动态多路扫线每次都需要初始化 $K$ 路堆并进行动态事件切分，其常数开销和堆调整开销可能导致短 Scan 延迟严重劣化。
3. **[阻塞 3] 双向扫描复杂度与内存开销**：
   Scan 必须支持 `SeekForPrev` 和 `TopPrev`。动态扫线天然适合单向事件驱动；若要高效支持反向扫描，合并器必须在内存中维护端点历史栈或双向区间线段树，逻辑极其脆弱且增加显著内存负担。

### 7.2 明确建议：NO-GO（禁止自研在线多路扫线合并器）

> [!CAUTION]
> **关于 M1c 生产实现的最终建议：明确 NO-GO**。
>
> 严禁在生产环境尝试开发、集成或替换 `MergingIterator` 内部的动态多路扫描线合并器。该方向收益极低（Scan 在读负载中天然需要全局视图），但工程风险与架构破坏性极大。

### 7.3 推荐的工程解法：按需延迟全局物化（Lazy Materialization with Invalidation Cache）

为保持架构优美性并兼顾性能，推荐未来针对 Scan 采取**按需延迟物化（Lazy Global Materialization）**方案：
1. **点查路径保持成熟的 AMTV 多层跳表**：
   Point Get / MultiGet 继续使用已在 M1b/M2/M3 中验证成熟的多层跳表独立二分取 max 旁路，享受零锁、高吞吐与无竞争写入。
2. **Scan 路径保持 100% 原生单槽契约**：
   活跃 MemTable 的 `NewRangeTombstoneIteratorInternal` 仍然对外输出标准的单一 `FragmentedRangeTombstoneIterator`。
3. **写时不物化，读时按需物化并缓存**：
   - 写入 `DeleteRange` 时仅将 tombstone 追加到 AMTV 的 Open Delta，**仅将缓存标记失效（`cache->initialized = false`），绝不执行 $O(N \log N)$ 物化**；
   - 若前台全部是 Point Get，系统永远不会发生全局物化；
   - 仅当有实际 Scan 到达且缓存失效时，才从 Base + Sealed + Open Delta 中抽取全量墓碑，一次性构建原生 `FragmentedRangeTombstoneList` 并缓存；后续连续 Scan 直接命中缓存。

---

## 8. 交付物与提交信息

- **文档交付**：
  [`docs/M1c-P0-scan-interface-and-semantics-audit.md`](file:///home/wam/grad/rocksdb-v11.8.0/docs/M1c-P0-scan-interface-and-semantics-audit.md)
- **代码交付（测试专用）**：
  - [`db/amtv_scan_oracle.h`](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_scan_oracle.h)
  - [`db/amtv_scan_oracle.cc`](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_scan_oracle.cc)
  - [`db/amtv_scan_oracle_test.cc`](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_scan_oracle_test.cc)
- **编译规则交付**：
  [`Makefile`](file:///home/wam/grad/rocksdb-v11.8.0/Makefile)（新增 `amtv_scan_oracle_test` 独立测试目标）
- **测试验证结果**：
  - `amtv_scan_oracle_test`: **15 / 15 PASSED (78 ms)**
  - `amtv_test`: **29 / 29 PASSED**
  - `amtv_probe_test`: **3 / 3 PASSED**
- **生产读路径变更**：**0 行（严格未修改真实 DB Scan 读路径）**
