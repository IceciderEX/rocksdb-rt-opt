# M1c: 活跃 MemTable 范围墓碑 Scan 架构可行性与接口约束审计

## 1. 背景与核心认知纠正

在 M1a 实施初期，曾有一种假设：“由于 Point Get 可以在 Base、Sealed Delta 和 Open Delta 三个层级分别调用 `MaxCoveringTombstoneSeqnum` 后取最大值（$\max(s_{\text{base}}, s_{\text{sealed}}, s_{\text{open}})$），因此 Scan 也可以通过简单的三路指针推进并在当前点取 max 实现零自研扫线”。

**该假设在数学与工程语义上均不成立**。
Point Get 是**零维点查询**，点覆盖判定在布尔代数和序关系上满足局部结合律；而 Scan 是**一维区间重叠与分片流式扫描**。当多个区间的起止边界相互交错（例如 Base 覆盖 $[A, D)@\text{seq}10$，Delta 覆盖 $[B, C)@\text{seq}20$，其中 $A < B < C < D$）时，原本 Base 的连续区间 $[A, D)$ 会被 Delta 强制切分成三个在时间序与空间序上均不连续的碎片：
$$[A, B) \to \text{seq}10,\quad [B, C) \to \text{seq}20,\quad [C, D) \to \text{seq}10$$
任何不经过严格区间事件点切分（Sweep-line Event Splitting）的单向指针推进，都无法在维持 $O(1)$ 或轻量状态下生成符合 RocksDB Scan 契约的非重叠分片流。

因此，本文档根据 RocksDB v11.8.0 真实源码，全面澄清 Scan 路径下活跃 MemTable 的单槽接口契约、实际语义与技术阻塞。

---

## 2. `TruncatedRangeDelIterator` 单槽接口完整契约

### 2.1 依赖结构与非虚函数限制
在 [`db/range_del_aggregator.h:31-95`](file:///home/wam/grad/rocksdb-v11.8.0/db/range_del_aggregator.h#L31-L95) 中，`TruncatedRangeDelIterator` 负责持有并截断单个输入源的范围墓碑流：

```cpp
class TruncatedRangeDelIterator {
 public:
  TruncatedRangeDelIterator(
      std::unique_ptr<FragmentedRangeTombstoneIterator> iter,
      const InternalKeyComparator* icmp, const InternalKey* smallest,
      const InternalKey* largest);
...
```

1. **强类型所有权**：构造函数入参为 `std::unique_ptr<FragmentedRangeTombstoneIterator>`，不支持基类 `InternalIterator`，也无法接入任意虚接口。
2. **核心导航方法非虚**：
   - `void Next() { iter_->TopNext(); }`
   - `void Prev() { iter_->TopPrev(); }`
   - `void InternalNext() { iter_->Next(); }`
   - `TopNext()`、`TopPrev()`、`SeekToTopFirst()` 在 `FragmentedRangeTombstoneIterator` 中均为 **非虚（non-virtual）成员函数**。
3. **输入前置假设**：
   - `iter_` 底层绑定的 `FragmentedRangeTombstoneList` 必须**预先全局有序且区间严格互斥**（Pairwise Disjoint）：对于任意相邻分片 $i$ 和 $i+1$，满足 $I_i.\text{end\_key} \le I_{i+1}.\text{start\_key}$。
   - 每个分片内部维护一个按 `SequenceNumber` 降序排列的栈。

### 2.2 接口输出契约
`TruncatedRangeDelIterator` 对外部消费者（如 `MergingIterator` 或 `CompactionRangeDelAggregator`）暴露的契约如下：
- `Valid()`: 当前指向有效分片，且未超出 `smallest` / `largest` 截断边界。
- `start_key()`: 返回 $\max(\text{iter\_}->parsed\_start\_key(), *smallest)$。
- `end_key()`: 返回 $\min(\text{iter\_}->parsed\_end\_key(), *largest)$。
- `seq()`: 当前可见的最大 sequence number（由 `SetRangeDelReadSeqno(read_seqno)` 约束）。
- `timestamp()`: 当前分片的 user timestamp。

---

## 3. Scan 中核心导航与 Snapshot 过滤的实际语义

在 [`db/range_tombstone_fragmenter.cc`](file:///home/wam/grad/rocksdb-v11.8.0/db/range_tombstone_fragmenter.cc) 中，原生的迭代语义由两层指针驱动：
- `pos_`：指向当前几何区间分片 `RangeTombstoneStack`（区间位置）。
- `seq_pos_`：指向该区间内当前可见的 sequence number。

### 3.1 核心操作语义
1. **`SeekToTopFirst()` / `SeekToTopLast()`**:
   - 定位到列表首个/末尾分片；
   - 调用 `SetMaxVisibleSeqAndTimestamp()`：在当前 `pos_` 的 sequence 列表中二分查找 $\le \text{upper\_bound\_}$ 的最大 sequence；
   - 调用 `ScanForwardToVisibleTombstone()`：如果当前分片在 $[lower\_bound\_, upper\_bound\_]$ 内没有可见 sequence，则沿 `pos_` 推进，直到找到有可见 sequence 的分片或到达末尾。
2. **`TopNext()` / `TopPrev()`**:
   - `pos_` 步进到下一个几何分片；
   - 重新计算该分片的可见 top sequence；
   - 跳过不可见分片。
3. **`Next()` / `Prev()` (InternalNext)**:
   - 在**同一个几何分片内部**沿 sequence 降序列表下移（主要供多 snapshot 切分和 compaction 消费）。
4. **边界切换与 Snapshot 过滤**:
   - 当 point key 推进跨过当前分片的 `end_key` 时，Scan 驱动器调用 `TopNext()` 切换边界；
   - 若 `read_seqno` 较小，较新写入的范围墓碑在当前分片中直接被过滤掉；若分片中所有墓碑均较新，则该分片整体对该读者隐形。

---

## 4. 多层 Base + N 个 Sealed Delta + OpenDelta 的最大来源数

在 AMTV 架构中，若支持增量视图且不全量重构，活跃 MemTable 内部的潜在来源包括：
1. **Base**: 0 或 1 个历史稳定分片列表 `FragmentedRangeTombstoneList`（已完成全量切分）。
2. **Sealed Deltas**: $N$ 个已冻结但未合并的分片列表（在 M1b/M1c 设计中，$N \le 1$，最多允许 1 个 sealed delta）。
3. **OpenDelta**: 1 个当前正在接收写入的动态 delta（条目数 $< 64$）。

**真实最大来源数**：
$$K_{\max} = 1 (\text{Base}) + 1 (\text{Sealed Delta}) + 1 (\text{OpenDelta}) = 3$$
（在更复杂的多级 delta 设计中，$K$ 随 sealed delta 数量可能上升为 $2 + N$）。

---

## 5. 为 MergingIterator 输出全局有序、无重叠分片流的要求

如果要在活跃 MemTable 的单槽接口中提供 AMTV 视图，该门面必须将这 $K$ 个来源融合成一个输出流，且必须满足以下五项铁律：

1. **时空等价性（Semantic Equivalence）**：
   对于任意键 $k \in \text{KeySpace}$，合成流在 $k$ 处的有效覆盖 sequence 必须严格等于全量墓碑构成的 `FragmentedRangeTombstoneList` 在 $k$ 处可见的最大 sequence：
   $$\text{Stream}(k).\text{seq} = \text{GroundTruth}(k).\text{seq}$$
2. **全局几何互斥（Pairwise Disjoint）**：
   合成流输出的相邻区间 $[S_i, E_i)$ 与 $[S_{i+1}, E_{i+1})$ 必须满足 $E_i \le S_{i+1}$，严禁输出相互交叠的区间。
3. **严格有序推进（Monotonic Ordering）**：
   $S_i < S_{i+1}$，严禁乱序或回退。
4. **支持随机 Seek 与 SeekForPrev**：
   Scan 过程中伴随前缀定位、范围重置（Iterator Refresh）及反向扫描（`SeekForPrev`、`TopPrev`），合成流必须支持双向游标定位，不能仅是单向 generator。
5. **内存独立性与生命周期安全**：
   AMTV 影子条目必须深拷贝边界键，以避免适配器自身的 Slice 生命周期错误。合成流在迭代过程中产生的新切分端点不能引用即将析构的临时缓冲区。

---

## 6. 新合并器性质与逐 Scan 三方 Differential Oracle

如果放弃全量物化，试图用多路游标在线合成该流，**这本质上是一个全新的动态多路区间切分扫线算法（Dynamic K-Way Interval Sweep-Line Splitter）**。它绝不是简单的“包装器”或“指针代理”。

### 6.1 算法复杂度与正确性代价
- 该算法需要维护 $K$ 个游标的当前端点堆（Min-Heap of Interval Endpoints）；
- 每次推进遇到最早的端点（不论是某个来源的 start 还是 end），都需要切割当前区间、重新计算所有重叠来源的最大活跃 sequence，并生成微碎片（Micro-fragments）；
- 反向扫描（`TopPrev`）需要维护前驱端点栈或双向线段树，逻辑复杂度极高；
- 边界切分引入的碎片数量可达 $O(M_{\text{base}} + M_{\text{sealed}} + M_{\text{open}})$，微碎片的增多会直接加重 `MergingIterator` 堆调整的开销。

### 6.2 逐 Scan 三方 Differential Oracle 方案
若未来实现该合并器，必须在接入真实 Scan 读路径前，通过以下逐 Scan 离线差分探针验证：

```text
Oracle 验证三方等价性：
对每次 DB::NewIterator() 创建的 Scan 扫描：
1. Target: AMTV 在线多路切分迭代器输出的分片序列 [(S_a, E_a, seq_a)]
2. GroundTruth 1: 原生 range_del_table_ 全量物化的 FragmentedRangeTombstoneList 分片序列 [(S_g, E_g, seq_g)]
3. GroundTruth 2: DB 真实迭代器遍历全键空间时的实际可见性（Deleted vs Live）
要求：
- 逐分片严格一致：S_a == S_g, E_a == E_g, seq_a == seq_g
- 逐点判定严格一致：对扫描到的每一条 Point Key，其被范围墓碑掩盖的状态完全等价
```

---

## 7. 路线阻塞与当前技术结论

### 7.1 技术阻塞梳理
1. **接口类型强阻断**：
   RocksDB `TruncatedRangeDelIterator` 强绑定 `FragmentedRangeTombstoneIterator` 具体类，且核心方法非虚。如果不修改 RocksDB 核心公共头文件以引入虚基类或模板化改造，任何外部多路合并器都无法直接填入该单槽位。
2. **在线扫线 CPU 开销可能反噬 Scan 性能**：
   全量 `FragmentedRangeTombstoneList` 是一次构建、$O(\log M)$ 二分查询；若在每次 Scan 中由新算法在线动态拆分多路区间，其常数开销和端点事件处理可能远大于一次性物化的开销，尤其是在高频短 Scan 场景下。
3. **零自研声明撤回**：
   正式撤回此前“零自研扫线算法”或“分层取 max 即可直接用于 Scan”的结论。在不构建全量 Fragment 的前提下，不存在不写扫线算法就能让多路区间流等价于单个分片流的魔法。

### 7.2 阶段决策与执行边界
- **重要澄清：`AddToRangeDelAggregator()` 的存在绝不等于具备可供 `MergingIterator` 使用的全局有序、互不重叠分片流**：
  `ReadRangeDelAggregator` 可以接受通过 `AddToRangeDelAggregator()` 挂载多个来源，但它仅用于点查布尔判定（`ShouldDelete(key)`）；在该场景下对各个来源独立二分并取最大 sequence 满足局部结合律。然而，范围扫描 `MergingIterator` 要求输入的单个范围墓碑槽位输出一个**全局严格有序、互不重叠、随游标推进且支持反向 Seek 的单调几何分片流**。多源独立挂载根本无法直接被 `MergingIterator` 消费。
- **M1c-ScanArchitecture 明确暂停在设计与差分探针阶段**，绝不修改、接入或替换真实 Scan 读路径（`DBImpl::NewIterator`、`MemTable::NewRangeTombstoneIteratorInternal`、`MergingIterator`、`DBIter`）。
- **保留原生 Scan 的一切行为**：包括 DeleteRange 后的 `cached_range_tombstone_` 缓存失效、原生锁争用与原生视图物化，确保原生 Scan 正确性 100% 不受影响。
- **M1b-GetOnly 聚焦于成熟的点查旁路**：Point Get 的分层取 max 已在数学上严格证明等价，且三方/四方 Oracle 已通过验证。M1b 将专注于将该成熟能力安全、确定性地接入活跃 MemTable 的 `Get`（及 `MultiGet`）路径。
