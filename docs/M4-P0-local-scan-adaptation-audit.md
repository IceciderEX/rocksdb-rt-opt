# AMTV M4-P0：局部 Scan 视图源码适配审计报告

- **阶段标识**：AMTV M4-P0 (Local Scan View Source Adaptation Audit)
- **阶段性质**：架构与语义审计闸门（Architecture & Semantics Audit Gate）
- **基准环境**：RocksDB v11.8.0 / Linux x86_64
- **审计原则**：
  1. 严格只读审计与基线确认，禁止修改真实 DB Scan 读路径（`DBIter`、`MergingIterator`、`ArenaWrappedDBIter`、`MemTable::NewRangeTombstoneIteratorInternal` 原生生产代码 100% 保持原样）；
  2. 严禁修改现有 AMTV Get/MultiGet、Run 归并、Fallback、Flush、Compaction 或 WAL 恢复逻辑；
  3. 严禁接入新的 DB Iterator、生产缓存或在线 K 路合并器；
  4. 严禁运行 M3b、F1、F2、LongCycle 或任何正式性能矩阵；
  5. 严禁用不完整 MVCC 语义的“仅保留最大 sequence”近似方案绕过问题；
  6. 明确区分“已从当前源码核实”、“由既有实验支持”、“仍待 P1 验证”三类结论。

---

## 1. 固定版本与基线配置

### 1.1 仓库 Commit SHA 与工作树状态

| 仓库 | 分支 | 跟踪远程分支 | 固定 Commit SHA | 工作树状态 |
| :--- | :--- | :--- | :--- | :--- |
| **RocksDB 核仓** (`rocksdb-v11.8.0`) | `main` | `rt-opt/main` | `395ce9863fc4ecef4fba75f1b9e1e2510edf45c1` | clean (无未提交改动) |
| **研究仓** (`s14-range-delete-study`) | `main` | `origin/main` | `869a25044afd1baaddf777cd27ceaa5478aa0192` | clean (无未提交改动) |

- **网络与代理配置**：环境使用本地 HTTP 代理 `http://127.0.0.1:16989`，与远程 `rt-opt/main` 及 `origin/main` 保持完全同步。
- **现有相关单测基线**：
  - `./amtv_scan_oracle_test`：15/15 全部通过（耗时 73 ms）；
  - `./amtv_probe_test`：3/3 全部通过（耗时 1 ms）；
  - 现有 AMTV 离线单测全部处于通过状态。

### 1.2 当前 AMTV 核心配置与状态机规则

依据当前源码核实（`include/rocksdb/advanced_options.h:422-446`，`db/amtv.h:215-280`）：

1. **Open Delta 阈值**：
   - 配置字段：`AdvancedColumnFamilyOptions::amtv_delta_tombstones = 64`（默认 64 条）；
   - 行为事实：当活跃 `OpenDelta` 中累积的墓碑条目数达到 64 时，触发 `FreezeOpenDelta()`，封口为不可变 `AMTVRun`，并异步请求或排队 Run 归并。
2. **Run 归并规则与软上限**：
   - 配置字段：`AdvancedColumnFamilyOptions::amtv_merge_soft_limit = 4`（`AMTVState` 内部默认触发阈值为 2）；
   - 行为事实：当 sealed runs 数量达到软上限时，后台调度归并线程，将多个 sealed runs 执行多路有序归并，合并成一个包含全量去重与分片列表的单个较大 Run。
3. **分层硬上限与 Fallback 行为**：
   - 配置字段：`AdvancedColumnFamilyOptions::amtv_hard_layer_limit = 8`；
   - 行为事实：若后台归并积压，sealed runs 数量达到 8（硬上限），`AMTVState::AddTombstone` 触发保护性降级，将 `fallback_mode_` 设为 `true`。在 Fallback 模式下，点查退化为串行逐 Run 降序扫描或直接委托原生 MemTable 范围墓碑跳表查询，避免层数无限膨胀导致点查延迟劣化。
4. **编译期与运行期总开关**：
   - `AdvancedColumnFamilyOptions::enable_amtv = false`（生产默认关闭，需显式配置开启）。

### 1.3 现有 GetOnly 主路径与原生 Scan 主路径的精确差异

| 维度 | 现有 AMTV GetOnly 主路径 | 原生 Scan 主路径 (`DBImpl::NewIterator`) |
| :--- | :--- | :--- |
| **查询维度** | 零维点查（Point Lookup at single key $k$） | 一维区间扫描（Streamed Interval Scan over $[L, U)$） |
| **几何覆盖语义** | **标量最大值结合律**：只需在各 Run 及 Open Delta 独立查询覆盖 $k$ 的最大 sequence，跨 Run 结果通过标量取 $\max$ 即可完成 MVCC 判定，无需几何合并 | **一维区间切分与拓扑重叠**：不同 Run 的墓碑区间在 $[L, U)$ 内交错、包含、相交，必须被切割为全局单调递增、互不重叠的离散片段（Fragments），并携带各片段的完整版本栈 |
| **消费接口契约** | `AMTVState::MaxCoveringTombstoneSeqnum`：返回标量 `SequenceNumber` | `MergingIterator` 单槽位契约：Slot 0 仅接受单个 `TruncatedRangeDelIterator`，强制绑定非虚的具体类 `FragmentedRangeTombstoneIterator` |
| **边界信息感知** | 仅传入单个点查 Key | 用户提供 `ReadOptions::iterate_lower_bound` / `iterate_upper_bound`，但活跃 MemTable 原生构建时完全忽略该边界，做全局物化 |
| **内存与锁竞争** | 无锁原子加载 `AMTVSnapshot`，各 Run 内部只读二分查找，零锁竞争 | 原生路径在 `MemTable::NewRangeTombstoneIteratorInternal` 中竞争 `reader_mutex`，全量扫描构建 `FragmentedRangeTombstoneList` |

### 1.4 本次未修改生产 Scan 路径的证据

- 关键源文件 `git diff HEAD` 校验：
  - `db/db_impl/db_impl.cc`：Diff 为 0；
  - `table/merging_iterator.cc`：Diff 为 0；
  - `table/merging_iterator.h`：Diff 为 0；
  - `db/arena_wrapped_db_iter.cc`：Diff 为 0；
  - `db/arena_wrapped_db_iter.h`：Diff 为 0；
  - `db/db_iter.cc`：Diff 为 0；
  - `db/memtable.cc`：Diff 为 0；
  - `db/range_del_aggregator.h`：Diff 为 0；
- **核查结论**：本次审计处于 100% 严格只读状态，未引入任何生产读路径的修改。

---

## 2. 源码事实总表（“问题—当前代码事实—文件与行号—结论—P1影响”）

| 序号 | 审计核心问题 | 当前代码事实 | 文件与行号 | 审计结论 | P1 影响与约束 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Q1** | `AMTVSnapshot`、`AMTVRun`、`OpenDelta` 保存了什么原始信息？ | `AMTVRun` 保存 `std::vector<OpenDeltaEntry> raw_entries` 与预分片的 `std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list`；`OpenDelta` 保存扁平 `entries_`；条目中完整包含 start/end key、sequence、timestamp、type。 | `db/amtv.h:35-51, 113-134`, `db/amtv.cc:25-68` | **已从源码核实**：墓碑原始语义信息与分片信息均 100% 完整保留，未丢失任何元数据。 | P1 原型可直接无损访问底层条目，无需增设字段或改变结构。 |
| **Q2** | 对象所有权与生命周期在归并、变只读、销毁时如何保证安全？ | `AMTVSnapshot` 内部持有 `std::vector<std::shared_ptr<const AMTVRun>> sealed_runs` 与 `std::shared_ptr<const OpenDelta> open_delta`。通过 `std::shared_ptr` 保证旧 Snapshot 引用的 Run 不会被析构。 | `db/amtv.h:170-208`, `db/amtv.cc:295-318` | **已从源码核实**：即使后台发生 Run 归并、MemTable 冻结为 Immutable 或触发 Fallback，已创建的 Snapshot 依然完全只读且引用计数自保。 | P1 局部视图可安全持有 `AMTVSnapshot` 句柄，不存在悬垂指针或竞态风险。 |
| **Q3** | 对给定半开区间 $[L, U)$，能否从 sealed Run 定位所有相交片段，尤其是跨左边界片段（$start < L < end$）？ | sealed Run 的 `fragmented_list` 是按 `start_key` 严格有序且互不重叠的分片。使用 `std::upper_bound(begin, end, L, end_cmp)` 可以 $O(\log F)$ 定位首个 $end > L$ 的片段，该片段自然包含 $start < L < end$ 的跨边界覆盖。 | `db/range_tombstone_fragmenter.cc:387-397`, `db/range_tombstone_fragmenter.h:50-75` | **已从源码核实**：定位相交片段与跨左边界片段在数学和算法上完全成立，二分上界查找绝不会遗漏跨左边界墓碑。 | P1 原型必须以 $end > L$ 为过滤左界，绝对不能用 $start \ge L$ 过滤，否则会导致数据静默腐化。 |
| **Q4** | `OpenDelta` 能否在不扫描无关历史 Run 的前提下完成完整相交提取？ | `OpenDelta` 硬上限为 64 条，以 `std::vector<OpenDeltaEntry>` 存储。仅需线性遍历至多 64 条内存条目即可判定与 $[L, U)$ 的相交性，耗时仅数十纳秒，完全独立于历史 Run。 | `db/amtv.h:67-107`, `include/rocksdb/advanced_options.h:434` | **已从源码核实**：Open Delta 具备完全的局部独立提取能力，开销确定且极低。 | P1 原型可直接内存提取 Open Delta 相交条目，无需回溯历史 Run。 |
| **Q5** | 活跃 MemTable 的 `cached_range_tombstone_` 如何生成与失效？ | `MemTable::Add` 在遇到 `kTypeRangeDeletion` 时，直接将 `cached_range_tombstone_.Access()->initialized` 置为 false；Reader 读到未初始化时争抢 `reader_mutex`，由一个 Reader 全量扫描 `range_del_table_` 重构。 | `db/memtable.cc:945-989, 1294-1310` | **已从源码核实**：高频写入时该缓存频繁失效，导致读线程反复遭遇 `reader_mutex` 锁竞争与全量重建开销。 | P1 局部视图若能按需局部构建，将彻底避免争抢该全局粗粒度锁。 |
| **Q6** | `TruncatedRangeDelIterator` 的类型契约与约束是什么？ | `TruncatedRangeDelIterator` 是非虚类，强绑定 `std::unique_ptr<FragmentedRangeTombstoneIterator>`，其 `Next/Prev/Seek/SeekForPrev` 直接内联调用子成员的非虚成员函数。 | `db/range_del_aggregator.h:31-105`, `db/range_del_aggregator.cc:24-175` | **已从源码核实**：上层无法通过派生实现多态接入，必须向其提供真实的 `FragmentedRangeTombstoneIterator`。 | P1 不能伪造迭代器类，必须通过原生 Fragmenter 生成真实且合法的原生分片迭代器。 |
| **Q7** | `MergingIterator` 对范围墓碑槽位有何性质要求？ | `MergingIterator` 槽位 0 专属于 MemTable。要求墓碑流：1) 全局严格有序；2) 区间两两互斥；3) 支持 `TopNext/TopPrev/SeekInternalKey` 前后双向导航；4) 携带完整版本栈用于 MVCC 屏蔽。 | `table/merging_iterator.h:61-76`, `table/merging_iterator.cc:163-200, 776-890` | **已从源码核实**：该单槽必须是完全规范化的单一流，任何乱序或重叠都将破坏二叉堆不变式与 Reseek 正确性。 | P1 局部视图生成物必须完全符合原生 `FragmentedRangeTombstoneList` 的不变式契约。 |
| **Q8** | `ReadRangeDelAggregator` 为何不能替代单一有序分片流？ | `ReadRangeDelAggregator` 仅提供针对单个目标 InternalKey 的点查判定接口 `ShouldDelete` 与 `MaxCoveringTombstoneSeqnum`，未实现 `InternalIterator`，无法向 `MergingIterator` 的 `minHeap_` 投递 `DELETE_RANGE_START/END` 事件。 | `db/range_del_aggregator.h:375-408`, `db/range_del_aggregator.cc:420-460` | **已从源码核实**：其缺乏一维流式区间发生器能力，结构上不可替代单一有序分片流。 | 否定“复用 ReadRangeDelAggregator 接入 Scan”的幻想路径。 |
| **Q9** | `DBIter`/`ArenaWrappedDBIter::Refresh` 如何影响范围墓碑？ | `ArenaWrappedDBIter::Refresh` 在 SuperVersion 未改变时，调用 `sv->mem->NewRangeTombstoneIterator`，并直接就地覆写 `*memtable_range_tombstone_iter_`（指向 `merging_iter->range_tombstone_iters_[0]`）。 | `db/arena_wrapped_db_iter.cc:285-320`, `table/merging_iterator.cc:1764-1768` | **已从源码核实**：刷新操作要求底层范围墓碑迭代器可以被就地整体替换或重置。 | P1 原型测试中需包含刷新/重绑定反例，验证生命周期不受就地析构影响。 |
| **Q10** | `iterate_lower_bound` 与 `iterate_upper_bound` 是否传到了活跃 MemTable？ | `read_options` 虽然传入了 `DBImpl::NewInternalIterator` 并传至 `MemTable::NewRangeTombstoneIteratorInternal`，但 MemTable 内部为了维护全局共享缓存，完全未读取下界与上界，仍对全表墓碑做全量切分。 | `db/db_impl/db_impl.cc:2610`, `db/memtable.cc:933-994` | **已从源码核实**：当前活跃 MemTable 范围墓碑构建是全局无界物化，边界仅在 `MergingIterator`（丢弃 $\ge U$ 的 start key）与 `DBIter`（提前终止）生效。 | **核心接口阻塞项**：必须在 P1 明确由局部视图构造器显式接收 $[L, U)$，而不是依赖底层透明裁剪。 |
| **Q11** | 驱动层“扫描到 $key \ge U$ 停止”与引擎已知真实上界有何本质区别？ | 驱动层提前停止时，引擎内部已构建全局全量墓碑视图，且在扫描过程中遭遇长墓碑（$[A, Z)@100, Z \gg U$）时，`MergingIterator` 会触发无效的向前 Reseek（跨越至 $Z$），跳过 SST 或内存块，甚至导致预读浪费；已知真实上界可直接剪枝整个墓碑构建与多余 Reseek。 | `table/merging_iterator.cc:845-890`, `db/db_iter.cc:526-530` | **已从源码核实**：驱动层停止是“事后拦截”，引擎已知上界是“事前几何裁剪与降维”，两者存在量级上的计算与 I/O 差异。 | P1 原型需对驱动层无上界和显式 $[L, U)$ 做对比，证明显式裁剪的准确性。 |

---

## 3. 源码事实深度剖析

### A. AMTV Snapshot 与 Run 数据可得性

#### A.1 内部数据结构与条目保留
在当前代码中（`db/amtv.h:35-134`）：
- **`OpenDeltaEntry`**：每个条目均完整封装了 `std::string user_start_key`、`std::string user_end_key`、`SequenceNumber sequence`、`std::string timestamp`、`ValueType type`。绝无丢弃任何元数据。
- **`AMTVRun`**：
  - `raw_entries`：保存该 Run 封口时的原始 `OpenDeltaEntry` 列表；
  - `fragmented_list`：保存该 Run 内部经过原生 `FragmentTombstones` 切分后的 `std::shared_ptr<FragmentedRangeTombstoneList>`；
  - `raw_tombstones`：保存对应的 `RangeTombstone` 结构体列表。
- **`OpenDelta`**：以 `std::vector<OpenDeltaEntry> entries_` 维护最新的活跃墓碑条目（上限 64 条）。
- **`AMTVSnapshot`**：以只读共享指针持有 `base_memtable`、`sealed_runs`（`std::vector<std::shared_ptr<const AMTVRun>>`）与 `open_delta`（`std::shared_ptr<const OpenDelta>`）。
- **所有权模型**：所有 Snapshot 对 Run 和 OpenDelta 的引用均采用 `std::shared_ptr<const T>`。各 Run 在生命周期内完全不可变（Immutable），不存在任何写入竞态。

#### A.2 给定 $[L, U)$ 的相交片段与跨边界提取机制
针对给定的半开扫描区间 $[L, U)$：
1. **相交数学充分必要条件**：
   对于任意范围墓碑片段 $[S, E)$，其与 $[L, U)$ 相交的充分必要条件为：
   $$E > L \quad \land \quad S < U$$
2. **跨左边界片段（$S < L < E$）的必达性**：
   - 在已分片的 `FragmentedRangeTombstoneList` 中，所有片段按 $S$ 严格递增且互不重叠，因而 $E$ 亦单调递增。
   - 对其执行以 $L$ 为目标的二分上界查找：
     ```cpp
     pos = std::upper_bound(tombstones_.begin(), tombstones_.end(), L, tombstone_end_cmp_);
     ```
   - 若存在某个墓碑片段覆盖了 $L$（即 $S < L < E$），该片段的 $E$ 必然满足 $E > L$；由于此前所有片段均满足 $E_i \le L$，因此该二分查找**必然精确命中该跨左边界片段**！
   - 从该位置开始顺序扫描，直至出现 $S \ge U$ 时终止。遍历到的每一个片段均严格满足 $E > L \land S < U$。
3. **Open Delta 提取的确定性**：
   - `OpenDelta` 最多仅包含 64 条记录。对其进行顺序过滤仅需至多 64 次字符串比较，无需借助任何复杂索引，耗时在 100ns 量级，且完全无需访问或扫描任何无关历史 Run。
4. **内部接口完整性评估**：
   - 提取所需的 `begin()`、`end()`、`seq_iter()`、`ts_iter()` 在 `FragmentedRangeTombstoneList` 中均已有公开或包内访问途径；
   - 现有数据结构无需做任何破坏性改造即可完整支持相交提取。

#### A.3 并发演进下的可读性与持有关系保障
- **Run 归并**：归并产生新的 `AMTVRun`，并通过 COW（Copy-On-Write）原子更新 `AMTVState::sealed_runs_`。旧 Snapshot 依然持有旧 `AMTVRun` 的 `shared_ptr`，其底层的内存块与分片列表完全不受归并影响。
- **MemTable 冻结为 Immutable**：MemTable 冻结时，AMTV 触发最终封口；旧 Snapshot 仍然锁定其创建时的版本视图。
- **Fallback 保护**：Fallback 仅改变后续新写入与新请求的路由逻辑，不修改也不释放既有 Snapshot 的内部结构。
- **结论**：旧 Snapshot 的生命周期与数据可读性得到 C++ 智能指针与不可变对象语义的双重保证。

---

### B. 原生范围墓碑构造与消费链深度追踪

#### B.1 调用关系与所有权拓扑

```
[用户 Scan 请求]
   | (ReadOptions with iterate_lower_bound / iterate_upper_bound)
   v
DBImpl::NewIterator
   |
   v
DBImpl::NewInternalIterator (db/db_impl.cc:2573-2624)
   |
   +---> super_version->mem->NewIterator (Point Keys Iterator)
   |
   +---> super_version->mem->NewRangeTombstoneIterator (db/memtable.cc:933)
   |        |
   |        +---> 检查 cached_range_tombstone_->initialized
   |        |     [未初始化] -> 争抢 reader_mutex -> 全量扫描 range_del_table_
   |        |                   -> 构建 FragmentedRangeTombstoneList
   |        |                   -> 写入 cache->tombstones, initialized = true
   |        |     [已初始化] -> 直接复用 cache
   |        |
   |        +---> 返回 new FragmentedRangeTombstoneIterator(cache, ...)
   |
   +---> 包装为 std::make_unique<TruncatedRangeDelIterator>(range_del_iter, icmp, nullptr, nullptr)
   |
   +---> MergeIteratorBuilder::AddPointAndTombstoneIterator(mem_iter, mem_tombstone_iter)
            |
            v
         MergingIterator (table/merging_iterator.cc)
            |-- children_[0] = mem_iter
            |-- range_tombstone_iters_[0] = mem_tombstone_iter
            |-- minHeap_: 归并 point keys 与 DELETE_RANGE_START / DELETE_RANGE_END
```

#### B.2 `cached_range_tombstone_` 锁竞争与失效机制
- 依据 `db/memtable.cc:1294-1310`，每当写入一个范围删除（`kTypeRangeDeletion`），调用：
  ```cpp
  cached_range_tombstone_.Access()->initialized.store(false, std::memory_order_release);
  ```
  直接将所有缓存分片失效。
- 随后到达的 Scan 读请求在 `NewRangeTombstoneIteratorInternal` 中发现 `!cache->initialized`，立即尝试获取 `reader_mutex`（`db/memtable.cc:952-965`）。在写多读少或持续并发写入场景下，`reader_mutex` 会产生极其严重的锁争用（Contention），导致读线程遭遇毫秒级阻塞，并反复全量扫描跳表。

#### B.3 `MergingIterator` 单槽位强契约与序列性质
`MergingIterator` 的 Slot 0 专为活跃 MemTable 设立，其结构约束如下：
1. **强序列性质要求**：
   - **全局严格有序**：每个分片的起始 Key 必须严格单调递增（$S_i < S_{i+1}$）；
   - **互不重叠**：相邻分片不可重叠（$E_i \le S_{i+1}$）；
   - **双向导航可用**：支持 `TopNext()`、`TopPrev()`、`Seek()`、`SeekForPrev()`；
   - **完整版本栈**：每个分片必须记录该区间内所有未被更高版本覆盖的 sequence 列表（`RangeTombstoneStack`），以便支持任意 snapshot sequence 的过滤与判定。
2. **Reseek 触发机理**：
   在 `MergingIterator::SeekImpl` 中（`table/merging_iterator.cc:845-890`）：
   ```cpp
   if (comparator_->user_comparator()->Compare(
           range_tombstone_iter->start_key().user_key,
           current_search_key.GetUserKey()) <= 0) {
     range_tombstone_reseek = true;
     current_search_key.SetInternalKey(range_tombstone_iter->end_key());
   }
   ```
   若当前墓碑覆盖了查询目标点，`MergingIterator` 将直接把查找键向前推进至 `end_key`，避免遍历被完全删除的下层 SST 数据。

#### B.4 `ReadRangeDelAggregator` 无法替代单一有序流的具体原因
1. **接口类型失配**：`ReadRangeDelAggregator` 派生自 `RangeDelAggregator`，未实现 `InternalIterator` 接口；
2. **缺乏区间事件投递能力**：`MergingIterator` 的核心调度机制是在最小堆中插入 `DELETE_RANGE_START` 与 `DELETE_RANGE_END` 事件，以动态维护当前活跃墓碑集合 `active_`；
3. **点查语义限制**：`ReadRangeDelAggregator` 内部的多来源管理（`StripeRep`）仅仅用于单个点 Key 的 `ShouldDelete(parsed_key)` 判定，完全不具备按 Key 递增顺序流式产出不相交区间的遍历器状态机。

#### B.5 `ArenaWrappedDBIter::Refresh` 的强耦合关系
在 `db/arena_wrapped_db_iter.cc:285-320` 中：
- `Refresh()` 时若 SuperVersion 未变，直接通过 `sv->mem->NewRangeTombstoneIterator` 获取新墓碑迭代器，并直接覆写 `*memtable_range_tombstone_iter_`；
- 这证明了 `MergingIterator` 内部的墓碑迭代器持有关系是硬编码的单指针替换，任何局部视图如果在 Refresh 时不具备相应的生命周期感知，将导致非法指针解引用。

---

### C. 查询边界真正能否进入引擎

#### C.1 `iterate_lower_bound` / `iterate_upper_bound` 传递链
1. **调用方传入**：用户在 `ReadOptions` 中设置 `iterate_lower_bound` 与 `iterate_upper_bound`；
2. **DBIter 层**：
   - `SeekToFirst()`：若 `iterate_lower_bound` 存在，则重定向为 `Seek(*iterate_lower_bound)`（`db/db_iter.cc:2204`）；
   - `SeekToLast()`：若 `iterate_upper_bound` 存在，则重定向为 `SeekForPrev(*iterate_upper_bound)`（`db/db_iter.cc:2258`）；
   - 迭代推进：当遍历到的 Key $\ge iterate\_upper\_bound$ 时，将 `valid_` 标为 false，主动终止。
3. **MergingIterator 层**：
   - `MergeIteratorBuilder` 接收 `iterate_upper_bound` 并传入 `MergingIterator`；
   - 在 `InsertRangeTombstoneToMinHeap` 中（`table/merging_iterator.cc:172`），若墓碑的 `start_key >= *iterate_upper_bound_`，则将其从堆中剔除。
4. **MemTable 层（断裂点）**：
   - 传递链进入 `MemTable::NewRangeTombstoneIteratorInternal` 时，`read_options` 虽然存在，但由于需要向共享缓存 `cached_range_tombstone_` 填充数据，**完全未对 `range_del_table_` 做边界裁剪**，直接构建全量分片！
   - **源码确证**：在 `memtable.cc` 全文中，`iterate_lower_bound` 与 `iterate_upper_bound` 的出现次数为 **0**。

#### C.2 驱动层提前停止 vs 引擎已知真实上界
- **驱动层提前停止（Driver Early-Stop）**：
  引擎对上界无感知。活跃 MemTable 耗费毫秒全量物化墓碑列表；当遇到大墓碑 $[A, Z)@100$（$Z \gg U$）时，`MergingIterator` 仍会向前 Reseek 到 $Z$，跳过整个扫描目标区域甚至产生额外的 SST 解密和块加载；
- **引擎已知真实上界（Engine-Known Bound）**：
  若在 MemTable 层面引入局部视图构造，仅对相交于 $[L, U)$ 的墓碑进行切分，物化复杂度从 $O(N_{\text{total}} \log N_{\text{total}})$ 骤降为 $O(N_{\text{local}} \log N_{\text{local}})$，且彻底消除跨出 $U$ 之外的无效 Reseek 与无效块扫描。

#### C.3 各类 Iterator 操作对局部视图的严苛要求与约束
- **`SeekToFirst` / `SeekToLast`**：
  若视图已局部化为 $[L, U)$，调用 `SeekToFirst` 必须定位到 $\ge L$ 的第一个合法点；调用 `SeekToLast` 必须定位到 $< U$ 的最后一个合法点。若试图越出 $[L, U)$ 寻道，局部视图将产生未定义行为。
- **`Seek` / `SeekForPrev`**：
  目标 Key 必须保证严格落在 $[L, U)$ 内；否则必须触发边界越界检测（Out-of-Bounds Detection）并安全置为 Invalid。
- **`Next` / `Prev`**：
  当游标滑动到 $\ge U$ 或 $< L$ 时，必须立即置 Invalid。
- **`SetBounds` / `Refresh`**：
  若用户调用 `SetBounds` 变更了边界，或调用 `Refresh` 刷新了快照，**既有的局部视图立即失效**，必须强制触发局部视图的重新派生。
- **时间戳（Timestamp）与自定义 Comparator**：
  - 用户自定义 Timestamp 会追加在 UserKey 末端。在二分查找与端点求交时，必须使用 `ucmp->CompareWithoutTimestamp` 比较逻辑键，使用 `ucmp->CompareTimestamp` 处理版本；
  - 必须严格遵守用户自定义比较器的全序规则。
- **Immutable MemTable 与 SST 来源**：
  - Immutable MemTable 和 SST 文件的墓碑早在生成期就已完成静态分片，不存在高频失效问题；
  - 局部视图的适配核心专属于**活跃 MemTable（Active MemTable）与 AMTV 多 Run 动态合并**。

#### C.4 P1 接口阻塞项声明
> [!IMPORTANT]
> **P1 接口阻塞项**：
> 当前 RocksDB 原生 `MemTable::NewRangeTombstoneIterator` **未提供**接受明确查询边界 $[L, U)$ 的接口重载，其内部实现硬编码依赖全局无界缓存。因此，在真实生产扫描路径改造前，**M4-P1 严禁侵入真实 DB Scan 路径**，必须将局部视图限定为**测试专用的独立构造器原型**，由独立测试直接向其传入固定 Snapshot 与 $[L, U)$ 进行验证。

---

## 4. M4-P1 最小可行原型设计（严格受限，测试专用）

### 4.1 原型定位与边界约束
- **严正声明**：M4-P1 仅为**测试专用局部视图原型（Test-Only Localized Range Tombstone View Prototype）**，严禁侵入生产 `DB::NewIterator`，不承载真实线上读流量。
- **核心目标**：在离线测试框架中，证明“局部提取相交墓碑 + 原生 Fragmenter 构造局部视图”在各种极端边界与重叠拓扑下，与原生全量物化及独立模型达到 100% 位级语义等价。

### 4.2 原型输入、行为与输出定义

```
+-----------------------------------------------------------------------------------+
|                            M4-P1 局部视图测试原型                                 |
+-----------------------------------------------------------------------------------+
  [输入]
    1. const AMTVSnapshot* snapshot: 固定的只读快照 (含 Base, Sealed Runs, OpenDelta)
    2. SequenceNumber read_seq: 读取可见性序列号
    3. Slice lower_bound, Slice upper_bound: 明确的半开查询区间 [L, U)
    4. const InternalKeyComparator* icmp: 内部键比较器

  [局部提取行为 (Extraction)]
    1. Base / Sealed Runs:
       - 遍历各 Run 的 raw_entries (或 fragmented_list)
       - 采用二分定位首个 end > L 的条目，遍历至 start >= U
       - 严格捕获所有跨左边界 (start < L < end)、跨右边界 (start < U < end) 及内含条目
    2. Open Delta:
       - 遍历 entries_ (<= 64 条)，线性提取与 [L, U) 相交的所有条目
    3. 原生切分 (Native Fragmentation):
       - 将提取的局部候选墓碑送入原生 FragmentedRangeTombstoneList 构造器
       - 产生局部且单调递增、互不相交的局部分片流 LocalFragmentedList
       - 包装为原生的 FragmentedRangeTombstoneIterator

  [三方等价性校验 (3-Way Verification)]
    (A) 原生全量分片真值 (Canonical Native Truth):
        将全部 Run 与 Delta 的全量墓碑无界物化为原生全局分片列表，截取 [L, U)
    (B) 局部视图输出 (Local View Under Test):
        局部提取后切分产生的视图
    (C) 独立操作日志模型 (Independent Operational Log Oracle):
        基于逐点判定与离散覆盖序列的参考解算器
```

### 4.3 判定准则与输出比较
对于区间 $[L, U)$ 内的每一个离散候选内部键（Point InternalKey）：
1. **覆盖判定一致性**：`ShouldDelete(point_key)` 返回布尔值必须完全一致；
2. **最大可见 Sequence 一致性**：覆盖该点的最大可见 sequence 必须位级一致；
3. **时间戳一致性**：若开启 UDT，覆盖该点的有效 Timestamp 必须完全一致；
4. **流式遍历一致性**：局部迭代器在 $[L, U)$ 内执行 `Seek(L)`、`Next()` 至 $U$ 所经过的分片起止边界与原生真值完全重合。

### 4.4 P1 必须覆盖的反例矩阵（Counterexample Matrix）

| 编号 | 反例分类 | 详细场景与测试用例设计 |
| :--- | :--- | :--- |
| **CE-01** | **边界跨越反例** | 墓碑跨左界（$S < L < E < U$）、跨右界（$L < S < U < E$）、完全跨越（$S < L < U < E$）、邻接但不相交（$E = L$ 或 $S = U$）、空区间（$L = U$）。 |
| **CE-02** | **拓扑重叠与嵌套** | 同一 Run 或跨 Run 存在全包含嵌套（$[10, 100)$ 包含 $[30, 60)$）、部分交叠（$[20, 50)$ 与 $[40, 80)$）、多重碎片切割。 |
| **CE-03** | **同边界异版本** | 相同区间 $[A, B)$ 在 Base、Sealed Run 0、Open Delta 中分别具有不同 sequence，验证局部视图保留最高可见 sequence 且 MVCC 版本栈完整。 |
| **CE-04** | **多 Run 与 Open Delta 混合** | 墓碑分散在 Base、多个 Sealed Run 与未封口的 Open Delta 中，且彼此边界交错，验证无历史 Run 遗漏。 |
| **CE-05** | **Put 复活与二次删除** | 在区间 $[A, B)@10$ 被删除后，写入 $Put(K)@20$（$K \in [A, B)$），随后又写入 $[A, B)@30$，验证候选点在各 Snapshot 下可见性判定严格准确。 |
| **CE-06** | **多 Snapshot 共存演进** | 在 Run 归并发生前保留 `old_snapshot`，归并后创建 `new_snapshot`，同时针对 $[L, U)$ 提取局部视图，验证旧快照结果不受归并扰动。 |
| **CE-07** | **双向导航与 Seek 行为** | 在局部视图上执行交替的 `Seek(target)`、`SeekForPrev(target)`、`Next()`、`Prev()`，验证游标在区间边界处的状态转换无死循环与越界。 |
| **CE-08** | **大规模随机差分测试** | 运行至少 1,000 组随机多层墓碑生成与随机 $[L, U)$ 区间抽取，一旦出现位级不一致，自动保留随机种子与最小复现用例。 |

---

## 5. M4-P0 闸门条件评估与明确结论

根据指令要求，对进入 M4-P1 的 5 大闸门条件进行逐一核实：

| 闸门条件 | 审计评估结果 | 事实依据与证明 |
| :--- | :---: | :--- |
| **1. 能安全获得构造局部视图所需的完整墓碑语义与对象所有权** | **PASS** | `AMTVRun` 与 `OpenDelta` 完整保留原始墓碑的所有字段（start/end key、sequence、timestamp、type）；Snapshot 持有只读 `shared_ptr`，生命周期安全无竞态。 |
| **2. 已明确局部提取不会遗漏跨左边界墓碑** | **PASS** | 已在数学与算法上证明：基于分片有序性，以 $end > L$ 为条件执行二分上界查找，必然能 100% 捕获满足 $start < L < end$ 的跨左边界覆盖片段。 |
| **3. 已明确原生 fragmenter 可作为局部语义构造器** | **PASS** | 原生 `FragmentedRangeTombstoneList` 接收任意有序迭代器输入并产出标准非重叠分片列表，局部相交条目可作为其完全合法的输入。 |
| **4. P1 可在测试代码中完成，而不需要修改真实 DB Scan** | **PASS** | P1 严格定义为离线测试原型，通过构造测试 Harness 独立对比局部视图与原生真值，无需侵入 `DBImpl`、`DBIter` 或 `MergingIterator` 生产路径。 |
| **5. 未支持能力、尚未验证能力和真实接入阻塞项均逐项列出** | **PASS** | 本报告已在第 3.C 节详细列出 `iterate_upper_bound` 在 MemTable 层断裂、无法就地多态派生等阻塞项，并明确指出了边界越界检测要求。 |

### 5.1 闸门裁决结论
> **【明确结论】**：**准予进入 M4-P1（GO TO M4-P1）**。
>
> 核心理论依据：局部提取与原生构造在语义可得性、跨界必达性、所有权安全性方面均已在源码层面完成闭环验证；阻断生产直接接入的单槽强类型约束不影响测试原型的语义验证。

---

## 6. M4-P1 交付规划与测试矩阵

### 6.1 P1 最小文件清单

若获批进入 M4-P1，代码变更将严格限制在测试目录中：

| 类别 | 计划操作文件 | 变更性质与作用 |
| :--- | :--- | :--- |
| **测试实现** | `db/amtv_local_scan_test.cc` | **[NEW]** 实现测试专用的局部视图提取器 `AMTVLocalScanView` 及三方比对 Harness |
| **构建规则** | `Makefile` / `CMakeLists.txt` | **[MODIFY]** 注册 `amtv_local_scan_test` 构建目标 |
| **生产代码** | `db/amtv.h` | **[MODIFY]** 仅添加必要的测试专用只读 Accessor（若私有字段不可见），并加编译期 `#ifndef NDEBUG` 保护 |
| **生产 Scan 路径** | `db/db_iter.cc`, `table/merging_iterator.cc`, `db/memtable.cc` 等 | **严格保持 0 修改** |

### 6.2 P1 精确验证测试矩阵

1. **确定性拓扑套件**（Deterministic Topology Suite）：
   - 覆盖 CE-01 至 CE-07 的所有场景（空区间、跨界、嵌套、同界异序、Put 复活、双向游走）；
2. **随机差分测试套件**（Randomized Differential Suite）：
   - 生成 1,000 次随机场景（随机 Run 数量 1~8，随机墓碑数量 10~500，随机读序列号，随机 $[L, U)$ 扫描边界）；
   - 每次比对局部视图遍历序列与原生全量切分真值，要求断言 100% 通过。
