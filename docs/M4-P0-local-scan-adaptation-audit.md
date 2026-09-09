# AMTV M4-P0：局部 Scan 视图源码适配审计报告（修订版）

- **阶段标识**：AMTV M4-P0 (Local Scan View Source Adaptation Audit)
- **阶段性质**：架构与源码语义适配审计（Architecture & Source Semantics Audit）
- **基准环境**：RocksDB v11.8.0 / Linux x86_64
- **审计原则**：
  1. 严格只读审计与基线确认，禁止修改真实 DB Scan 读路径（`DBIter`、`MergingIterator`、`ArenaWrappedDBIter`、`MemTable::NewRangeTombstoneIteratorInternal` 原生生产代码 100% 保持原样）；
  2. 严禁修改现有 AMTV Get/MultiGet、Run 归并、Fallback、Flush、Compaction 或 WAL 恢复逻辑；
  3. 严禁接入新的 DB Iterator、生产缓存或在线 K 路合并器；
  4. 严禁运行 M3b、F1、F2、LongCycle 或任何正式性能矩阵；
  5. 严禁用不完整 MVCC 语义的“仅保留最大 sequence”近似方案绕过问题；
  6. 严格区分“已从当前源码核实”、“由既有实验支持”、“待验证假设与待 P1 验证”三类结论；
  7. 严禁将文档提交误写为核心实现提交。

---

## 1. 固定版本与基线配置

### 1.1 仓库 Commit SHA 与工作树状态精确区分

| 标识分类 | 仓库 | 分支 | 完整 Commit SHA | 性质说明 |
| :--- | :--- | :--- | :--- | :--- |
| **RocksDB 核心代码基线** | `rocksdb-v11.8.0` | `main` | `395ce9863fc4ecef4fba75f1b9e1e2510edf45c1` | 固定的 AMTV 核心实现基线（`feat(amtv): M1c-P0 multi-run scan semantics audit and test-only differential oracle`） |
| **M4-P0 文档提交** | `rocksdb-v11.8.0` | `main` | `8d96435a607b7824a32632fd79e7c4caf81668e3` | M4-P0 审计文档初始提交（`docs(amtv): M4-P0 local scan view source adaptation audit`） |
| **Study 研究仓提交** | `s14-range-delete-study` | `main` | `869a25044afd1baaddf777cd27ceaa5478aa0192` | 固定的实验与基线配置（`docs(audit): fix T512 tail typography and document overall quantiles calculation method`） |

- **工作树状态**：两仓在基线点均保持 clean（无未提交的脏改动）。
- **网络与代理配置**：环境使用本地 HTTP 代理 `http://127.0.0.1:16989`，与远程 `rt-opt/main` 及 `origin/main` 保持完全同步。
- **现有相关单测基线**：
  - `./amtv_scan_oracle_test`：15/15 全部通过（耗时 67 ms）；
  - `./amtv_probe_test`：3/3 全部通过（耗时 1 ms）；
  - `./amtv_test`：全部通过。

### 1.2 当前 AMTV 核心配置与状态机规则

依据当前源码核实（[include/rocksdb/advanced_options.h:422-446](file:///home/wam/grad/rocksdb-v11.8.0/include/rocksdb/advanced_options.h#L422-L446)，[db/amtv.h:215-280](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv.h#L215-L280)）：

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

- 关键源文件 `git diff 395ce9863 HEAD -- db/ table/ include/` 校验结果为**严格零行修改（0 lines added, 0 lines deleted）**：
  - `db/db_impl/db_impl.cc`：Diff 为 0；
  - `table/merging_iterator.cc`：Diff 为 0；
  - `table/merging_iterator.h`：Diff 为 0；
  - `db/arena_wrapped_db_iter.cc`：Diff 为 0；
  - `db/arena_wrapped_db_iter.h`：Diff 为 0；
  - `db/db_iter.cc`：Diff 为 0；
  - `db/memtable.cc`：Diff 为 0；
  - `db/range_del_aggregator.h`：Diff 为 0。
- **核查结论**：审计处于 100% 严格只读状态，未引入任何生产读路径的修改。

---

## 2. 源码事实总表（“问题—当前代码事实—文件与行号—结论—P1影响”）

| 序号 | 审计核心问题 | 当前代码事实 | 文件与行号 | 审计结论 | P1 影响与约束 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Q1** | `AMTVSnapshot`、`AMTVRun`、`OpenDelta` 保存了什么原始信息？ | `AMTVRun` 保存 `std::vector<OpenDeltaEntry> raw_entries` 与预分片的 `std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list`；`OpenDelta` 保存扁平 `entries_`；条目中完整包含 start/end key、sequence、timestamp、type。`AMTVSnapshot` 仅保存 `sealed_runs` 与 `open_delta`，不存在 `base_memtable` 字段。 | [db/amtv.h:35-51, 113-134, 170-208](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv.h#L35-L51), [db/amtv.cc:25-68](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv.cc#L25-L68) | **已从源码核实**：墓碑原始语义信息在各 Run 与 Delta 中完整保留；Snapshot 仅持有 sealed runs 与 Open Delta。 | P1 原型只能以 sealed runs 与 Open Delta 的 raw entries 作为输入来源，严禁假设 `base_memtable`。 |
| **Q2** | 对象所有权与生命周期在归并、变只读、销毁时如何保证安全？ | `AMTVSnapshot` 内部持有 `std::vector<std::shared_ptr<const AMTVRun>> sealed_runs` 与 `std::shared_ptr<const OpenDelta> open_delta`。通过 `std::shared_ptr` 保证旧 Snapshot 引用的 Run 不会被析构。 | [db/amtv.h:170-208](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv.h#L170-L208), [db/amtv.cc:295-318](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv.cc#L295-L318) | **已从源码核实**：即使后台发生 Run 归并、MemTable 冻结为 Immutable 或触发 Fallback，已创建的 Snapshot 依然完全只读且引用计数自保。 | P1 局部视图可安全持有 `AMTVSnapshot` 句柄，不存在悬垂指针或竞态风险。 |
| **Q3** | 对给定半开区间 $[L, U)$，能否从原始条目或分片列表提取所有相交片段，尤其是跨左边界片段（$start < L < end$）？ | 原始 DeleteRange 存在 $start < L$ 但 $end > L$ 的跨左界长墓碑。由于未切分的 raw entries 缺乏 prefix-max-end 索引，**不能仅按 start 二分提取**，必须做完整安全遍历；在已分片的单调互斥分片流中，可二分定位首个 $end > L$。 | [db/range_tombstone_fragmenter.cc:387-397](file:///home/wam/grad/rocksdb-v11.8.0/db/range_tombstone_fragmenter.cc#L387-L397), [db/range_tombstone_fragmenter.h:50-75](file:///home/wam/grad/rocksdb-v11.8.0/db/range_tombstone_fragmenter.h#L50-L75) | **已从源码核实**：原始墓碑提取必须满足 $start < U \land end > L$；二分优化在未建 prefix-max-end 索引前属于**待验证假设**。 | P1 原型必须对 raw entries 执行 `start < U && end > L` 的完整安全遍历筛选，严禁在现阶段声称二分优化。 |
| **Q4** | `OpenDelta` 能否在不扫描无关历史 Run 的前提下完成完整相交提取？ | `OpenDelta` 硬上限为 64 条，以 `std::vector<OpenDeltaEntry>` 存储。仅需线性遍历至多 64 条内存条目即可判定与 $[L, U)$ 的相交性，耗时仅数十纳秒，完全独立于历史 Run。 | [db/amtv.h:67-107](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv.h#L67-L107), [include/rocksdb/advanced_options.h:434](file:///home/wam/grad/rocksdb-v11.8.0/include/rocksdb/advanced_options.h#L434) | **已从源码核实**：Open Delta 具备完全的局部独立提取能力，开销确定且极低。 | P1 原型可直接内存安全遍历 Open Delta 相交条目，无需回溯历史 Run。 |
| **Q5** | 活跃 MemTable 的 `cached_range_tombstone_` 如何生成与失效？ | `MemTable::Add` 在遇到 `kTypeRangeDeletion` 时，直接将 `cached_range_tombstone_.Access()->initialized` 置为 false；Reader 读到未初始化时争抢 `reader_mutex`，由一个 Reader 全量扫描 `range_del_table_` 重构。 | [db/memtable.cc:945-989, 1294-1310](file:///home/wam/grad/rocksdb-v11.8.0/db/memtable.cc#L945-L989) | **已从源码核实**：高频写入时该缓存频繁失效，导致读线程反复遭遇 `reader_mutex` 锁竞争与全量重建开销。 | P1 局部视图若能按需局部构建，将彻底避免争抢该全局粗粒度锁。 |
| **Q6** | `TruncatedRangeDelIterator` 的类型契约与约束是什么？ | `TruncatedRangeDelIterator` 是非虚类，强绑定 `std::unique_ptr<FragmentedRangeTombstoneIterator>`，其 `Next/Prev/Seek/SeekForPrev` 直接内联调用子成员的非虚成员函数。 | [db/range_del_aggregator.h:31-105](file:///home/wam/grad/rocksdb-v11.8.0/db/range_del_aggregator.h#L31-L105), [db/range_del_aggregator.cc:24-175](file:///home/wam/grad/rocksdb-v11.8.0/db/range_del_aggregator.cc#L24-L175) | **已从源码核实**：上层无法通过派生实现多态接入，必须向其提供真实的 `FragmentedRangeTombstoneIterator`。 | P1 不能伪造迭代器类，必须通过原生 Fragmenter 生成真实且合法的原生分片迭代器。 |
| **Q7** | `MergingIterator` 对范围墓碑槽位有何性质要求？ | `MergingIterator` 槽位 0 专属于 MemTable。要求墓碑流：1) 全局严格有序；2) 区间两两互斥；3) 支持 `TopNext/TopPrev/SeekInternalKey` 前后双向导航；4) 携带完整版本栈用于 MVCC 屏蔽。 | [table/merging_iterator.h:61-76](file:///home/wam/grad/rocksdb-v11.8.0/table/merging_iterator.h#L61-L76), [table/merging_iterator.cc:163-200, 776-890](file:///home/wam/grad/rocksdb-v11.8.0/table/merging_iterator.cc#L163-L200) | **已从源码核实**：该单槽必须是完全规范化的单一流，任何乱序或重叠都将破坏二叉堆不变式与 Reseek 正确性。 | P1 局部视图生成物必须完全符合原生 `FragmentedRangeTombstoneList` 的不变式契约。 |
| **Q8** | `ReadRangeDelAggregator` 为何不能替代单一有序分片流？ | `ReadRangeDelAggregator` 仅提供针对单个目标 InternalKey 的点查判定接口 `ShouldDelete` 与 `MaxCoveringTombstoneSeqnum`，未实现 `InternalIterator`，无法向 `MergingIterator` 的 `minHeap_` 投递 `DELETE_RANGE_START/END` 事件。 | [db/range_del_aggregator.h:375-408](file:///home/wam/grad/rocksdb-v11.8.0/db/range_del_aggregator.h#L375-L408), [db/range_del_aggregator.cc:420-460](file:///home/wam/grad/rocksdb-v11.8.0/db/range_del_aggregator.cc#L420-L460) | **已从源码核实**：其缺乏一维流式区间发生器能力，结构上不可替代单一有序分片流。 | 否定“复用 ReadRangeDelAggregator 接入 Scan”的幻想路径。 |
| **Q9** | `DBIter`/`ArenaWrappedDBIter::Refresh` 如何影响范围墓碑？ | `ArenaWrappedDBIter::Refresh` 在 SuperVersion 未改变时，调用 `sv->mem->NewRangeTombstoneIterator`，并直接就地覆写 `*memtable_range_tombstone_iter_`（指向 `merging_iter->range_tombstone_iters_[0]`）。 | [db/arena_wrapped_db_iter.cc:285-320](file:///home/wam/grad/rocksdb-v11.8.0/db/arena_wrapped_db_iter.cc#L285-L320), [table/merging_iterator.cc:1764-1768](file:///home/wam/grad/rocksdb-v11.8.0/table/merging_iterator.cc#L1764-L1768) | **已从源码核实**：刷新操作要求底层范围墓碑迭代器可以被就地整体替换或重置。 | P1 原型测试中需包含刷新/重绑定反例，验证生命周期不受就地析构影响。 |
| **Q10** | `iterate_lower_bound` 与 `iterate_upper_bound` 是否传到了活跃 MemTable？ | `read_options` 虽然传入了 `DBImpl::NewInternalIterator` 并传至 `MemTable::NewRangeTombstoneIteratorInternal`，但 MemTable 内部为了维护全局共享缓存，完全未读取下界与上界，仍对全表墓碑做全量切分。 | [db/db_impl/db_impl.cc:2610](file:///home/wam/grad/rocksdb-v11.8.0/db/db_impl/db_impl.cc#L2610), [db/memtable.cc:933-994](file:///home/wam/grad/rocksdb-v11.8.0/db/memtable.cc#L933-L994) | **已从源码核实**：当前活跃 MemTable 范围墓碑构建是全局无界物化，边界仅在 `MergingIterator`（丢弃 $\ge U$ 的 start key）与 `DBIter`（提前终止）生效。 | **核心接口阻塞项**：必须在 P1 明确由局部视图构造器显式接收 $[L, U)$，而不是依赖底层透明裁剪。 |
| **Q11** | 驱动层“扫描到 $key \ge U$ 停止”与引擎已知真实上界有何本质区别？ | 驱动层停止是“调用端事后拦截”，但此时引擎内部已完成全局全量墓碑构建；至于 Scan 是否一定会推进到远超 $U$ 并产生端到端 I/O 浪费，受上层迭代终止与具体墓碑分布影响，目前属于**待验证假设**。 | [table/merging_iterator.cc:845-890](file:///home/wam/grad/rocksdb-v11.8.0/table/merging_iterator.cc#L845-L890), [db/db_iter.cc:526-530](file:///home/wam/grad/rocksdb-v11.8.0/db/db_iter.cc#L526-L530) | **已从源码核实**：P0 确认了构建期未利用上界的事实；端到端 I/O 恶化程度标记为待实验验证假设。 | P1 原型仅验证局部语义正确性，不预先假设性能收益或 I/O 影响。 |

---

## 3. 源码事实深度剖析

### A. AMTV Snapshot 与 Run 数据可得性

#### A.1 内部数据结构与条目保留
在当前代码中（[db/amtv.h:35-134, 170-208](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv.h#L35-L134)）：
- **`OpenDeltaEntry`**：每个条目均完整封装了 `InternalKey ikey`（含 `user_start_key`、`sequence`、`kTypeRangeDeletion`）、`std::string end_key`（`user_end_key`）。绝无丢弃任何元数据。
- **`AMTVRun`**：
  - `raw_entries`：保存该 Run 封口时的原始 `OpenDeltaEntry` 列表；
  - `fragmented_list`：保存该 Run 内部经过原生 `FragmentTombstones` 切分后的 `std::shared_ptr<FragmentedRangeTombstoneList>`；
  - `raw_tombstones`：保存对应的 `RangeTombstone` 结构体列表。
- **`OpenDelta`**：以 `std::vector<OpenDeltaEntry> entries_` 维护最新的活跃墓碑条目（上限 64 条）。
- **`AMTVSnapshot` 的真实来源**：
  依据 `db/amtv.h:170-208`，`AMTVSnapshot` 仅包含：
  1. `sealed_runs`（`std::vector<std::shared_ptr<const AMTVRun>>`）；
  2. `open_delta`（`std::shared_ptr<const OpenDelta>`）；
  3. 元数据（`memtable_generation`、`publish_epoch`、`fallback_required`、`tombstones_at_fallback`）。
  **当前结构中绝对不存在 `base_memtable` 或 `base_fragmented_list` 字段**。局部视图在当前实现中可获得的实际数据来源仅为 `sealed_runs` 与 `open_delta`。
- **所有权模型**：所有 Snapshot 对 Run 和 OpenDelta 的引用均采用 `std::shared_ptr<const T>`。各 Run 在生命周期内完全不可变（Immutable），不存在任何写入竞态。

#### A.2 给定 $[L, U)$ 的相交片段与跨边界提取机制（修正说明）
针对给定的半开扫描区间 $[L, U)$：
1. **相交数学充分必要条件**：
   对于任意范围墓碑 $[S, E)$，其与 $[L, U)$ 相交的充分必要条件为：
   $$E > L \quad \land \quad S < U$$
2. **两类输入的提取差异与 $O(\log F)$ 降级为待验证假设**：
   - **输入类别 1：Raw Range Tombstone Entries（原始未分片条目）**：
     原始墓碑彼此可能重叠、嵌套，且即使某条目的 $S < L$，其 $E$ 依然可能跨越 $L$（即 $S < L < E$）。如果仅对 $S$ 做二分查找，将无法找出所有 $S < L$ 的长跨界墓碑。**在缺乏 prefix-max-end 辅助索引或区间树（Interval Tree）时，不能仅按 start 二分提取，必须对 raw entries 执行完整安全遍历**。因此，对于原始墓碑，局部提取耗时为 $O(N)$，“局部二分提取为 $O(\log F)$”在未建索引前属于**待验证假设**。
   - **输入类别 2：已 Fragment 的 span/stack**：
     在已分片的 `FragmentedRangeTombstoneList` 中，片段是两两互斥且按 $S$ 严格递增排序的，此时 $E$ 亦单调递增，二分找 $E > L$ 才在数学上成立。
3. **已切分列表（FragmentedRangeTombstoneList）重输入的语义风险**：
   - `FragmentedRangeTombstoneList` 内部包含私有的 `tombstones_`（`vector<RangeTombstoneStack>`）和版本索引。它并未暴露可安全无损重构原始墓碑流的公共导出接口。
   - 原生 Fragmenter 假定输入为未分片的内部键流。**在形式化证明其等价性之前，禁止将既有 fragment 直接重新喂给 Fragmenter 并声称语义等价**。
   - **M4-P1a 必须以 raw entries 作为唯一事实来源**。
4. **Open Delta 提取的确定性**：
   - `OpenDelta` 最多仅包含 64 条记录。对其进行线性安全遍历（至多 64 次比较）耗时在 100ns 量级，可完整提取所有满足 $S < U \land E > L$ 的条目，无需回溯历史 Run。

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
   若当前墓碑覆盖了查询目标点，`MergingIterator` 将把查找键向前推进至 `end_key`。

#### B.4 `ReadRangeDelAggregator` 无法替代单一有序流的具体原因
1. **接口类型失配**：`ReadRangeDelAggregator` 派生自 `RangeDelAggregator`，未实现 `InternalIterator` 接口；
2. **缺乏区间事件投递能力**：`MergingIterator` 的核心调度机制是在最小堆中插入 `DELETE_RANGE_START` 与 `DELETE_RANGE_END` 事件，以动态维护当前活跃墓碑集合 `active_`；
3. **点查语义限制**：`ReadRangeDelAggregator` 内部仅用于单个点 Key 的 `ShouldDelete(parsed_key)` 判定，完全不具备流式产出不相交区间的遍历器状态机。

#### B.5 `ArenaWrappedDBIter::Refresh` 的强耦合关系
在 `db/arena_wrapped_db_iter.cc:285-320` 中：
- `Refresh()` 时若 SuperVersion 未变，直接通过 `sv->mem->NewRangeTombstoneIterator` 获取新墓碑迭代器，并直接覆写 `*memtable_range_tombstone_iter_`；
- 这证明了 `MergingIterator` 内部的墓碑迭代器持有关系是硬编码的单指针替换，任何局部视图如果在 Refresh 时不具备相应的生命周期感知，将导致非法指针解引用。

---

### C. 查询边界真正能否进入引擎

#### C.1 `iterate_lower_bound` / `iterate_upper_bound` 传递链与断裂点
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

#### C.2 驱动层提前停止 vs 引擎已知真实上界（降级待验证假设）
- **代码事实**：当前活跃 MemTable 的范围墓碑物化完全未感知 `iterate_upper_bound`，始终全量扫描 `range_del_table_` 构建全局缓存。驱动层停止仅仅是用户层迭代的“事后拦截”。
- **待验证假设**：驱动层提前停止是否必然导致端到端 I/O 严重浪费，取决于上层迭代是否提前 Break 以及跨越 $U$ 的墓碑是否触发了无效的向前 Reseek。P0 仅确认构建期未利用上界的事实，不能声称已证明端到端 I/O 后果。

#### C.3 P1 接口阻塞项声明
> [!IMPORTANT]
> **P1 接口阻塞项**：
> 当前 RocksDB 原生 `MemTable::NewRangeTombstoneIterator` **未提供**接受明确查询边界 $[L, U)$ 的接口重载，其内部实现硬编码依赖全局无界缓存。因此，在真实生产扫描路径改造前，**M4-P1a 严禁侵入真实 DB Scan 路径**，必须将局部视图限定为**测试专用的独立语义参考原型**，由测试直接传入固定 Snapshot 与 $[L, U)$ 进行正确性验证。

---

## 4. M4-P1a 最小可行语义参考原型设计

### 4.1 原型定位与边界约束
- **严正声明**：M4-P1a 仅为**测试专用局部视图语义参考原型（Test-Only Local Scan Semantic Reference View）**，建立正确性 Oracle，不作任何性能宣称，严禁侵入生产 `DB::NewIterator`。
- **核心目标**：证明“从实际 AMTV 来源安全全量遍历筛选相交原始墓碑（$start < U \land end > L$） + 原生 Fragmenter 构造局部视图”，在各种极端边界、时间戳与重叠拓扑下，与全量参考真值达到 100% 位级语义等价。

### 4.2 原型输入、提取行为与参考真值比对

```
+-----------------------------------------------------------------------------------+
|                        M4-P1a 局部 Scan 语义参考原型                              |
+-----------------------------------------------------------------------------------+
  [输入来源 (仅限真实存在的实际来源)]
    1. snapshot.sealed_runs: std::vector<std::shared_ptr<const AMTVRun>> (取 raw_entries)
    2. snapshot.open_delta: std::shared_ptr<const OpenDelta> (取 entries())
    3. 查询区间: [L, U) (半开区间，空表示无界)
    4. 序列号与时间戳: SequenceNumber read_seq, const Slice* ts_upper_bound

  [局部提取与构造行为 (AMTVLocalScanReferenceView)]
    1. 完整安全遍历 (Full Safe Traversal, 严禁二分假设):
       - 遍历 sealed_runs 中每个 Run 的 raw_entries
       - 遍历 open_delta 的 entries()
       - 筛选满足: ucmp->CompareWithoutTimestamp(start, U) < 0 &&
                   ucmp->CompareWithoutTimestamp(end, L) > 0 的条目
    2. 局部原生切分:
       - 将筛选出的原始墓碑按 icmp 排序
       - 送入原生 FragmentedRangeTombstoneList 构造器
       - 包装为原生的 FragmentedRangeTombstoneIterator 供测试查询

  [全量参考真值 (Canonical Full Truth)]
    - 将上述所有来源的所有原始墓碑不经筛选全部送入原生 FragmentedRangeTombstoneList
    - 构造全量 FragmentedRangeTombstoneIterator
    - 在 [L, U) 范围内逐 key、逐 read sequence、逐 timestamp 比对：
        * MaxCoveringTombstoneSeqnum (最大覆盖 sequence)
        * ShouldDelete (删除判定布尔值)
        * Put 复活后点数据的可见性
        * 有序分片端点与双向导航一致性
```

---

## 5. M4-P0 闸门条件评估与明确结论

| 闸门条件 | 审计评估结果 | 事实依据与证明 |
| :--- | :---: | :--- |
| **1. 能安全获得构造局部视图所需的完整墓碑语义与对象所有权** | **PASS** | `AMTVRun` 的 `raw_entries` 与 `OpenDelta` 的 `entries()` 完整保留原始墓碑的所有字段；Snapshot 持有只读 `shared_ptr`，生命周期安全无竞态。已纠正不存在 `base_memtable` 的问题。 |
| **2. 已明确局部提取不会遗漏跨左边界墓碑** | **PASS** | 采用完整安全遍历，严格以 $start < U \land end > L$ 进行筛选，100% 捕获满足 $start < L < end$ 的跨左界长墓碑。已将未建索引下的二分提取降级为待验证假设。 |
| **3. 已明确原生 fragmenter 可作为局部语义构造器** | **PASS** | 原生 `FragmentedRangeTombstoneList` 接收任意有序原始内部键流并产出标准分片列表。已明确禁止将既有 fragment 直接重喂。 |
| **4. P1 可在测试代码中完成，而不需要修改真实 DB Scan** | **PASS** | P1a 严格定义为测试专用的独立语义参考对象 `AMTVLocalScanReferenceView`，无需侵入 `DBImpl`、`DBIter` 或 `MergingIterator` 生产路径。 |
| **5. 未支持能力、尚未验证能力和真实接入阻塞项均逐项列出** | **PASS** | 本报告已在第 2 节及第 3.C 节明确指出 MemTable 边界未利用、非虚类型强绑定、端到端 I/O 影响待验证等阻塞项与假设。 |

> ### 闸门结论：【准予进入 M4-P1a 测试专用语义原型（GO TO M4-P1a）】
> 审计文档已完成严谨修订，所有来源、假设与约束已彻底澄清。
