# M4-P2b-0 窗口来源与真实迭代器契约审计报告

## 1. 执行准则与范围声明

* **真实 Scan 生产路径 0 修改**：保持 `DBIter`、`MergingIterator`、`ArenaWrappedDBIter`、`MemTable::NewRangeTombstoneIteratorInternal`、`RangeDelAggregator` 等真实 Scan 消费路径 0 修改。
* **阶段 A 只读审计**：本阶段严格限定为只读调用链追踪、原生迭代器行为事实核验与安全性资格推导；不进入阶段 B（消费兼容性测试原型），不接入真实 Scan 生产路径，不运行任何性能评测。
* **可追溯性保证**：本报告直接归档于仓库内 `docs/M4-P2b-window-and-consumer-contract-audit.md`。

---

## 2. 真实调用链完整追踪

通过对 RocksDB v11.8.0 源码的只读审计，真实 Scan 迭代器创建、组装与消费调用链如下：

```
[User Code]
   │
   ▼ db->NewIterator(read_options, column_family)
[DBImpl::NewIterator] (db/db_impl/db_impl.cc:4132)
   │  • 校验 read_options（io_activity, timestamp, read_tier 等）
   │  • 获取当前 SuperVersion: sv = cfd->GetReferencedSuperVersion(this)
   │  • 捕获快照 sequence: snapshot = (read_options.snapshot ? seq : versions_->LastSequence())
   ▼
[DBImpl::NewIteratorImpl] (db/db_impl/db_impl.cc:4213)
   │  • 准备连续 Arena 内存布局（ArenaWrappedDBIter 内部包含 Arena）
   ▼
[NewArenaWrappedDbIterator] (db/arena_wrapped_db_iter.cc:361)
   │  • 创建 ArenaWrappedDBIter 实例
   │  • 调用 db_iter->Init(...) 初始化包装层 DBIter，但不立即构造底层子迭代器（惰性延迟初始化）
   │  • 暂存 deferred DB 状态: db_impl, cfd, sv, sequence
   ▼
[User First Operation: Seek(target) / SeekToFirst() / Prepare()]
   │
   ▼ ArenaWrappedDBIter::EnsureInternalIteratorInitialized() (db/arena_wrapped_db_iter.cc:79)
[DBImpl::NewInternalIterator] (db/db_impl/db_impl.cc:2573)
   │  • 构造 MergeIteratorBuilder(&cfd->internal_comparator(), arena, ..., read_options.iterate_upper_bound)
   │
   ├─► [1. Active Mutable MemTable] (db/db_impl/db_impl.cc:2603-2618)
   │     • point_iter = super_version->mem->NewIterator(read_options, ...)
   │     • range_del_iter = super_version->mem->NewRangeTombstoneIterator(read_options, sequence, false)
   │     • mem_tombstone_iter = std::make_unique<TruncatedRangeDelIterator>(
   │           std::move(range_del_iter), &cfd->internal_comparator, nullptr, nullptr);
   │     • merge_iter_builder.AddPointAndTombstoneIterator(point_iter, std::move(mem_tombstone_iter));
   │
   ├─► [2. Immutable MemTables] (db/db_impl/db_impl.cc:2636)
   │     • super_version->imm->AddIterators(..., &merge_iter_builder, ...)
   │
   ├─► [3. SST Files (L0 - Ln)] (db/db_impl/db_impl.cc:2651)
   │     • super_version->current->AddIterators(read_options, file_options_, &merge_iter_builder, ...)
   │
   ▼ merge_iter_builder.Finish(db_iter) (table/merging_iterator.cc:1750)
   │  • 实例化 MergingIterator
   │  • 将 active memtable 的范围墓碑迭代器槽位地址绑定给 db_iter:
   │    db_iter->SetMemtableRangetombstoneIter(&merge_iter->range_tombstone_iters_.front())
   │
   ▼ SetIterUnderDBIterImpl(internal_iter) (db/arena_wrapped_db_iter.cc:103)
      • DBIter 内部成员 iter_ 指向 MergingIterator
```

---

## 3. 窗口可见性与确定性时间点审计

审计针对以下关键参数在各层级对象的可见位置与可用时间窗口进行了明确界定：

| 参数 / 操作 | 来源 | 可见层级与对象 | 首次可见时间点 | 是否足以在构造期确定局部窗口 $[L, U)$ |
| :--- | :--- | :--- | :--- | :---: |
| `ReadOptions.iterate_lower_bound` | 用户传入 | `DBImpl::NewIterator`、`ArenaWrappedDBIter`、`DBIter`、`MemTable::NewRangeTombstoneIterator` | `NewIterator` 调用时 | **是**（若用户显式指定） |
| `ReadOptions.iterate_upper_bound` | 用户传入 | `DBImpl::NewIterator`、`ArenaWrappedDBIter`、`DBIter`、`MergeIteratorBuilder` | `NewIterator` 调用时 | **是**（若用户显式指定） |
| 用户首次 `Seek(target)` | 客户端指令 | `ArenaWrappedDBIter::Seek`、`DBIter::Seek`、`MergingIterator::Seek` | 构造完成后用户显式调用 | **否**（仅代表起点，非区间，不含上界） |
| 用户 `SeekToFirst()` | 客户端指令 | `ArenaWrappedDBIter::SeekToFirst`、`DBIter::SeekToFirst` | 构造完成后用户显式调用 | **否**（无显式键，扫描整个键空间） |
| 用户 `SeekToLast()` | 客户端指令 | `ArenaWrappedDBIter::SeekToLast`、`DBIter::SeekToLast` | 构造完成后用户显式调用 | **否**（反向全扫描，下界不确定） |

### 核心结论：局部窗口 $[L, U)$ 的可见性与延迟初始化确定性
1. **捕获与使用时机分离**：
   - 用户传入的 `ReadOptions.iterate_lower_bound` 与 `iterate_upper_bound` 在 `DBImpl::NewIterator` 调用时被捕获并保存于 `DBIter` 状态中；
   - 真实的底阶内部迭代器（包括活跃 MemTable 的点/范围墓碑迭代器）在 `NewIterator` 阶段**并不立即构造**，而是遵循 RocksDB 惰性延迟初始化机制，在客户端首次调用 `Seek()` / `SeekToFirst()` 时于 `ArenaWrappedDBIter::EnsureInternalIteratorInitialized()` -> `DBImpl::NewInternalIterator()` 中统一构造；
   - 因此，局部视图的判断与构造时机应落在首次延迟初始化 `NewInternalIterator`（或未来刷新）中，但其窗口确定性前提依然是在 `NewIterator` 创建期就已由捕获的 `ReadOptions` 完整提供。
2. **所有权铁律：边界 Key 内容必须深拷贝**：
   - 未来局部视图 `State` 必须**深拷贝** `lower_bound` 与 `upper_bound` 的实际 key 内容（转换为拥有型 `std::string`）；
   - **严禁**直接保存或依赖外部 `ReadOptions.iterate_lower_bound` / `iterate_upper_bound` 的 `Slice` 指针或外部生命周期（防止客户端提前释放或修改 options 缓冲区）；
   - 在用户定义时间戳（UDT）场景下，还必须在深拷贝后严格校验并保留时间戳格式。
3. **不可推断性**：若客户端未在 `ReadOptions` 中指定完整上下界：
   - 随后的 `Seek(target)` 只能提供遍历起始点，客户端随后可以通过任意次数的 `Next()` 扫描至无穷大，绝不构成右边界 $U$；
   - 任何试图“等待用户调用首次 `Seek` 时动态猜测/构造局部视图”的做法，在面对跨界 `Next()`、反向 `Prev()` 或动态重新 `Seek` 时必然发生墓碑漏选（Under-coverage），严重违反快照一致性。
4. **架构铁律**：**局部视图的生命周期必须绑定在静态已知的确切有界扫描（Bounded Scan）上**；无界或半有界扫描必须在构造期直接决策回退原生全量视图。

---

## 4. 原生完整 `TruncatedRangeDelIterator` 基线行为表

为了脱离测试包装器（如 `WindowGuard`）获得 RocksDB 原生截断机制的真实契约，在 `db/amtv_local_scan_reference_test.cc` 中通过 `P2b_NativeTruncatedRangeDelIteratorBaseline` 执行了无修饰的原生接口事实核验。

* **测试拓扑**：
  * 原始范围墓碑：`[k10, k50)@100`、`[k35, k65)@140`、`[k50, k90)@120`；
  * 原生切分片段（全量）：`[k10, k35)@100`、`[k35, k50)@140`、`[k50, k65)@140`、`[k65, k90)@120`；
  * 截断窗口：$[L, U) = [\text{"k30"}, \text{"k70"})$；
  * 内部键边界：`smallest = (k30, kMaxSequenceNumber, kTypeRangeDeletion)`, `largest = (k70, kMaxSequenceNumber, kTypeRangeDeletion)`。

### 原生 `TruncatedRangeDelIterator` 事实行为核验表

| 操作 (Operation) | 目标键 (Target) | 调用是否成功 | `Valid()` | `status()` | `start_key()` | `end_key()` | `seq()` | 原生底层行为与机理解析 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `Seek` | `k20 (L-ε)` | 是 | **TRUE** | `Status::OK()` | `k30` | `k35` | 100 | **原生自动收缩到 L**：`target < smallest` 时，原生逻辑自动重定向为 `iter_->Seek(smallest_->user_key)`，定位并裁剪至覆盖 L 的首个片段 `[k30, k35)`。 |
| `Seek` | `k30 (L)` | 是 | **TRUE** | `Status::OK()` | `k30` | `k35` | 100 | **左边界精确命中**：定位首个在窗片段 `[k30, k35)`。 |
| `Seek` | `k40 (mid)` | 是 | **TRUE** | `Status::OK()` | `k35` | `k50` | 140 | **内部定位**：精准定位到覆盖 `k40` 的片段 `[k35, k50)`。 |
| `Seek` | `k70 (U)` | 是 | **FALSE** | `Status::OK()` | `N/A` | `N/A` | 0 | **上界判为无效**：`target == largest_` 时判定越界，`iter_->Invalidate()`，`Valid() == false`。 |
| `Seek` | `k80 (U+ε)` | 是 | **FALSE** | `Status::OK()` | `N/A` | `N/A` | 0 | **右越界判为无效**：`target > largest_`，直接使底层迭代器失效，`Valid() == false`。 |
| `SeekForPrev` | `k80 (U+ε)` | 是 | **TRUE** | `Status::OK()` | `k65` | `k70` | 120 | **原生自动收缩到 U**：`target > largest` 时，原生逻辑重定向为 `iter_->SeekForPrev(largest_->user_key)`，命中在窗最后有效片段 `[k65, k70)`。 |
| `SeekForPrev` | `k70 (U)` | 是 | **TRUE** | `Status::OK()` | `k65` | `k70` | 120 | **上界哨兵寻位**：搜索 $start \le U$ 的墓碑，并在边界截断至 `end = k70`，有效命中 `[k65, k70)`。 |
| `SeekForPrev` | `k40 (mid)` | 是 | **TRUE** | `Status::OK()` | `k35` | `k50` | 140 | **内部反向定位**：准确定位覆盖 `k40` 的在窗片段 `[k35, k50)`。 |
| `SeekForPrev` | `k30 (L)` | 是 | **TRUE** | `Status::OK()` | `k30` | `k35` | 100 | **左边界寻位**：定位覆盖 L 的有效片段 `[k30, k35)`。 |
| `SeekForPrev` | `k20 (L-ε)` | 是 | **FALSE** | `Status::OK()` | `N/A` | `N/A` | 0 | **左越界判为无效**：`target < smallest` 时反向无可见在窗墓碑，直接失效。 |
| `SeekToFirst` | `-` | 是 | **TRUE** | `Status::OK()` | `k30` | `k35` | 100 | 定位窗口内起始片段 `[k30, k35)`。 |
| `Next (step 1)` | `-` | 是 | **TRUE** | `Status::OK()` | `k35` | `k50` | 140 | 前向推进一步。 |
| `Next (step 2)` | `-` | 是 | **TRUE** | `Status::OK()` | `k50` | `k65` | 140 | 前向推进一步。 |
| `Next (step 3)` | `-` | 是 | **TRUE** | `Status::OK()` | `k65` | `k70` | 120 | 前向推进一步（末尾在窗片段，被 `largest_` 截断至 `k70`）。 |
| `Next (step 4)` | `-` | 是 | **FALSE** | `Status::OK()` | `N/A` | `N/A` | 0 | **右边界耗尽**：下一步遇到 `start >= k70`，`Valid()` 变为 `false`。 |
| `SeekToLast` | `-` | 是 | **TRUE** | `Status::OK()` | `k65` | `k70` | 120 | 定位窗口内末尾片段 `[k65, k70)`。 |
| `Prev (step 1)` | `-` | 是 | **TRUE** | `Status::OK()` | `k50` | `k65` | 140 | 反向后退一步。 |
| `Prev (step 2)` | `-` | 是 | **TRUE** | `Status::OK()` | `k35` | `k50` | 140 | 反向后退一步。 |
| `Prev (step 3)` | `-` | 是 | **TRUE** | `Status::OK()` | `k30` | `k35` | 100 | 反向后退一步（首个在窗片段，被 `smallest_` 截断至 `k30`）。 |
| `Prev (step 4)` | `-` | 是 | **FALSE** | `Status::OK()` | `N/A` | `N/A` | 0 | **左边界耗尽**：下一步超出左侧窗口边界，`Valid()` 变为 `false`。 |

### 原生基线行为的重要发现（对设计的重要修正）
1. **原生迭代器绝不抛出 `InvalidArgument` 异常**：
   RocksDB 原生 `TruncatedRangeDelIterator` 对于越界目标（例如 `Seek(L-ε)` 或 `SeekForPrev(U+ε)`），其规范处理行为是**自动边界收缩（Clamping）**或**静默失效（Invalidate）**，调用始终成功且返回 `Status::OK()`。
2. **`WindowGuard` 的定位**：
   P2a 中的 `WindowGuard` 采用的 `InvalidArgument` 报错策略仅属于严格的“测试越界探测守卫”，**绝不能作为生产 `InternalIterator` 的契约行为**。在阶段 B 的真实 `MergingIterator` 消费兼容性测试中，必须恢复原生无阻碍截断行为。

---

## 5. 活跃 MemTable 在 MergingIterator 中的槽位与生命周期机制

### 5.1 槽位数量确定性
* `MergingIterator` 维护向量 `range_tombstone_iters_`，其大小严格与 `children_.size()` 一致。
* 活跃 MemTable 在其数据未被剪枝（`!memtable_pruned_`）的情况下，**永久且确定地占据第 0 号槽位（Slot 0 / Index 0 / front）**。
* 当活跃 MemTable 无范围删除时，Slot 0 放置 `nullptr`；当包含范围删除时，放置 `std::unique_ptr<TruncatedRangeDelIterator>`。

### 5.2 对象所有权与生命周期
* **所有权**：Slot 0 中的 `TruncatedRangeDelIterator` 由 `MergingIterator::range_tombstone_iters_[0]` 唯一所有。
* **借用指针**：`ArenaWrappedDBIter` 内部持有 `std::unique_ptr<TruncatedRangeDelIterator>* memtable_range_tombstone_iter_`，直接指向 `MergingIterator` 的 Slot 0。
* **生命周期**：整个 `MergingIterator` 及所有 Slot 均分配于 `ArenaWrappedDBIter` 内置的 `Arena` 上。外部迭代器析构或触发 Full Refresh 时，Arena 整体释放，不存在跨迭代器的生命周期残留。

### 5.3 Refresh / SuperVersion 变化时的重建边界

```
                       [ArenaWrappedDBIter::Refresh()]
                                      │
                         Is sv_number_ == cur_sv_number ?
                                     / \
                                    /   \
                             YES   /     \   NO
                                  /       \
      ┌──────────────────────────┘         └─────────────────────────┐
      ▼                                                              ▼
[In-Place Active MemTable Refresh]                          [DoRefresh: Complete Teardown]
  1. 获取活跃 MemTable 新 tombstone iter t                     1. 析构 DBIter 与 Arena:
  2. 若原 Slot 0 非空且 t 非空:                                  DestroyDBIterAndArena()
     直接在原地覆写 Slot 0:                                   2. 重新绑定最新 SuperVersion
     *memtable_range_tombstone_iter_ =                      3. 全新分配 Arena
       make_unique<TruncatedRangeDelIterator>(...)          4. 重新完整执行 NewInternalIterator()
  3. 若原 Slot 0 为空但 t 产生新墓碑:                           5. 彻底重建包括 MergingIterator
     原地无法扩容，退化进入 DoRefresh 整体重建                   在内的整个迭代器树
```

* **安全边界推论**：
  若在 Slot 0 中注入 AMTV 局部视图迭代器，在 `sv_number_ == cur_sv_number` 的原地 Refresh 路径中，如果不加处理，RocksDB 原生逻辑会无条件调用 `sv->mem->NewRangeTombstoneIterator` 覆盖该 Slot 0。因此，生产接入设计必须在此处拦截，确保原地 Refresh 同步维护局部视图，或在发生变更时安全触发完整重建。

---

## 6. 安全资格规则（Safety Eligibility Rules - Version 1）

为了杜绝在动态复杂查询中发生范围墓碑漏选或迭代器越界，第一版候选准入策略制定如下安全铁律：

### 准入四重门（Must Satisfy ALL 4 Conditions）：
1. **对象限定**：必须仅针对当前正在写入的**活跃 MemTable**（Active Mutable MemTable）；Immutable MemTable 与 SST 文件 100% 走原生路径。
2. **开关使能**：ColumnFamily 配置显式启用 AMTV（`mutable_cf_options.amtv_enabled == true`）。
3. **显式双向有界**：用户传入的 `ReadOptions` 中，`iterate_lower_bound != nullptr` **且** `iterate_upper_bound != nullptr`。
4. **有效开区间**：`user_comparator->CompareWithoutTimestamp(*lower, false, *upper, false) < 0`。

### 强制回退原生规则（Strict Fallback to Native）：
* 只要上述任一条件不满足（例如无界扫描、单侧有界扫描、前缀扫描未显式指定双边界、空区间），**100% 回退 RocksDB 原生全量范围墓碑视图**（`MemTable::NewRangeTombstoneIteratorInternal`）。
* 严禁依赖任何“等待用户后续调用 `Seek()` 时再探测或构造局部视图”的假设。

### 未来 Local State 所有权与 UDT 强约束：
1. **边界深拷贝**：未来局部视图 `State` 必须深拷贝 `iterate_lower_bound` 与 `iterate_upper_bound` 对应的 key 内容到由 State 独立拥有的 `std::string`，绝不能保存外部 `Slice` 指针或依赖其生命周期；
2. **UDT 格式与时间戳校验**：在启用用户定义时间戳时，深拷贝后必须保留并校验时间戳长度与格式，截断边界必须严格对齐原生时间戳规范。

---

## 7. 阶段 A 准出结论与停机状态

* 阶段 A 审计任务全部达成，调用链、时间窗口、原生基线表、槽位生命周期与资格规则已完整证明并归档。
* **当前代码完全未改动真实 Scan 生产路径**。
* 本阶段在此正式停机，等待审核通过后再行开启阶段 B。
