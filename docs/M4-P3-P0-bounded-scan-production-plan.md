# AMTV M4-P3-P0 审计与实施方案（修订版）：AMTV 有界 Scan 最小生产接入与负载适用性

> **文档版本**：v2.0（根据 P3-P0 审计反馈全面修订）  
> **状态**：P3-P0 审计通过，拆分为 P3a-1 与 P3a-2 两阶段实施；P3a-1 实施中，P3a-2 待审核  
> **基准提交**：`52c1edb5b` (`rt-opt/main`)  

---

## 1. 目标与阶段范围界定

本方案旨在为 RocksDB 活跃 MemTable（Active MemTable）在有界范围扫描（Bounded Scan）场景下引入基于 AMTV 的局部范围墓碑视图（Bounded Local RangeDel View），将当前全量扫描构造从 $O(N_{\text{all\_tombstones}})$ 裁剪为仅处理与扫描区间 $[L, U)$ 相交的局部候选墓碑 $O(N_{\text{candidates}})$。

### 1.1 阶段拆分定义

为严格控制风险，实施过程严格拆分为两个顺序阶段：

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ M4-P3a-1：内部组件与统一工厂 (当前执行阶段)                              │
│ - 允许修改：                                                            │
│   • db/amtv_local_scan_view.h / .cc (局部状态、候选提取、统一工厂)       │
│   • MemTable 只读访问接口 (IsImmutable() 等)                            │
│   • include/rocksdb/advanced_options.h, options/cf_options.h/.cc        │
│   • 构建文件 (src.mk, CMakeLists.txt, Makefile)                         │
│   • 工厂单元测试 db/amtv_local_scan_view_test.cc                        │
│ - 严格禁止：                                                            │
│   • 暂不修改 DBImpl::NewInternalIterator 和 ArenaWrappedDBIter::Refresh │
│ - 准出条件：单元测试通过 Debug/Release/ASan，提交证据并立即停机验收      │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │ (审核批准后)
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ M4-P3a-2：真实 DBIter 接入与端到端集成测试 (后续阶段)                   │
│ - 接入点 1：DBImpl::NewInternalIterator 调用统一工厂                     │
│ - 接入点 2：ArenaWrappedDBIter::Refresh (Same-SV) 调用统一工厂           │
│ - 接入点 3：ArenaWrappedDBIter::DoRefresh (Full Rebuild) 自然继承       │
│ - 端到端测试：db/amtv_db_scan_integration_test.cc (T1–T12 对照)          │
│ - 包含并发写入/Refresh (建议 TSAN)、AMTV fallback 动态测试、全库摘要对账│
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 统一工厂返回契约与错误传播设计

### 2.1 统一工厂接口签名

禁止使用带有歧义的 `std::unique_ptr<TruncatedRangeDelIterator> Build...(..., Status* out_status = nullptr)` 接口。统一工厂规范签名如下：

```cpp
Status BuildActiveMemTableRangeDelIteratorForScan(
    MemTable* memtable,
    const ReadOptions& read_options,
    SequenceNumber read_seq,
    const InternalKeyComparator& icmp,
    bool enable_bounded_scan_view,
    std::unique_ptr<TruncatedRangeDelIterator>* out_iter,
    AMTVScanBuildMetadata* build_meta = nullptr);
```

### 2.2 严格契约规则

1. **入口重置**：入口第一步立即执行 `out_iter->reset()`（或指针为空时返回 `Status::InvalidArgument`）。
2. **合法空结果**：`Status::OK()` 且 `*out_iter == nullptr` 表示当前活跃 MemTable 中**没有有效的范围墓碑**，这是完全合法的正常结果，调用方（如 `DBImpl::NewInternalIterator` 或 `Refresh`）不得将其视为错误。
3. **回退到原生完整迭代器**：当不满足资格条件（未开启、无界、反向区间、非活跃 MemTable、AMTV 要求 fallback）或局部构建失败时，工厂在内部直接构造当前活跃 MemTable 的原生完整 `TruncatedRangeDelIterator`，并包装为 `[nullptr, nullptr]`（无界截断模式）。
4. **原生回退标记**：原生回退成功后返回 `Status::OK()`，并通过 `build_meta` 详细标记回退原因及所处模式（`kNativeFallback`）。
5. **非 OK 语义**：只有当无法提供当前正确的 RangeDel iterator 时（例如参数非法、或原生回退构造也失败返回错误）才返回非 OK `Status`。
6. **Refresh 无条件替换**：在 `ArenaWrappedDBIter::Refresh` 中，只要工厂返回 `Status::OK()`，必须无条件执行：
   ```cpp
   *memtable_range_tombstone_iter_ = std::move(new_iter);
   ```
   **即使 `new_iter == nullptr` 也必须执行替换以清除旧 Slot 0 迭代器**（防止在墓碑已被清除或失效时旧墓碑仍留在 Slot 0 误删数据）。
7. **禁止有条件替换**：严禁使用 `if (new_iter != nullptr)` 才进行替换的错误逻辑。

### 2.3 既有错误传播机制核验

- **`DBImpl::NewInternalIterator`**：
  经核验 RocksDB v11.8.0 源码（`db/db_impl/db_impl.cc:2670`），当内部状态非 OK 时，既有标准传播机制为：
  ```cpp
  if (!s.ok()) {
    CleanupSuperVersion(super_version);
    return NewErrorInternalIterator<Slice>(s, arena);
  }
  ```
  统一工厂返回非 OK 时，立即遵循此机制返回 `NewErrorInternalIterator<Slice>`，绝不吞掉错误。
- **`ArenaWrappedDBIter::Refresh`**：
  经核验（`db/arena_wrapped_db_iter.cc:334`），`Refresh` 自身返回 `Status`。若统一工厂返回非 OK，在释放 ThreadLocal SuperVersion 后直接向上返回该 `Status`，调用方捕获错误。

---

## 3. 错误处理分类与生命周期所有权

### 3.1 错误处理分类原则

取消“never throws”和“OOM 一定转换为 Status”等绝对承诺，严格区分三类错误场景：

1. **可预期业务回退（Predictable Fallbacks）**：
   - 包括：选项关闭、未设双边界、反向窗口（$L \ge U$）、非活跃 MemTable、AMTV 进入 fallback 状态、局部状态不完整等；
   - **处理机制**：通过正常控制流自动切换至原生 `memtable->NewRangeTombstoneIterator` 构造全量迭代器，返回 `Status::OK()`，记录 metadata。
2. **原生 Iterator 状态错误（Native Iterator Status Errors）**：
   - 原生构建或下层迭代器返回的不可恢复错误；
   - **处理机制**：直接向上传播 `Status`。
3. **内存分配失败（Allocation Failures）**：
   - 遵循 RocksDB 当前全局构建配置与 C++ 内存分配策略（如抛出 `std::bad_alloc` 或配置的内存分配失败处理程序）；
   - 不作任何超出底层环境的额外恢复承诺。
4. **故障注入机制**：
   - 单元测试与集成测试中全部使用 `TEST_SYNC_POINT` 或显式测试码注入局部构建失败，绝不依赖 C++ 异常捕获机制。

### 3.2 统一边界所有权设计（Official API Contract）

根据 RocksDB 官方 API 契约事实：`DBIter` 和 `ArenaWrappedDBIter` 仅浅拷贝（borrow）`ReadOptions.iterate_lower_bound` 与 `iterate_upper_bound` 的 `Slice` 指针，调用方必须保证其指向的底层内存空间在 Iterator 生命周期内持续有效。

在 M4-P3a 中统一执行如下所有权契约：
1. **调用方保活**：调用者按照官方规范保证 `ReadOptions` 边界在 Iterator 生命周期内有效；
2. **工厂内部深拷贝**：每次调用统一工厂（无论初次构造还是 Refresh），工厂第一步将当前 $L/U$ 字节深拷贝进新创建的独立 `LocalRangeDelViewState`（`lower_bound_bytes` 和 `upper_bound_bytes`）；
3. **InternalKey 借用**：新生成的 `TruncatedRangeDelIterator` 仅借用 `LocalRangeDelViewState` 内部拥有的 `smallest_ikey` 与 `largest_ikey` 指针；
4. **Aliasing Shared Pointer 生命周期绑定**：
   通过 C++11 Aliasing 构造函数：
   ```cpp
   std::shared_ptr<FragmentedRangeTombstoneList> aliased_list(
       state, state->fragmented_list.get());
   ```
   将其传入 `FragmentedRangeTombstoneIterator`。`FragmentedRangeTombstoneIterator` 持有 `aliased_list`，而 `TruncatedRangeDelIterator` 拥有 `FragmentedRangeTombstoneIterator`，Slot 0 拥有 `TruncatedRangeDelIterator`。
   **只要 Slot 0 存活，`LocalRangeDelViewState` 必存活；Slot 0 析构或被新迭代器替换时，旧 State 引用归零自动析构。**
5. **不扩展 DBIter 成员**：不宣称 `ArenaWrappedDBIter` 拥有边界，第一版不为 `ArenaWrappedDBIter` 增加第二份持久边界字段。

---

## 4. 性能表述与开销规范

- **基线开销承诺**：
  > 默认关闭（`amtv_enable_bounded_scan_view == false`）时必须保持功能行为与原生完全一致；实际开销将在后续 Release 基准中严格测量。
- **严禁虚假宣传**：当前阶段删除所有诸如“开销 <1ns”、“吞吐提升 X%”等未经测量的性能断言。

---

## 5. 构建系统集成与 Options 规范

### 5.1 构建系统文件注册

RocksDB 编译清单需同步维护：
1. **`src.mk`**：
   - `LIB_SOURCES`：新增 `db/amtv_local_scan_view.cc`
   - `TEST_MAIN_SOURCES`：新增 `db/amtv_local_scan_view_test.cc`
2. **`CMakeLists.txt`**：
   - `SOURCES`：新增 `db/amtv_local_scan_view.cc`
   - `TESTS`（`if(WITH_ALL_TESTS)`）：新增 `db/amtv_local_scan_view_test.cc`
3. **`Makefile`**：
   - 新增 `amtv_local_scan_view_test` 构建与链接规则：
     ```makefile
     amtv_local_scan_view_test: $(OBJ_DIR)/db/amtv_local_scan_view_test.o $(TEST_LIBRARY) $(LIBRARY)
     	$(AM_LINK)
     ```

### 5.2 Options 规范与完备性测试

在 `AdvancedColumnFamilyOptions` 与 `ImmutableCFOptions` 中新增：
```cpp
bool amtv_enable_bounded_scan_view = false;
```
并在 `options/cf_options.cc` 中注册映射 `"amtv_enable_bounded_scan_view"`。

**必须覆盖的 Options 单元测试**：
1. **默认值**：验证默认必须为 `false`；
2. **Options String 解析**：`GetColumnFamilyOptionsFromString` 能正确解析 `"amtv_enable_bounded_scan_view=true"` 及 `"false"`；
3. **Options 序列化恢复**：`GetStringFromColumnFamilyOptions` 输出包含该配置项，且重新反序列化后值完全恢复；
4. **非法值报错**：传入 `"amtv_enable_bounded_scan_view=invalid_val"` 时返回非 OK 状态；
5. **DB Reopen 行为**：带此选项打开、写入 OPTIONS 文件后 reopen，配置正确持久化；
6. **Native 行为零变化**：默认配置下，所有原生扫描行为、墓碑过滤行为与基线无任何差异。

---

## 6. 现有实验负载适用性审计结论固化

通过对 `rt-study` 现有全部扫描驱动（`e8_micro_driver`、`m3a_driver`、`formal_driver`、`longcycle_driver`、`read_path_audit_driver`、`m2d_driver`）的只读审计，确认：

1. **现有负载 100% 无法触发 Local View**：
   所有既有 Scan 实现均传入未设置双边界的默认 `ReadOptions`（`lower_bound == nullptr && upper_bound == nullptr`），依赖客户端循环条件 `key >= end_k` 或计数器 `count < N` 终止。由于双边界为空，**全部走原生全量路径**。
2. **严禁混淆评价**：不得使用既有 E8、M3b、F1/F2、LongCycle 的旧实验数据评价 AMTV Scan 的效果。
3. **负载分类与规划**：
   - **`SCAN_RANGE`**：属于显式范围查询语义，后续将构造独立的、语义等价的新 driver（如 `tools/amtv_m4/bounded_scan_driver.cc`），显式填充 `ReadOptions` 双边界进行评测；
   - **`SCAN_LIMIT`**：属于计数终止语义（从 $L$ 开始取 $N$ 个合规 key），事先无法预测第 $N$ 个 key 的具体位置，**不能直接转换为固定的 upper bound**；
   - **命名与报告隔离**：未来所有有界 Scan 实验必须与旧负载在命名、驱动源码、报告文件上彻底隔离，禁止混为一谈。

---

## 7. M4-P3a 两阶段实施细则与验收标准

### 7.1 M4-P3a-1：内部组件与统一工厂（当前阶段）

#### 实施任务
1. **头文件与源文件**：
   - `db/amtv_local_scan_view.h`：定义 `AMTVScanMode`、`AMTVScanBuildMetadata`、`LocalRangeDelViewState`、统一工厂声明；
   - `db/amtv_local_scan_view.cc`：实现候选墓碑提取、原生 Fragmenter 组装、Aliasing shared_ptr 绑定、统一工厂与 Native fallback 逻辑；
2. **只读访问接口**：
   - 在 `db/memtable.h` 中增加 `bool IsImmutable() const`；
3. **Options 与构建文件**：
   - 更新 `include/rocksdb/advanced_options.h`、`options/cf_options.h`、`options/cf_options.cc`；
   - 更新 `src.mk`、`CMakeLists.txt`、`Makefile`；
4. **工厂单元测试**：
   - `db/amtv_local_scan_view_test.cc`：覆盖 Local 命中、Native fallback（关闭、无界、反向、AMTV fallback）、空墓碑合法返回 `OK + nullptr`、SyncPoint 故障注入 Native 回退、Options 完备测试、生命周期 ASan 验证。

#### 准出与验收要求
1. 各种状态（Local、Native fallback、空墓碑、AMTV fallback、非法边界）均由 `Status` + `out_iter` 无歧义表达；
2. State aliasing 生命周期在 Debug、Release、ASan 下全部通过，无任何内存泄露或野指针；
3. SyncPoint 注入 Local build failure 时，安全返回当前 MemTable 原生迭代器，返回 `Status::OK()`；
4. 原始墓碑为空时返回 `Status::OK()` 且 `*out_iter == nullptr`；
5. Options 默认关闭和解析测试全部通过；
6. 原生范围删除单测无回归；
7. 固定 commit SHA，输出完整报告后停机，等待用户审批 P3a-2。

---

### 7.2 M4-P3a-2：真实 DBIter 接入与端到端集成测试（后续阶段）

在 P3a-1 验收通过后方可执行：

1. **生产接入点对接**：
   - `db/db_impl/db_impl.cc` 初次初始化接入统一工厂，错误时通过 `NewErrorInternalIterator<Slice>` 传播；
   - `db/arena_wrapped_db_iter.cc` Same-SV Refresh 接入统一工厂，Status OK 时无条件替换 `*memtable_range_tombstone_iter_ = std::move(new_iter)`；
   - Full Rebuild（`DoRefresh`）自然走初次构造路径；
2. **端到端集成测试套件**：
   新增 `db/amtv_db_scan_integration_test.cc`，在真实 `DB::NewIterator` 层面与 Native DB 进行逐点双库对照：
   - **T1**：`DBScan_ForwardScan_NextEquivalence`（真实 `Seek(L)` + `Next()` 逐键对账）
   - **T2**：`DBScan_ReverseScan_PrevEquivalence`（真实 `SeekForPrev(U)` + `Prev()` 逐键对账）
   - **T3**：`DBScan_SeekToFirstAndLast_BoundaryClamp`（有界迭代器边界钳位）
   - **T4**：`DBScan_PutResurrectionAcrossLevels`（SST 墓碑被 MemTable Put 复活）
   - **T5**：`DBScan_FutureTombstonesAndSnapshots`（未决墓碑与多 Snapshot）
   - **T6**：`DBScan_UserDefinedTimestamp`（开启 8 字节 UDT 端到端对账）
   - **T7**：`DBScan_SameSuperVersion_Refresh`（同 SV 追加墓碑后 Refresh 原地替换）
   - **T8**：`DBScan_FlushAndFullRebuild`（写入触发 Flush，SV 变化 DoRefresh 重建）
   - **T9**：`DBScan_CloseReopenWALRecovery`（关闭重启 WAL 恢复）
   - **T10**：`DBScan_FallbackMatrix`（空窗口、反向、无界全部回退）
   - **T11**：`DBScan_FailureInjectionSafety`（SyncPoint 模拟局部失败安全回退原生）
   - **T12（修订）**：`DBScan_FullDatabaseStateHashAccounting`（划分为多个连续双边界窗口分别扫描并拼接，计算全库 SHA-256 与 Native DB 对账；另保留一次无界全库扫描验证 fallback 语义）
   - **并发与动态迁移**：并发 `DeleteRange` 写入与有界 Iterator 创建/Refresh（TSAN 验证）、AMTV 进入 fallback 后的真实 DB Scan、`active → immutable → SST` 连续状态迁移。
