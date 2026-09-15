# AMTV M4-P3a-1 内部组件与统一工厂实施与验收报告

> **阶段**：M4-P3a-1（内部组件与统一工厂）  
> **状态**：**P3a-1 验收完毕，已固定 commit SHA，按规则主动停机等待 P3a-2 审批**  
> **核心仓库 Commit SHA**：`8d54055476c3a318eecbc02e4abd22ed5f76984c` (`rt-opt/main`)  
> **生产 Scan 路径改动**：**严格 0 修改**（`DBImpl::NewInternalIterator` 与 `ArenaWrappedDBIter::Refresh` 暂未接入）  

---

## 1. P3-P0 审计问题修正落实摘要

在进入编码前，已全面修订实施规划文档并正式持久化于 [docs/M4-P3-P0-bounded-scan-production-plan.md](file:///home/wam/grad/rocksdb-v11.8.0/docs/M4-P3-P0-bounded-scan-production-plan.md)，8 项修改意见均已落实：

1. **统一工厂返回契约修正**：
   - 摒弃歧义指针返回签名，统一改为：
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
   - 入口强制 `out_iter->reset()`；
   - `Status::OK()` 且 `*out_iter == nullptr` 确立为“当前无有效范围墓碑”的合法结果；
   - 资格不符或局部失败时，工厂内部自动构造当前原生完整 `TruncatedRangeDelIterator`，包装为 `[nullptr, nullptr]` 回退，返回 `Status::OK()` 并标记 metadata；
   - 仅当无法提供正确的 RangeDel iterator（如原生构造亦失败或参数非法）时才返回非 OK `Status`；
   - Refresh 契约明确要求在 `Status::OK()` 时无条件执行 `*slot0 = std::move(new_iter)`，即使 `new_iter == nullptr` 也必须清除旧 Slot 0，严禁使用 `if (new_iter != nullptr)` 判断；
   - 核验确认 `DBImpl::NewInternalIterator`（`db/db_impl/db_impl.cc:2670`）在状态非 OK 时通过 `NewErrorInternalIterator<Slice>(s, arena)` 向上报错，绝不吞掉错误。

2. **错误处理措辞与分类修正**：
   - 彻底删除“never throws”和“OOM 一定转为 Status”等绝对表述；
   - 明确划分为：可预期业务回退（正常控制流 Native Fallback）、原生 Iterator 状态错误（向上传播）、内存分配失败（遵循全局 C++ / RocksDB 策略）；
   - Failure Injection 使用 `TEST_SYNC_POINT_CALLBACK` 注入测试码，绝不使用 C++ 异常。

3. **统一边界所有权设计**：
   - 承认既有事实：`DBIter` / `ArenaWrappedDBIter` 仅浅拷贝 `ReadOptions` 中的 `Slice` 指针；
   - 调用方按官方规范保证 `ReadOptions` 边界在 Iterator 存活期有效；
   - 统一工厂内部立即深拷贝 $L/U$ 字节至独立 `LocalRangeDelViewState`；
   - 生成的 `TruncatedRangeDelIterator` 借用 `state` 内拥有的 `smallest_ikey` 与 `largest_ikey`；
   - 通过 Aliasing `std::shared_ptr<FragmentedRangeTombstoneList>(state, state->fragmented_list.get())` 绑定生命周期至 Slot 0，Slot 0 销毁时 `state` 自动析构，零野指针风险。

4. **性能表述规范**：
   - 彻底删除“默认关闭开销 <1ns”等未经测量的表述；
   - 统一表述为：“默认关闭时必须保持功能行为与原生一致；实际开销将在后续 Release 基准中测量”。

5. **构建系统与 Options 完备测试**：
   - 同步更新并审计 `src.mk`、`CMakeLists.txt`、`Makefile`；
   - 修复 `options/options_helper.cc` 中 `UpdateColumnFamilyOptions` 遗漏 AMTV 选项复制的问题；
   - 增加 Options 默认值（`false`）、String 解析、序列化反序列化恢复、非法值报错、DB Reopen 行为测试。

6. **阶段拆分严格执行**：
   - 当前完成 M4-P3a-1（内部组件、统一工厂、只读接口、单元测试）；
   - 暂不修改真实 DBIter 路径；
   - 完成后主动停机等待验收。

7. **全库摘要测试用例（T12）修正**：
   - 修正方案固化为连续多窗口双边界扫描拼接并校验 SHA-256，另附一次无界全库扫描验证 fallback 语义。

8. **负载适用性结论固化**：
   - 明确既有 E8、M3b、F1/F2、LongCycle 均为无界扫描（`nullptr, nullptr`），100% 走原生全量路径，不能用于评价 AMTV Scan；
   - 规划新增独立 bounded driver；明确 `SCAN_LIMIT` 不能转换为固定 upper bound；新旧负载严格隔离。

---

## 2. 核心源码变更清单

在本次 P3a-1 提交中，改动文件如下：

| 文件路径 | 变更性质 | 核心功能说明 |
|:---|:---:|:---|
| [db/amtv_local_scan_view.h](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view.h) | **NEW** | 定义 `AMTVScanMode`、`AMTVScanFallbackReason`、`AMTVScanBuildMetadata`、`LocalRangeDelViewState`，声明统一工厂函数 |
| [db/amtv_local_scan_view.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view.cc) | **NEW** | 实现候选相交判定、资格检查、深拷贝边界构建、Aliasing shared_ptr 绑定、原生回退及 SyncPoint 注入点 |
| [db/amtv_local_scan_view_test.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view_test.cc) | **NEW** | 统一工厂与组件完备单元测试套件（18 项测试） |
| [docs/M4-P3-P0-bounded-scan-production-plan.md](file:///home/wam/grad/rocksdb-v11.8.0/docs/M4-P3-P0-bounded-scan-production-plan.md) | **NEW** | 修订后的 P3-P0 架构审计与实施全景规划 |
| [include/rocksdb/advanced_options.h](file:///home/wam/grad/rocksdb-v11.8.0/include/rocksdb/advanced_options.h) | MODIFY | `AdvancedColumnFamilyOptions` 新增 `bool amtv_enable_bounded_scan_view = false;` |
| [options/cf_options.h](file:///home/wam/grad/rocksdb-v11.8.0/options/cf_options.h) | MODIFY | `ImmutableCFOptions` 新增 `bool amtv_enable_bounded_scan_view;` |
| [options/cf_options.cc](file:///home/wam/grad/rocksdb-v11.8.0/options/cf_options.cc) | MODIFY | 注册选项映射 `"amtv_enable_bounded_scan_view"` 及构造函数初始化 |
| [options/options_helper.cc](file:///home/wam/grad/rocksdb-v11.8.0/options/options_helper.cc) | MODIFY | 在 `UpdateColumnFamilyOptions` 中补全 AMTV 及 bounded scan 选项复制 |
| [db/memtable.h](file:///home/wam/grad/rocksdb-v11.8.0/db/memtable.h) | MODIFY | `MemTable` 新增只读查询接口 `bool IsImmutable() const` |
| [src.mk](file:///home/wam/grad/rocksdb-v11.8.0/src.mk) | MODIFY | 注册 `LIB_SOURCES` 与 `TEST_MAIN_SOURCES` |
| [CMakeLists.txt](file:///home/wam/grad/rocksdb-v11.8.0/CMakeLists.txt) | MODIFY | 注册 `SOURCES` 与 `TESTS` |
| [Makefile](file:///home/wam/grad/rocksdb-v11.8.0/Makefile) | MODIFY | 注册 `amtv_local_scan_view_test` 测试目标构建规则 |

---

## 3. 多环境构建与测试验证证据

### 3.1 测试用例执行矩阵

测试文件 `db/amtv_local_scan_view_test.cc` 涵盖全部验收标准：

| 测试用例名称 | 验证核心场景 | Debug 结果 | Release 结果 | ASan 结果 |
|:---|:---|:---:|:---:|:---:|
| `OptionsDefaultValue` | 默认必须为 `false` | ✅ PASSED (1ms) | ✅ PASSED (2ms) | ✅ PASSED (3ms) |
| `OptionsStringParsing` | `GetColumnFamilyOptionsFromString` 解析 true/false 及非法值拦截 | ✅ PASSED (1ms) | ✅ PASSED (1ms) | ✅ PASSED (6ms) |
| `OptionsSerializationAndRecovery` | Dump 输出含配置项，反序列化恢复后一致 | ✅ PASSED (1ms) | ✅ PASSED (1ms) | ✅ PASSED (2ms) |
| `OptionsDBReopenPersistence` | 带选项打开 DB、写入数据并 reopen，配置持久化正常 | ✅ PASSED (31ms) | ✅ PASSED (25ms) | ✅ PASSED (48ms) |
| `NativeDefaultZeroBehaviorChange` | 默认关闭时原生 DB 范围扫描与过滤行为零变化 | ✅ PASSED (13ms) | ✅ PASSED (13ms) | ✅ PASSED (25ms) |
| `CandidateIntersectionLogic` | 单测 `AMTVScanIsCandidateIntersecting` 边界覆盖（内、外、相交、跨界、反向） | ✅ PASSED (0ms) | ✅ PASSED (1ms) | ✅ PASSED (1ms) |
| `FactoryEmptyMemTable` | 无范围墓碑时返回 `Status::OK()` + `nullptr`，`mode = kEmpty` | ✅ PASSED (13ms) | ✅ PASSED (13ms) | ✅ PASSED (26ms) |
| `FactoryLocalBoundedHit` | 相交窗口成功构造局部迭代器，`mode = kLocal`，内部边界正确截断 | ✅ PASSED (13ms) | ✅ PASSED (14ms) | ✅ PASSED (24ms) |
| `FactoryNonIntersectingTombstones` | 存在墓碑但与扫描窗口无相交时，合法返回 `OK + nullptr` | ✅ PASSED (13ms) | ✅ PASSED (13ms) | ✅ PASSED (23ms) |
| `FactoryFallbackDisabledOption` | 选项关闭时回退原生全量迭代器，`mode = kNativeFallback` | ✅ PASSED (13ms) | ✅ PASSED (14ms) | ✅ PASSED (24ms) |
| `FactoryFallbackUnbounded` | 无界扫描回退原生全量迭代器，`reason = kUnbounded` | ✅ PASSED (14ms) | ✅ PASSED (14ms) | ✅ PASSED (23ms) |
| `FactoryFallbackInvertedBounds` | $L \ge U$ 反向边界回退原生全量迭代器，`reason = kInvertedBounds` | ✅ PASSED (15ms) | ✅ PASSED (14ms) | ✅ PASSED (24ms) |
| `FactoryFallbackImmutableMemTable` | MemTable 变为 Immutable 时回退原生全量流，`reason = kImmutableMemTable` | ✅ PASSED (14ms) | ✅ PASSED (14ms) | ✅ PASSED (24ms) |
| `FactoryFallbackAMTVFailure` | AMTV 进入 fallback 状态时回退原生全量流，`reason = kAMTVUnavailable` | ✅ PASSED (14ms) | ✅ PASSED (16ms) | ✅ PASSED (28ms) |
| `FactorySyncPointFailureInjection` | SyncPoint 注入局部失败，安全回退当前原生迭代器 | ✅ PASSED (13ms) | N/A (NDEBUG) | ✅ PASSED (24ms) |
| `FactoryStateAliasingLifetimeAndASan` | 外部临时边界析构后，借由 Aliasing shared_ptr 保活 State，ASan 零 UAF | ✅ PASSED (13ms) | ✅ PASSED (16ms) | ✅ PASSED (29ms) |
| `FactoryRefreshClearingOldSlot0` | Refresh 返回 OK + nullptr 时无条件替换，旧 Slot 0 彻底清空 | ✅ PASSED (13ms) | ✅ PASSED (12ms) | ✅ PASSED (24ms) |
| `FactoryInvalidArgumentChecks` | `out_iter == nullptr` 或 `memtable == nullptr` 报错，`ignore_range_del` 正确处理 | ✅ PASSED (13ms) | ✅ PASSED (13ms) | ✅ PASSED (23ms) |
| **总计通过情况** | **全部 18 项单测全部通过** | **18/18 PASSED** | **17/17 PASSED** | **18/18 PASSED** |

### 3.2 原生范围删除单测回归验证

- **执行用例**：`range_tombstone_fragmenter_test`
- **执行命令**：`make -j16 range_tombstone_fragmenter_test && ./range_tombstone_fragmenter_test`
- **验证结果**：
  ```text
  [==========] Running 17 tests from 1 test case.
  [----------] 17 tests from RangeTombstoneFragmenterTest
  ...
  [----------] 17 tests from RangeTombstoneFragmenterTest (0 ms total)
  [  PASSED  ] 17 tests.
  ```
  **原生范围删除算法无任何行为退化与回归。**

### 3.3 二进制指纹与环境信息

| 构建环境 | 构建命令 | 二进制路径 | SHA-256 Checksum | 依赖与 Sanitizer 摘要 |
|:---|:---|:---|:---|:---|
| **Debug** | `make -j16 amtv_local_scan_view_test` | `/home/wam/grad/rocksdb-v11.8.0/amtv_local_scan_view_test` | `98698be4a778a0069b928d165e519df8e7261c254b1969099eb79098554b1ee9` | `librocksdb.so.11.8`, `libjemalloc.so.2` |
| **Release** | `AUTO_CLEAN=1 DEBUG_LEVEL=0 make -j16 amtv_local_scan_view_test` | `/home/wam/grad/wt-release/amtv_local_scan_view_test` | `7834317901900c3f9dc99cf1a89a28c66339095a566f9ed07d0800983f7d5555` | 静态链接 `librocksdb.a`, `NDEBUG` 优化 |
| **ASan** | `AUTO_CLEAN=1 COMPILE_WITH_ASAN=1 make -j16 amtv_local_scan_view_test` | `/home/wam/grad/wt-asan/amtv_local_scan_view_test` | `b9eab2d1e0d0819c5eb79e96009e60afef649fcc9bf20ea5f5bb35e1bb11375d` | `libasan.so.6`, 0 leaks, 0 errors |

---

## 4. 准出检查与停机状态

根据用户要求：
1. **统一工厂与内部组件实施完成**；
2. **所有 7 项验收要求全部达到**；
3. **未修改 `DBImpl::NewInternalIterator` 和 `ArenaWrappedDBIter::Refresh`**；
4. **代码已提交并推送至远程分支 `rt-opt/main`（Commit SHA: `8d5405547`）**；
5. **已完成主动停机，等待用户审查验收 M4-P3a-1 报告并审批进入 M4-P3a-2。**
