# AMTV M4-P2c 准出与代码审阅报告：原生活跃 MemTable 有界化等价审计与多来源 MergingIterator 消费兼容性验证

## 1. 严格结论与范围边界声明

> [!IMPORTANT]
> **本阶段结论严格限定声明**：
> 1. **已证明**：在用户有界窗口 $[L, U)$ 投影域内，活跃 MemTable 从无界截断模式改为有界截断模式后，消费端可观察结果 100% 严格等价；
> 2. **已证明**：由 AMTV 局部有界墓碑流与多来源（Active + Immutable-like + SST-like）组装的真实 `MergingIterator`，被消费时与当前生产真实基线 100% 兼容；
> 3. **尚未证明**：`DBIter` / `ArenaWrappedDBIter` 顶层端到端等价（尚未接入真实生产调用栈）；
> 4. **尚未接入**：生产 Scan 消费路径（`DBIter`、`MergingIterator`、`ArenaWrappedDBIter`、`MemTable`、`RangeDelAggregator`）**保持 0 修改**；
> 5. **尚未测量**：本阶段不测量任何性能指标，未运行任何性能测试矩阵（无 M3b、F1、F2、LongCycle）。

---

## 2. 源码核验事实与生产接入契约修正

### 2.1 Refresh 边界所有权源码核验（字段、位置、行号）
经对 RocksDB v11.8.0 源码的深度只读核验：
1. **`ArenaWrappedDBIter` 字段与构造**：
   - 源码位置：[db/arena_wrapped_db_iter.h:188](file:///home/wam/grad/rocksdb-v11.8.0/db/arena_wrapped_db_iter.h#L188)
   - 字段定义：`ReadOptions read_options_;`
   - `ReadOptions` 内部仅持有原始借用指针：`const Slice* iterate_lower_bound = nullptr; const Slice* iterate_upper_bound = nullptr;`，**未做任何深拷贝**。
2. **`DBIter` 字段与构造**：
   - 源码位置：[db/db_iter.cc:92-93](file:///home/wam/grad/rocksdb-v11.8.0/db/db_iter.cc#L92-L93)
   - 字段定义：`const Slice* const iterate_lower_bound_; const Slice* const iterate_upper_bound_;`，由构造函数直接从 `ReadOptions` 浅拷贝指针，**无内部边界字符串副本**。
3. **核验结论与未来改动要求**：
   - 原设计中关于“`ArenaWrappedDBIter` 在创建期已经深拷贝 bounds 为 `std::string`”的陈述不成立，在此彻底纠正并废除。
   - **未来 AMTV 设计必须自行保证**：
     - 首次建立局部 Scan State 时，深拷贝 $L/U$ 字节至拥有型 `std::string`；
     - `Refresh` 需要的新 State 必须能从持久拥有型边界中构造；
     - 严禁依赖外部调用者临时 `Slice` 或已析构的旧 State；
     - **“新增持久边界所有者”明确记录为未来生产接入所需的显式改动**，不得描述为现有既有能力。

### 2.2 Refresh 失败处理与安全回退协议
针对 `DBImpl::CheckAndRefreshIterator` / `ArenaWrappedDBIter::Refresh`：
- **禁止旧迭代器残留**：新墓碑写入且 read sequence 递增后，旧局部迭代器缺少新增墓碑，继续使用将产生漏删（Under-coverage）。
- **标准协议流程**：
  1. 先在旁路构造新的 Local State；
  2. 构造成功后，原子/安全替换 Slot 0；
  3. 若局部视图构建失败（如内存超限或边界异常），**立即为当前活跃 MemTable、当前 read sequence 构造原生完整范围墓碑迭代器**（`TruncatedRangeDelIterator(..., nullptr, nullptr)`）；
  4. 原生 iterator 构造成功后，用它替换旧 Slot 0 并记录 fallback reason 审计指标；
  5. 若原生回退亦构建失败，则让 `Refresh()` 返回对应的错误 `Status`，中断迭代操作；
  6. **严禁“局部构建失败后保留旧 iterator 并继续读取”**。
- **规范要求**：统一遵循 RocksDB 原生 `Status` 返回值协议，不使用异常捕获替代错误码。

### 2.3 有界迭代器等价性判定：`observable_valid` 统一投影
由于 Baseline A 是底层无界 RangeDel 迭代器，而 B/C 是 `[L, U)` 截断迭代器，在越过窗口边界时底层原生状态允许分歧（如 A 仍 Valid 但 key $\ge U$；B/C 已底层 Invalid 且 status OK）。因此不能直接以原始 `Valid()` 断言等价。
- **统一投影定义**：
  $$\text{observable\_valid} = \text{iter.Valid()} \land (\text{CompareWithoutTimestamp}(\text{user\_key}, L) \ge 0) \land (\text{CompareWithoutTimestamp}(\text{user\_key}, U) < 0)$$
- **比较规则**：
  1. 比较 A、B、C 的 `observable_valid`；
  2. 仅在 `observable_valid == true` 时，比较 key、value、type、sequence、timestamp 是否严格一致；
  3. `observable_valid == false` 时，三组均视为有界扫描完成；
  4. 底层原始 `Valid()` 与越界 key 仅记录到窗外行为审计表；
  5. 迭代器 `status()` 必须在全生命周期保持一致且为 `Status::OK()`；
  6. UDT 情况下窗口判断严格使用无时间戳空间比较，避免编码混淆。

---

## 3. M4-P2c-0 原生活跃 MemTable 有界化等价审计

### 3.1 窗外原始行为审计表（Raw Out-of-Window Audit Table）
在测试拓扑（点键分布于窗外与窗内，全量墓碑覆盖窗外点键）下，记录底层原生行为对照：

| 操作 (Operation) | 目标 (Target) | Group A (无界 Baseline) | Group B (全量有界 B) | Group C (AMTV 候选 C) | 窗外行为分歧本质分析 |
|:---|:---|:---:|:---:|:---:|:---|
| `SeekToFirst` | `-` | `k48` | `k10` | `k10` | A 在窗外应用了全量墓碑 `[k05, k25)`，跳过 `k10`、`k22`，落在 `k48`；B/C 被截断至 `[k30, k70)`，窗外墓碑不生效，停在首个点键 `k10` |
| `Seek` | `L-eps (k00)` | `k48` | `k10` | `k10` | 窗外目标 Seek；A 同样遮蔽窗外点键，B/C 被截断边界 clamp，停在 `k10` |
| `Seek` | `U+eps (k99)` | `INVALID` | `INVALID` | `INVALID` | 超过最大数据键，三组均判定为无效 |
| `SeekToLast` | `-` | `k48` | `k95` | `k95` | A 受右侧全量墓碑 `[k55, k85)`、`[k90, k98)` 遮蔽停在 `k48`；B/C 截断迭代器右界为 `k70`，窗外墓碑未遮蔽 `k95` |
| `SeekForPrev` | `L-eps (k00)` | `INVALID` | `INVALID` | `INVALID` | 目标在所有点键左侧，三组均判定为无效 |

> **审计结论**：窗外原始行为的分歧完全源于 `TruncatedRangeDelIterator` 截断参数设定；而在用户有界可观察域 `[k30, k70)` 内，**三者状态与键值投影 100% 严格等价**。

### 3.2 生命周期与所有权闭环（Zero UAF 证明）
- **机制落实**：在 `LocalRangeDelView::CreateNativeTruncatedRangeDelIterator` 中利用 `std::shared_ptr<FragmentedRangeTombstoneList>` 的 **Aliasing 构造函数**，将整个 `LocalRangeDelView::State` 控制块绑定到 `FragmentedRangeTombstoneIterator` 内部的 `tombstones_ref_` 中；
- **存活保证**：即使 `LocalRangeDelView`、`MergeIteratorBuilder`、边界字符串、点数据 vectors 在内部作用域提前析构，外部独立存活的 `MergingIterator` 仍安全持有边界与墓碑内存；
- **单测证据**：`P2c_0_Lifecycle_StateAndBoundsDestruction` 验证了外层独立存活的 `MergingIterator` 完整执行 Forward / Reverse Scan，可见键（`k38`、`k50`）完全正确，**AddressSanitizer 检测 0 UAF、0 悬垂指针**。

### 3.3 8 项 P2c-0 核心测试通过列表

| 测试用例名称 | 测试目标与场景 | 状态 |
|:---|:---|:---:|
| `P2c_0_RawOutOfWindowBehaviorAudit` | 窗外原始行为审计（非断言性记录差异表） | **PASSED** |
| `P2c_0_Lifecycle_StateAndBoundsDestruction` | State/Builder/Bounds 销毁后独立存活与零 UAF 闭环 | **PASSED** |
| `P2c_0_ActiveMemTable_LongTombstonesCrossingBounds` | 活跃 MemTable 长墓碑跨越 L/U 边界与多层等价 | **PASSED** |
| `P2c_0_ActiveMemTable_OverlappingAndNestedTombstones` | 重叠与嵌套墓碑在有界截断下的消费者等价 | **PASSED** |
| `P2c_0_ActiveMemTable_AdjacentBoundaries` | 相邻边界点墓碑的截断与点键可见性一致性 | **PASSED** |
| `P2c_0_ActiveMemTable_PutResurrectionAndFutureTombstones` | 活跃 MemTable 范围删除后 Put 复活与未决未来墓碑 | **PASSED** |
| `P2c_0_ActiveMemTable_UserDefinedTimestamp` | 活跃 MemTable 包含 8 字节 UDT 的无时间戳空间有界比较 | **PASSED** |
| `P2c_0_ActiveMemTable_MultiSnapshot` | 活跃 MemTable 跨多个快照序列号的有界消费等价性 | **PASSED** |

---

## 4. M4-P2c-1 多来源 MergingIterator 消费兼容性验证

### 4.1 多来源对照架构
复刻真实生产多层级 Scan 环境：
- **Slot 0 (Active MemTable)**:
  - **Group A (生产基线)**：`TruncatedRangeDelIterator(..., nullptr, nullptr)`（全量无界墓碑流）
  - **Group B (中间诊断)**：`TruncatedRangeDelIterator(..., &smallest, &largest)`（全量有界截断流）
  - **Group C (候选实现)**：`TruncatedRangeDelIterator(..., &smallest, &largest)`（AMTV 局部有界墓碑流）
- **Slot 1 (Immutable MemTable)**:
  - A、B、C **逐字节完全一致**：`TruncatedRangeDelIterator(..., nullptr, nullptr)`（原生无界墓碑流，复刻 `memtable_list.cc:337`）
- **Slot 2 (SST-like File)**:
  - A、B、C **逐字节完全一致**：`TruncatedRangeDelIterator(..., &sst_smallest, &sst_largest)`（原生物理文件边界截断流，复刻 `table_cache.cc:432`）

### 4.2 5 项 P2c-1 多来源测试通过列表

| 测试用例名称 | 测试拓扑与多来源场景 | 状态 |
|:---|:---|:---:|
| `P2c_1_MultiSource_CascadingAndCrossSourceMasking` | Active 墓碑覆盖 Imm 点键、Imm 墓碑覆盖 SST 点键的跨层级级联遮蔽 | **PASSED** |
| `P2c_1_MultiSource_PutResurrectionAcrossLevels` | SST 墓碑被 Imm Put 复活，Imm 墓碑又被 Active Put 复活的跨层级版本演进 | **PASSED** |
| `P2c_1_MultiSource_FutureTombstonesAndMultiSnapshot` | Active/Imm/SST 各自拥有未来墓碑时，跨 8 个 Snapshot 序列号消费等价 | **PASSED** |
| `P2c_1_MultiSource_UserDefinedTimestamp` | 多来源均携带 8 字节 UDT 时，MergingIterator 堆排序与时间戳等价 | **PASSED** |
| `P2c_1_MultiSource_ActiveRunMergeEquivalence` | Active AMTV 在后台 Run 归并前（多 Run）与归并后（单 Run）对外部消费完全透明 | **PASSED** |

---

## 5. 三重物理隔离构建与真实 ASan 闸门证据

为了彻底杜绝口头声明，分别在 3 个物理隔离的独立工作树中完成完整构建与测试执行：

| 验证维度 | Debug 独立工作树 (`wt-debug`) | Release 独立工作树 (`wt-release`) | **ASan 独立构建工作树 (`wt-asan`)** |
|:---|:---|:---|:---|
| **物理隔离路径** | `/home/wam/grad/wt-debug` | `/home/wam/grad/wt-release` | `/home/wam/grad/wt-asan` |
| **Commit SHA** | `cd6c55fb3` | `cd6c55fb3` | `cd6c55fb3` |
| **完整构建命令** | `ALLOW_BUILD_PARAMETER_CHANGE=1 make -j16 amtv_local_scan_reference_test` | `DEBUG_LEVEL=0 ALLOW_BUILD_PARAMETER_CHANGE=1 make -j16 amtv_local_scan_reference_test` | `COMPILE_WITH_ASAN=1 DEBUG_LEVEL=1 make -j16 amtv_local_scan_reference_test` |
| **构建退出码** | `exit 0` | `exit 0` | `exit 0` |
| **单测二进制路径** | `/home/wam/grad/wt-debug/amtv_local_scan_reference_test` | `/home/wam/grad/wt-release/amtv_local_scan_reference_test` | `/home/wam/grad/wt-asan/amtv_local_scan_reference_test` |
| **单测二进制 SHA-256** | `2d8e3f8cc45b0285d4492ed855532e17246c271ea4e3e88fe3ab5c50b7c63e9b` | `827344939bbd21adee1da5c343630165e1e3a0f525b99193d1d735650e9d63f8` | `6e4d0d110ee4adf8e753798b8ee75789f682f4e62754e05e1582d9323c62d12d` |
| **动态库路径** | `/home/wam/grad/wt-debug/librocksdb.so.11.8` | （静态链接 `librocksdb.a`） | `/home/wam/grad/wt-asan/librocksdb.so.11.8.1` |
| **动态库 SHA-256** | `bb07a253517635dfa4f06bc210633ac0a63d7419979dc7baf7aba347ecba0cfc` | - | `6347f3dea418e446d6a6d5cfd2ca3892a1dcbb3846b21cf331050045e149d202` |
| **`ldd` 链接证据** | 标准 Linux 动态链接库 | 标准静态链接 Linux 库 | **`libasan.so.6 => /lib/x86_64-linux-gnu/libasan.so.6`** |
| **编译器关键标志** | `-g`, 无 `-O2`, 严格无 `-DNDEBUG` | `-O2`, 显式包含 `-DNDEBUG` | **`-fsanitize=address`, `DEBUG_LEVEL=1`** |
| **P2c 测试运行结果** | **13/13 PASSED** (16 ms) | **13/13 PASSED** (2 ms) | **13/13 PASSED** (15 ms) |
| **全量参考测试集** | **59/59 PASSED** (792 ms) | **59/59 PASSED** (650 ms) | **59/59 PASSED** (4261 ms) |
| **内存/错误诊断** | 0 error | 0 error | **AddressSanitizer 0 error, LeakSanitizer 0 leak** |

---

## 6. 固定提交与停机声明

1. **提交记录**：
   - `7271d0a85`: `feat(amtv): M4-P2c-0 native active memtable bounded equivalence and audit`
   - `cd6c55fb3`: `feat(amtv): M4-P2c-1 multi-source merging iterator consumer compatibility test`
2. **GitHub 远端推送**：
   - 远端仓库：`https://github.com/IceciderEX/rocksdb-rt-opt.git`
   - 目标分支：`rt-opt/main`（已同步至最新 commit `cd6c55fb3`，工作树 clean）
3. **严格停机声明**：
   - **生产 Scan 消费路径 0 修改**；
   - **未接入真实生产 Scan 路径，未运行任何性能基准测试**。
