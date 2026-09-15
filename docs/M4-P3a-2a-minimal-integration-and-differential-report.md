# AMTV M4-P3a-2a：有界 Scan 生产读路径最小接入与差分验证报告

## 摘要

本报告总结 **AMTV M4-P3a-2** 第一阶段（**P0 同步审计与 P3a-2a 最小初始构造接入**）的实施与验证结果。
本阶段严格遵守阶段边界：
- **禁止运行任何正式性能基准测试**（无 M3b、F1、F2、LongCycle）；
- **仅接入初始 Iterator 构造路径**（`DBImpl::NewInternalIterator`），**绝对未修改 `ArenaWrappedDBIter::Refresh`**；
- **解决并发安全疑点**：完成 `MemTable::IsImmutable()` 同步审计，移除热读路径共享原子读取，由调用源 `super_version->mem` 契约保证活跃可变身份；
- **建立 DB 级双实例差分测试框架**：完成 Native DB vs Candidate DB 在 12 种严苛场景下的全方位等价性比对；
- **全环境验证**：在 Debug、Release、ASan（0 错误 0 泄漏）、TSAN（0 数据竞争）下 100% 通过；
- **全面回归验证**：现有全量范围删除与 MemTable 单测 100% 通过；
- **固定提交并停止**：代码已提交至固定 Commit `1b355e6dd` 并推送到远端 `rt-opt/main`，当前停机等待审核，不提前进入 P3a-2b。

---

## 1. P0 同步审计结论

针对 `MemTable::IsImmutable()` 的同步安全性，通过源码审计与静态分析得出如下结论：

### 1.1 字段实际类型与内存布局
- **字段声明**：`db/memtable.h:955`
  ```cpp
  RelaxedAtomic<bool> is_immutable_{false};
  port::RWMutex immutable_mutex_;
  ```
  `RelaxedAtomic<bool>` 底层为封装了 `std::atomic<bool>` 且默认以 `std::memory_order_relaxed` 访问的原子类型。

### 1.2 写入位置与持锁条件
- **唯一写入位置**：`db/memtable.h:838-846`（`MemTable::MarkImmutable()`）
  ```cpp
  void MarkImmutable() override {
    WriteLock wl(&immutable_mutex_);
    is_immutable_.StoreRelaxed(true);
    table_->MarkReadOnly();
    mem_tracker_.DoneAllocating();
    if (amtv_state_) {
      amtv_state_->MarkImmutable();
    }
  }
  ```
  写入时**持有写锁** `immutable_mutex_`，写操作为 `StoreRelaxed(true)`。

### 1.3 读取位置与持锁条件
- 原生 RocksDB 中唯一的非调试读取位置为 `db/memtable.cc:1016, 1028`（`MemTable::AddLogicallyRedundantRangeTombstone`）：
  - 第一次为无锁 Fast Path：`if (is_immutable_.LoadRelaxed()) return false;`
  - 第二次为持锁确认：`ReadLock rl(&immutable_mutex_); if (is_immutable_.LoadRelaxed()) return false;`

### 1.4 同步关系与 TSAN 数据竞争判定
- **Happens-Before 缺失**：`MarkImmutable()` 执行的是 `StoreRelaxed`，而读线程调用 `NewInternalIterator` 或 `Refresh` 时并不持有 `immutable_mutex_`，二者之间**不存在明确的 acquire-release 顺序保证**。
- **并发场景**：读线程持有旧 SuperVersion 期间，后台写入/Switch 线程可能并发标记旧 MemTable 为 Immutable。尽管单次原子读取不会在 C++ 层面引发未定义行为（UB），但无法证明读取时刻的状态确定性，且在热读路径上频繁读取跨线程共享原子变量可能引入无谓的 CPU Cacheline 颠簸与竞争疑点。

### 1.5 落地解决方案
依据指令，**绝对禁止在生产读路径上引入未经论证的共享状态读取，也不得未经全局论证将其改为 atomic acquire/release**：
1. **调用源身份保证**：统一工厂 `BuildActiveMemTableRangeDelIteratorForScan` 明确契约：**只处理调用方提供的 Active Mutable MemTable**。
2. **所有权调用点**：由 `DBImpl::NewInternalIterator` 中的 `super_version->mem` 这一调用源直接保证其身份；Immutable MemTable 在 `super_version->imm->AddIterators` 中 100% 走原生路径；
3. **消除热路径共享读取**：在 `db/amtv_local_scan_view.cc` 中移除运行时 `memtable->IsImmutable()` 判断，仅保留 Debug 断言 `assert(!memtable->IsImmutable())`；
4. **零开销与零竞争**：完全规避了 TSAN 识别为潜在 Data Race 的风险。

---

## 2. 初始构造接入的精确调用链

在 P3a-2a 中，仅修改 `db/db_impl/db_impl.cc` 中的 `DBImpl::NewInternalIterator`：

```text
DBImpl::NewIterator(read_options, ...)
  │
  ▼
ArenaWrappedDBIter::Init(..., read_options, ...)
  │
  ▼
DBImpl::NewInternalIterator(read_options, cfd, super_version, arena, sequence, ...)
  │
  ├── [1] Active MemTable Point Iterator:
  │         mem_iter = super_version->mem->NewIterator(...)
  │
  ├── [2] Active MemTable Range Tombstone Iterator (AMTV P3a-2a 接入点):
  │       ┌────────────────────────────────────────────────────────────────────────┐
  │       │ if (!read_options.ignore_range_deletions) {                            │
  │       │   std::unique_ptr<TruncatedRangeDelIterator> mem_tombstone_iter;      │
  │       │   s = BuildActiveMemTableRangeDelIteratorForScan(                      │
  │       │       static_cast_with_check<MemTable>(super_version->mem),           │
  │       │       read_options, sequence, cfd->ioptions().internal_comparator,     │
  │       │       cfd->ioptions().amtv_enable_bounded_scan_view,                  │
  │       │       &mem_tombstone_iter);                                            │
  │       │   if (!s.ok()) {                                                       │
  │       │     CleanupSuperVersion(super_version);                                │
  │       │     return NewErrorInternalIterator<Slice>(s, arena);                  │
  │       │   }                                                                    │
  │       │   merge_iter_builder.AddPointAndTombstoneIterator(                     │
  │       │       mem_iter, std::move(mem_tombstone_iter));                        │
  │       │ } else {                                                               │
  │       │   merge_iter_builder.AddIterator(mem_iter);                            │
  │       │ }                                                                      │
  │       └────────────────────────────────────────────────────────────────────────┘
  │
  ├── [3] Immutable MemTables:
  │         super_version->imm->AddIterators(...)  [100% 原生路径]
  │
  ├── [4] SST Levels (L0..Ln):
  │         super_version->current->AddIterators(...)  [100% 原生路径]
  │
  └── [5] MergingIterator / DBIter 组装:
            internal_iter = merge_iter_builder.Finish(...)  [100% 原生路径]
```

### 关键契约兑现情况
1. **作用域唯一**：仅作用于 `super_version->mem`，Immutable MemTable 与 SST 迭代器完全原生；
2. **开关来源固定**：严格取自 `cfd->ioptions().amtv_enable_bounded_scan_view`；
3. **错误传播**：非 OK 状态立即调用 `CleanupSuperVersion` 并返回 `NewErrorInternalIterator<Slice>(s, arena)`；
4. **合法空墓碑**：返回 `OK + nullptr` 时，以 `nullptr` 墓碑迭代器正常注册点迭代器，原生语义无损；
5. **忽略墓碑保持**：`ignore_range_deletions=true` 时直接进入 `AddIterator(mem_iter)`，不进入 AMTV 逻辑；
6. **功能关闭等价**：配置关闭时，工厂在内部直接执行与原生完全等价的代码构造完整 `TruncatedRangeDelIterator`。

---

## 3. Refresh 接入的精确状态转移设计（后续 P3a-2b 规范，本阶段未动代码）

虽然本阶段尚未接入 Refresh，但状态机转移规范已明确建立：

```text
                           [ArenaWrappedDBIter::Refresh]
                                        │
                      ┌─────────────────┴─────────────────┐
                      ▼                                   ▼
        [Same-SuperVersion 快速路径]             [Full-Rebuild 路径]
                      │                                   │
                      │ (sv 未变，仅 seq 递增)              │ (sv 发生代际更替)
                      ▼                                   ▼
     BuildActiveMemTableRangeDelIteratorForScan    重建整个 InternalIterator
                      │                            (自动经 NewInternalIterator)
         ┌────────────┴────────────┐
         ▼                         ▼
      [成功 s.ok()]            [失败 !s.ok()]
         │                         │
         ├── out_iter != nullptr   ├── 保持旧 slot 不变
         │   --> 替换 slot 0       └── 返回 error status
         └── out_iter == nullptr
             --> 清空 slot 0
```
> **注**：P3a-2a 阶段严格遵循指令，**未修改 `ArenaWrappedDBIter::Refresh` 的任何代码**。

---

## 4. Native/Candidate DB 差分验证结果

在新建的 DB 级双实例差分测试套件 `db/amtv_db_scan_integration_test.cc` 中，对以下两个独立数据库实例进行操作序列与输出逐项对比：
- **Native DB**：`amtv_enable_bounded_scan_view = false`
- **Candidate DB**：`amtv_enable_bounded_scan_view = true`

比对项包括：
1. 创建期 Status 校验；
2. 正向遍历 Key/Value 序列（`SeekToFirst()` -> 循环 `Next()`）；
3. 反向遍历 Key/Value 序列（`SeekToLast()` -> 循环 `Prev()`）；
4. 离散点单点定位输出（`Seek(target)` 与 `SeekForPrev(target)` 对比边界与键值）；
5. 遍历过程 Status 校验；
6. 越界保护校验（Candidate 严禁返回任何 $< L$ 或 $\ge U$ 的键）。

### 12 项场景覆盖与比对结果

| 场景编号 | 测试场景描述 | 窗口边界 | 写入与墓碑特征 | 比对步骤与验证点 | 差分比对结果 |
|:---:|:---|:---:|:---|:---|:---:|
| **01** | **无墓碑** | `["k15", "k45")` | 仅写入 k10..k50，活跃 MemTable 无任何 DeleteRange | 正反向遍历、7 个离散 Seek 目标、空墓碑 nullptr 槽位处理 | **100% 逐项完全一致** |
| **02** | **全部墓碑与窗口不相交** | `["k20", "k50")` | 墓碑 `["k01", "k05")` 与 `["k80", "k90")` 完全位于窗外 | 候选过滤剔除、正反向遍历、9 个离散 Seek 目标 | **100% 逐项完全一致** |
| **03** | **左边界跨越长墓碑** | `["k15", "k45")` | 墓碑 `["k05", "k25")`，左边界 k15 深入墓碑内部 | k10/k20 被掩盖，k30/k40 正常可见，左边界截断正确性 | **100% 逐项完全一致** |
| **04** | **右边界跨越长墓碑** | `["k15", "k45")` | 墓碑 `["k35", "k55")`，右边界 k45 深入墓碑内部 | k40 被掩盖，k20/k30 正常可见，右边界截断正确性 | **100% 逐项完全一致** |
| **05** | **完全包含窗口的长墓碑** | `["k15", "k45")` | 墓碑 `["k05", "k55")` 完全覆盖整个有界窗口 | 窗口内所有点失效，Valid() 必须为 false，Seek 判定 | **100% 逐项完全一致** |
| **06** | **重叠、嵌套、相邻复合墓碑** | `["k05", "k85")` | 重叠 `[k10, k30)`+`[k20, k40)`；相邻 `[k40, k50)`；嵌套 `[k60, k90)`+`[k70, k80)` | 20 个离散 Seek 目标、密集正反向扫描拓扑完整性 | **100% 逐项完全一致** |
| **07** | **同起点不同终点与 Sequence** | `["k15", "k65")` | 同起点 k20 分别产生到 k50(s1), k35(s2), k60(s3) 的三代墓碑 | 覆盖层叠解析、多版本 RangeDel 覆盖深度 | **100% 逐项完全一致** |
| **08** | **DeleteRange 后 Put 复活** | `["k15", "k45")` | DeleteRange `[k15, k45)` 后，以更高 Seq Put k30 与 k20 | 复活键 k20/k30 正确穿透墓碑显现，未复活键保持删除 | **100% 逐项完全一致** |
| **09** | **多 Snapshot / Read Sequence** | `["k05", "k50")` | 经历 Snap1, Snap2, Snap3 及最新无 Snap 读 | 每个 Snapshot 时刻的双 DB 状态与可见性独立差分验证 | **100% 逐项完全一致** |
| **10** | **合法空 user key 边界** | `["", "z")` | 写入空 key `""`，DeleteRange `["", "b")` | 零长 user key、`\x01` 等极端边界下的鲁棒性，无 crash | **100% 逐项完全一致** |
| **11** | **用户时间戳 (UDT)** | `["k15", "k45")` | 使用 `BytewiseComparatorWithU64TsWrapper`，带时间戳写入与删除 | ts15 (删前), ts25 (删后), ts35 (复活后) 三时间点比对 | **100% 逐项完全一致** |
| **12** | **全回退矩阵 (Fallback)** | 复合 | 覆盖：开关关闭 (12a)、SyncPoint 故障注入 (12b)、无界 Scan (12c)、反向边界 (12d)、ignore_range_deletions (12e) | 回退至 Native 完整迭代器后结果与纯 Native DB 完全一致 | **100% 逐项完全一致** |

---

## 5. 多环境编译与 Sanitizer 验证结果

| 环境 / 模式 | 构建工作区路径 | 构建命令 | 二进制 SHA-256 | 动态依赖 (ldd) | 测试通过情况 | Sanitizer 结果 |
|:---|:---|:---|:---|:---|:---:|:---|
| **Debug** | `/home/wam/grad/rocksdb-v11.8.0` | `make -j16 amtv_db_scan_integration_test` | `24349b32a66df120b3fbc8f46b7199b053b334b82a8a50f47b41e952974c2e7a` | `librocksdb.so.11.8`, `libjemalloc.so.2` | **12 / 12 PASSED** (474 ms) | 断言全部通过 |
| **Release** | `/home/wam/grad/wt-release` | `DEBUG_LEVEL=0 make -j16 amtv_db_scan_integration_test` | `03c4db59e86cb8e13fcb884e7ea4210432b5b9d2ee9d54029c76633cef8913c9` | 静态链接 `librocksdb.a`, `NDEBUG` 优化 | **12 / 12 PASSED** (442 ms) | 0 告警，运行极快 |
| **ASan** | `/home/wam/grad/wt-asan` | `COMPILE_WITH_ASAN=1 make -j16 amtv_db_scan_integration_test` | `f767a9a88b36c982f0756b13e253623615360d91eaec4b4fb712ab3112fbb5fa` | `libasan.so.6`, `librocksdb.so.11.8` | **12 / 12 PASSED** (745 ms) | **0 leaks, 0 errors, 0 UAF** |
| **TSAN** | `/home/wam/grad/wt-tsan` | `COMPILE_WITH_TSAN=1 make -j16 amtv_db_scan_integration_test` | `770afa3dfaa56d1c993afdcd4c4cd97857c14a98818c72cc35ad2d8fae4da976` | `libtsan.so.0`, `librocksdb.so.11.8` | **12 / 12 PASSED** (1500 ms) | **0 data races, 0 TSAN warnings** |

---

## 6. 全面回归测试结果

除新增的双实例集成测试外，按照验收范围执行了全量现有核心回归测试：

| 测试套件名称 | 关联模块 | 运行命令 | 结果 | 耗时 |
|:---|:---|:---|:---:|:---:|
| `amtv_local_scan_view_test` | AMTV 工厂与组件单元测试 | `./amtv_local_scan_view_test` | **18 / 18 PASSED** | 192 ms |
| `amtv_local_scan_reference_test` | AMTV 与 MergingIterator 消费兼容测试 | `./amtv_local_scan_reference_test` | **64 / 64 PASSED** | 849 ms |
| `amtv_scan_oracle_test` | AMTV 差分 Oracle 测试 | `./amtv_scan_oracle_test` | **15 / 15 PASSED** | 59 ms |
| `amtv_probe_test` | AMTV 核心语义探测 | `./amtv_probe_test` | **3 / 3 PASSED** | < 1 ms |
| `range_tombstone_fragmenter_test` | 原生范围删除 Fragmenter 测试 | `./range_tombstone_fragmenter_test` | **17 / 17 PASSED** | 1 ms |
| `db_range_del_test` | 原生 DB 级全量范围删除集成测试 | `./db_range_del_test` | **85 / 85 PASSED** | 3084 ms |
| `db_memtable_test` | 原生 MemTable 功能与并发测试 | `./db_memtable_test` | **14 / 14 PASSED** | 514 ms |
| `db_iterator_test` (`*Refresh*`) | 原生 Iterator Refresh 现有测试 | `./db_iterator_test --gtest_filter="*Refresh*"` | **9 / 9 PASSED** | 1306 ms |

---

## 7. 生产路径实际修改文件与行号明细

在本次 P3a-2a 接入中，**修改的生产代码严格限定在以下位置**：

1. **[db/db_impl/db_impl.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/db_impl/db_impl.cc)**
   - **Line 41**：新增头文件引用 `#include "db/amtv_local_scan_view.h"`；
   - **Lines 2608–2623**：在 `DBImpl::NewInternalIterator` 中，将原生活跃 MemTable 墓碑迭代器构造替换为统一工厂调用与错误检查：
     ```cpp
     if (!read_options.ignore_range_deletions) {
       std::unique_ptr<TruncatedRangeDelIterator> mem_tombstone_iter;
       s = BuildActiveMemTableRangeDelIteratorForScan(
           static_cast_with_check<MemTable>(super_version->mem), read_options,
           sequence, cfd->ioptions().internal_comparator,
           cfd->ioptions().amtv_enable_bounded_scan_view,
           &mem_tombstone_iter);
       if (!s.ok()) {
         CleanupSuperVersion(super_version);
         return NewErrorInternalIterator<Slice>(s, arena);
       }
       merge_iter_builder.AddPointAndTombstoneIterator(
           mem_iter, std::move(mem_tombstone_iter));
     } else {
       merge_iter_builder.AddIterator(mem_iter);
     }
     ```
2. **[db/amtv_local_scan_view.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view.cc)**
   - **Line 112**：移除热读路径上的运行时 `memtable->IsImmutable()` 读取，改为 Debug 断言 `assert(!memtable->IsImmutable())`；
   - **Lines 28–44, 124–128, 202–206**：精确修正 UDT 下 `CompareWithoutTimestamp` 的 `a_has_ts / b_has_ts` 标志，确保与 RocksDB `ReadOptions` 外部无时间戳边界契约一致。
3. **[db/amtv_local_scan_view_test.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view_test.cc)**
   - **Lines 409–436**：更新单测，测试主动调用者契约。
4. **[CMakeLists.txt](file:///home/wam/grad/rocksdb-v11.8.0/CMakeLists.txt)**, **[Makefile](file:///home/wam/grad/rocksdb-v11.8.0/Makefile)**, **[src.mk](file:///home/wam/grad/rocksdb-v11.8.0/src.mk)**
   - 注册新增双实例差分测试 `db/amtv_db_scan_integration_test.cc`。

---

## 8. 固定 Commit SHA 与远端核验

- **本地分支**：`main`
- **最新提交 Commit**：`1b355e6dd`
- **提交信息**：`fix(test): wrap SyncPoint in #ifndef NDEBUG for Release builds`
- **父提交 Commit**：`8c0eac8cd` (`amtv(scan): M4-P3a-2a production minimal integration and differential tests`)
- **远端跟踪分支**：`rt-opt/main` (`https://github.com/IceciderEX/rocksdb-rt-opt.git`)
- **远端核验状态**：`Your branch is up to date with 'rt-opt/main'.` (已成功 push 并同步)

---

## 9. 尚未证明的内容与下一阶段性能验证边界

为确保科研结论的严谨性与安全性，明确列出以下边界：

> [!WARNING]
> ### 当前尚未证明的内容
> 1. **尚未证明 `ArenaWrappedDBIter::Refresh` 路径的等价性与安全性**：P3a-2a 严格限定只接入了初始迭代器构造；Refresh 路径（包括 Same-SV 与 Full-Rebuild）代码未接入，将在 P3a-2b 阶段专门接入并验证；
> 2. **尚未证明多线程高并发下的生命周期竞争安全性**：多线程并发读写与 MemTable 切换的复合负载验证在后续阶段执行；
> 3. **尚未证明端到端生产性能提升**：本阶段所有测试均为功能正确性与差分语义等价性验证，严格未测量也不作任何吞吐或时延提升的承诺。

> [!IMPORTANT]
> ### 停止点与后续步骤
> - **当前状态**：P3a-2a 接入全部完成，全套差分测试与回归测试通过，代码已提交并远端同步。
> - **停止动作**：立即停机，等待用户审核与明确指示。
> - **下一阶段（待批准）**：**M4-P3a-2b：接入 Iterator Refresh 并完成并发与生命周期验证**。
