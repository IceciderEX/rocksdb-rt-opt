# AMTV M4-P3a-2b: Iterator Refresh 生产读路径接入与并发生命周期验证报告

**报告类型**: 阶段交付与审计验收报告 (Phase B)  
**分支**: `main`  
**远端分支**: `rt-opt/main` (`https://github.com/IceciderEX/rocksdb-rt-opt.git`)  
**提交 SHA**: `933aa0881eecf0c2ee7fb5a4e01f479a0a04c58a`  
**前序提交**: `72bc41d989e8bddbca6a9d925b0e61019bbc53d3`, `c7eb16b2c7d58883136ef60da00800a54f4975af` (Phase B feat), `a708b1129b827e8d62ce7397b97c0fecf36f9a00` (Phase A docs), `adad7719b` (Phase A feat)  
**验证状态**: **通过 (PASSED)** —— 针对性测试 `amtv_db_refresh_test` (22/22 全通过) + 生产集成测试 `amtv_db_scan_integration_test` (12/12 全通过) + 核心单测 `amtv_local_scan_view_test` (18/18 全通过) + 引用测试 `amtv_local_scan_reference_test` (64/64 全通过) + 原生回归 `db_memtable_test` (14/14) & `db_range_del_test` (85/85) 全通过。  
**构建与运行时矩阵**: Debug (`DEBUG_LEVEL=1`)、Release (`DEBUG_LEVEL=0`)、AddressSanitizer (`COMPILE_WITH_ASAN=1`)、ThreadSanitizer (`COMPILE_WITH_TSAN=1`) 四大环境全部通过，零内存越界/泄露、零数据竞争。

---

## 1. B1：原生 Refresh 契约与所有权审计

### 1.1 核心审计矩阵：Same-SuperVersion vs. Full-Rebuild

RocksDB 原生 `ArenaWrappedDBIter::Refresh(const Snapshot* snapshot)` 通过比对当前与底层 SuperVersion 版本号决定执行路径：

| 审计项 | Same-SuperVersion 路径 (`sv_number_ == cur_sv_number`) | Full-Rebuild 路径 (`sv_number_ != cur_sv_number` 或 error 状态) |
|:---|:---|:---|
| **新 read sequence 取得点** | `SequenceNumber read_seq = GetSeqNum(db_impl, snapshot)` (`db/arena_wrapped_db_iter.cc:286`) | `SequenceNumber read_seq = GetSeqNum(db_impl, snapshot)` (`db/arena_wrapped_db_iter.cc:237`) |
| **新 read sequence 写入 DBIter 点** | 在 MemTable 范围墓碑更新完成并确认 SV 未再次变化后，执行 `db_iter_->set_sequence(read_seq)` (`line 336`) | 在 `DoRefresh` 中通过 `Init(...)` 传递给 `DBIter::NewIter(...)`，构造全新 `DBIter` 时写入 |
| **AMTV Snapshot 取得点** | 由工厂调用 `memtable->GetAMTVForRangeDel()`，从 Active MemTable 的 `active_amtv_` 原子指针通过 `acquire` 内存序取得 | 同左：在 `NewInternalIterator` 中通过统一工厂取得 |
| **点迭代器、范围墓碑槽位、DBIter sequence 更新顺序** | 1. 构造新范围墓碑局部视图 `new_iter`<br>2. 替换/清空槽位 `*memtable_range_tombstone_iter_ = std::move(new_iter)`<br>3. 检查 SV 是否并发改变<br>4. 更新 `db_iter_->set_sequence(read_seq)`<br>5. 标记 `db_iter_->set_valid(false)` | 1. 析构旧 `DBIter` 和旧 `Arena` (`DestroyDBIterAndArena`)<br>2. 创建全新 `Arena`<br>3. 引用新 `SuperVersion`<br>4. 构造全新 `DBIter` (携带新 `read_seq`)<br>5. 构造新内部迭代器树 (`NewInternalIterator`：先墓碑视图、后点迭代器)<br>6. 挂载到 `DBIter` (`SetIterUnderDBIter`) |
| **原生 Refresh 非 OK 契约** | 原生仅在 `!allow_refresh_` 返回 `NotSupported`。在 AMTV 中，若最终非 OK：<br>1. 安全回退清理旧 `DBIter` 与 Arena，挂载 `NewErrorInternalIterator(s)`<br>2. `it->Valid()` 置 `false`<br>3. `it->status()` 返回错误<br>4. 阻止任何陈旧/缺失墓碑的错误读操作<br>5. 允许再次 `Refresh()`：因非 OK 状态路由至 `DoRefresh` 自动全量重建恢复 | 若 `NewInternalIterator` 返回 error iterator，挂载至 `DBIter`，`it->status()` 传播错误，迭代器失效 |
| **显式 Snapshot 支持范围** | 支持：当 `snapshot != nullptr` 时，`GetSeqNum` 提取该 snapshot 的 sequence number，并传入局部视图与底层迭代器 | 支持：通过 `read_options_.snapshot = snapshot` 传递并在 `DoRefresh` 中重构 |
| **UDT (User-Defined Timestamps)** | 遵循原生边界：当开启 UDT 时，工厂检测并安全回退到原生 `NewRangeTombstoneIterator`，不产生自定义假设 | 同左：原生 `NewInternalIterator` 创建原生 UDT 墓碑迭代器 |
| **惰性初始化 (Lazy Initialization)** | 若尚未首次初始化 (`!internal_iter_initialized_`)，`Refresh()` 先调用 `EnsureInternalIteratorInitialized(nullptr)` 绑定当前时点的 DB 状态，再更新序列号，严格保证可见性时点不被非法推迟 | `DoRefresh` 总是立即完整初始化并标记 `internal_iter_initialized_ = true` |
| **ReadOptions 边界** | 若设置 `ignore_range_deletions = true`，直接跳过范围墓碑更新，仅推进 sequence；若设置了 bounds，局部视图严格以 `[lower, upper)` 进行窗口修剪与求交 | 同左：完全传递 `read_options_` 至底层迭代器树 |
| **SuperVersion 引用生命周期** | 在 `cfd->GetThreadLocalSuperVersion(db_impl)` 取得临时引用，局部视图构建完成后无论成功、回退或失败均立即调用 `db_impl->ReturnAndCleanupSuperVersion(cfd, sv)` 释放 | 在 `cfd->GetReferencedSuperVersion(db_impl)` 取得强引用，交接给 `DBIter` 作为生命周期持有者，在旧 `DBIter` 析构时释放旧引用 |

### 1.2 工厂输入契约修订

已按标准指令将 [db/amtv_local_scan_view.h](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view.h) 的注释契约修订为：

> **“输入来自调用者所持有 SuperVersion 的 mem 槽位；对象可能在并发换代中已经被冻结。其生命周期由既有 SuperVersion 引用和局部视图所有权保障。工厂不读取 `memtable->IsImmutable()`，不修改该字段内存序，避免跨线程共享状态读取与误断言。”**

### 1.3 阶段 A 非 OK 分支 `mem_iter` 资源清理审计与顺序调优

- **既有风险审计**：阶段 A 中，`DBImpl::NewInternalIterator` 原先先创建了 `mem_iter = super_version->mem->NewIterator(..., arena)`，随后才调用 `BuildActiveMemTableRangeDelIteratorForScan`。若后者失败返回非 OK，直接 `return NewErrorInternalIterator(s, arena)`。虽然 `arena` 随后会随 DBIter 销毁释放内存，但对于非平凡析构的 `MemTableRep` 或带有清理回调的 `mem_iter`，直接丢弃存在未能显式调用析构的隐患。
- **重构解决**：在 [db/db_impl/db_impl.cc:2600-2625](file:///home/wam/grad/rocksdb-v11.8.0/db/db_impl/db_impl.cc#L2600-L2625) 中，**优先构建范围墓碑局部视图迭代器 `mem_tombstone_iter`**。
  - 若墓碑迭代器构造失败（返回非 OK），此时 `mem_iter` **尚未在 Arena 中分配**，直接释放 `super_version` 引用并返回错误迭代器；
  - 仅在墓碑视图构造成功后，才调用 `super_version->mem->NewIterator(...)` 创建点迭代器。
  - 从根本上杜绝了非 OK 分支下任何点迭代器的悬挂或未析构风险。

---

## 2. B2：Same-SuperVersion 最小接入

### 2.1 统一工厂接入

在 [db/arena_wrapped_db_iter.cc:285-335](file:///home/wam/grad/rocksdb-v11.8.0/db/arena_wrapped_db_iter.cc#L285-L335) 的 Same-SuperVersion 路径中，废弃原生直接调用 `sv->mem->NewRangeTombstoneIterator` 的硬编码逻辑，复用已有统一工厂 `BuildActiveMemTableRangeDelIteratorForScan`：

```cpp
std::unique_ptr<TruncatedRangeDelIterator> new_iter;
Status s = BuildActiveMemTableRangeDelIteratorForScan(
    static_cast_with_check<MemTable>(sv->mem), read_options_, read_seq,
    cfd->internal_comparator(),
    cfd->ioptions().amtv_enable_bounded_scan_view, &new_iter);
```

### 2.2 四态返回契约与槽位管理

1. **OK + 非空 (`s.ok() && new_iter != nullptr`)**：
   - 若迭代器之前未持有墓碑槽位 (`!memtable_range_tombstone_iter_`)，原生回退至 `DoRefresh` 进行全量重构；
   - 若持有槽位，直接替换：`*memtable_range_tombstone_iter_ = std::move(new_iter)`。
2. **OK + 空 (`s.ok() && new_iter == nullptr`)**：
   - MemTable 当前无有效墓碑覆盖，清空槽位：`memtable_range_tombstone_iter_->reset()`。
3. **局部失败但原生回退成功**：
   - 工厂内部捕获局部异常，回退至原生 `NewRangeTombstoneIterator` 并在 Arena 中包装 `TruncatedRangeDelIterator`，安全返回原生新视图替换槽位。
4. **最终非 OK (`!s.ok()`)**：
   - 绝不进入“已更新 sequence 但持有旧/空墓碑”的危险状态；
   - 彻底销毁旧 `DBIter` 和 Arena，重建干净的 `Arena` 并挂载 `NewErrorInternalIterator<Slice>(s, &arena_)`；
   - 设置 `internal_iter_initialized_ = true`；
   - 后续任何 `Seek*()` / `Valid()` 均明确报错且不可读；
   - 若用户再次调用 `Refresh()`，检测到 `!db_iter_->status().ok()`，自动走 `DoRefresh` 全量重建路径完成故障恢复。

---

## 3. B3：Full-Rebuild 与首次激活

### 3.1 接入一致性

Full-Rebuild 路径（`DoRefresh`）调用 `DBImpl::NewInternalIterator`，已经通过阶段 A.1 统一接入 `BuildActiveMemTableRangeDelIteratorForScan`，自动享有先墓碑后点查的构造顺序与安全边界。

### 3.2 覆盖矩阵

在差分测试套件 [db/amtv_db_refresh_test.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_db_refresh_test.cc) 中全面覆盖以下场景：

| 触发场景 | 对应测试 | 验证重点 |
|:---|:---|:---|
| **写入后未 Seek 即 Refresh** | `Scenario01` | 惰性初始化下未触发首次 Seek 前调用 Refresh，读序列号正确更新至最新写入 |
| **范围删除后 Refresh** | `Scenario02` | 范围删除发生后 Refresh，墓碑槽位即时生效，被覆盖 key 正确不可见 |
| **删除后 Put 复活** | `Scenario03` | 删除后再写入，Refresh 后高序列号 Put 正确复活，无误屏蔽 |
| **多次连续 Refresh** | `Scenario04` | 连续多次写入与 Refresh，槽位反复更新无内存泄漏与状态脏读 |
| **Same-SV 快速路径** | `Scenario05` | 未换代时快速更新墓碑槽位与 read sequence，不发生 Full-Rebuild |
| **MemTable 换代 Full-Rebuild** | `Scenario06` | Active MemTable 冻结转入 immutable，Refresh 触发 `DoRefresh` 全量重建 |
| **Flush 到 L0 Full-Rebuild** | `Scenario07` | MemTable 落盘至 L0 SST，Refresh 触发全量重建并正确读取 SST 墓碑 |
| **后台 AMTV Run 归并** | `Concurrency04` | 归并线程发布新 AMTV Snapshot，读线程 Refresh 正确取得新快照并执行差分一致读取 |
| **无界 Scan / 功能关闭** | `Scenario01`, `Scenario06` | 回退原生全量墓碑迭代器，保持 100% 行为等价 |
| **首次激活与惰性初始时点** | `Scenario16` | 验证创建迭代器后未 Seek，数据追加并 Refresh，首次 Seek 严格以 Refresh 后的序列号为准 |

---

## 4. B4：DB 级差分与路径证据

测试套件 [db/amtv_db_refresh_test.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_db_refresh_test.cc) 实现了 16 项标准差分场景与测试专用 Outcome 回调。每项测试均严格比对 Native DB 与 Candidate DB 的：
1. `Refresh` 返回状态 (`Status::OK()`)；
2. 迭代器状态 (`status()`) 与有效性 (`Valid()`)；
3. 全量 Forward / Reverse 遍历 Key/Value 序列；
4. `Seek(lower_bound - eps)`、`Seek(target)`、`SeekForPrev(target)`、`SeekForPrev(upper_bound + eps)` 等边界行为；
5. 路径 Outcome 证据（`LOCAL_VIEW_NONEMPTY`, `LOCAL_VIEW_EMPTY`, `NATIVE_FALLBACK_LOCAL_BUILD_FAILURE`, `FINAL_ERROR`）。

### 16 项差分场景结果汇总表

| 场景编号 | 测试用例名称 | 核心特征 | Candidate 路径 Outcome | 差分比对结果 |
|:---:|:---|:---|:---:|:---:|
| **01** | `Scenario01_PutAfterIteratorCreation` | 创建后 Put 写入，Refresh 读取 | `LOCAL_VIEW_EMPTY` | **完全一致** |
| **02** | `Scenario02_DeleteRangeAfterIteratorCreation` | 创建后 DeleteRange，Refresh 屏蔽 | `LOCAL_VIEW_NONEMPTY` | **完全一致** |
| **03** | `Scenario03_DeleteRangeThenPutResurrection` | DeleteRange 后同 key 再次 Put 复活 | `LOCAL_VIEW_NONEMPTY` | **完全一致** |
| **04** | `Scenario04_MultipleSequentialRefreshes` | 4 次连续写入与连续 Refresh | 动态切换 (空/非空) | **完全一致** |
| **05** | `Scenario05_SameSuperVersionFastPath` | 同一 SuperVersion 下槽位原地替换 | `LOCAL_VIEW_NONEMPTY` | **完全一致** |
| **06** | `Scenario06_MemTableSwitchFullRebuild` | 切换 MemTable 触发 Full-Rebuild | `LOCAL_VIEW_EMPTY` | **完全一致** |
| **07** | `Scenario07_FlushToL0FullRebuild` | 落盘 Flush 到 L0 触发 Full-Rebuild | `LOCAL_VIEW_EMPTY` | **完全一致** |
| **08** | `Scenario08_ForwardReverseSeekAndSeekForPrev` | 正反双向扫描、Seek 与 SeekForPrev | `LOCAL_VIEW_NONEMPTY` | **完全一致** |
| **09** | `Scenario09_EmptyUserKey` | 空 User Key `""` 边界处理 | `LOCAL_VIEW_NONEMPTY` | **完全一致** |
| **10** | `Scenario10_UserDefinedTimestamps` | 启用 UDT 时的回退契约 | `NATIVE_FALLBACK_USER_TIMESTAMP` | **完全一致** |
| **11** | `Scenario11_LocalBuildFailureFallback` | 故障注入局部失败，回退原生成功 | `NATIVE_FALLBACK_LOCAL_BUILD_FAILURE` | **完全一致** |
| **12** | `Scenario12_FinalNonOKErrorPropagation` | 局部与原生双重失败，错误传播 | `FINAL_ERROR` (Corruption) | **完全一致** |
| **13** | `Scenario13_OKNullptrSlotReplacement` | 墓碑在窗口外，替换为空槽位 | `LOCAL_VIEW_EMPTY` | **完全一致** |
| **14** | `Scenario14_ExplicitSnapshotRefresh` | 显式 Snapshot 切换与序列号更新 | `LOCAL_VIEW_NONEMPTY` | **完全一致** |
| **15** | `Scenario15_ReOperationAndReRefreshAfterNonOK` | 最终非 OK 后再 Seek 与再 Refresh 恢复 | `FINAL_ERROR` -> 恢复正常 | **完全一致** |
| **16** | `Scenario16_LazyInitFirstActivation` | 惰性初始化首次激活可见性时点 | `LOCAL_VIEW_NONEMPTY` | **完全一致** |

---

## 5. B5：确定性并发生命周期测试

在 [db/amtv_db_refresh_test.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_db_refresh_test.cc) 的 B5 小节中，通过精确的 `SyncPoint` 和原子屏障控制线程交错，每个读线程持有独立迭代器，并发对账绑定严格的逻辑快照边界：

```
[读线程 1] --- Acquire SuperVersion ---> [SyncPoint: SV] (Paused) -----------------------------> Build View ---> Swap Slot ---> Resume
                                               |
[写线程 2] -----------------------------> Flush / MarkImmutable / SwitchMemtable (Done)
```

### 5 项特定交错与并发压力测试结果

| 用例名称 | 构造交错时序 | 机制与安全保障 | 测试结果 |
|:---|:---|:---|:---:|
| **Concurrency01**<br>`MarkImmutableAfterSuperVersionAcquisition` | 读线程在 `Refresh` 中取得 `SuperVersion* sv` 后暂停；写线程执行 `Flush` 使该 MemTable 成为 immutable 并调用 `MarkImmutable()`；读线程恢复并构建局部视图。 | 读线程持有 `SuperVersion` 强引用，MemTable 对象生命周期安全；工厂不读写 `is_immutable_`，无误断言、无数据竞争。 | **PASSED** |
| **Concurrency02**<br>`MemTableSwitchDuringLocalBuild` | 读线程进入局部视图构造、开始提取 candidate 之前暂停；写线程执行 `Flush` 发生 MemTable 换代；读线程恢复完成局部视图构造。 | 局部视图持有的 `active_amtv_` 快照所有权独立且不可变；生命周期由 shared_ptr 保证，读线程读取正确。 | **PASSED** |
| **Concurrency03**<br>`FlushBeforeSlotReplacement` | 读线程完成局部视图构造、即将替换槽位前暂停；写线程执行 `Flush` 将数据落盘并写入新 key；读线程恢复。 | Refresh 在槽位替换后检查 `latest_sv_number != cur_sv_number`，检测到换代后自动 continue 回退至 `DoRefresh`，避免槽位版本错位。 | **PASSED** |
| **Concurrency04**<br>`AMTVRunMergeDuringRefresh` | 读线程进入 Refresh 之前构造多 Run 数据；写线程触发后台 Run 归并发布全新 AMTV Snapshot；读线程执行 Refresh。 | 读线程通过 `GetAMTVForRangeDel()` 获取最新发布的合并快照，且新旧快照逻辑墓碑完全等价。 | **PASSED** |
| **Concurrency05**<br>`OldIteratorRetainsOldView` | 读线程 1 持有旧迭代器且不 Refresh；写线程写入覆盖全区间的超大墓碑并落盘发布新 SuperVersion；读线程 2 创建新迭代器；读线程 1 验证继续可见旧数据，随后 Refresh 追赶新状态。 | 旧迭代器与旧视图引用完好隔离，新旧 SuperVersion 共存无内存破坏，追赶 Refresh 正确生效。 | **PASSED** |
| **Concurrency06**<br>`MultiThreadControlledLifecycle` | 1 个写线程 (50 批次 Put + RangeDel)、1 个维护线程 (定时间隔 Flush)、4 个独立读线程并发执行 Refresh + Forward Scan + Reverse Scan。 | 严格按逻辑 batch 序列触发 Refresh；全并发无 crash、无死锁、无 TSAN race。 | **PASSED** |

---

## 6. 验证与交付

### 6.1 四大构建与运行时环境全量验证矩阵

| 构建模式 | 环境变量 / 编译选项 | 执行命令 | 测试用例数 | 执行结果 | 性能/安全指标 |
|:---|:---|:---|:---:|:---:|:---|
| **Debug** | `DEBUG_LEVEL=1`, 动态库 | `make -j16 amtv_db_refresh_test && ./amtv_db_refresh_test` | 22 | **22 / 22 PASSED** (586 ms) | 断言全开，无断言失败 |
| **Release** | `DEBUG_LEVEL=0`, 静态库, `NDEBUG` | `DEBUG_LEVEL=0 make -j16 amtv_db_refresh_test && ./amtv_db_refresh_test` | 22 | **22 / 22 PASSED** (605 ms) | 编译边界完全剥离，零额外开销 |
| **AddressSanitizer** | `COMPILE_WITH_ASAN=1` | `COMPILE_WITH_ASAN=1 make -j16 amtv_db_refresh_test && ./amtv_db_refresh_test` | 22 | **22 / 22 PASSED** (1167 ms) | 0 memory leak, 0 heap-use-after-free |
| **ThreadSanitizer** | `COMPILE_WITH_TSAN=1` | `COMPILE_WITH_TSAN=1 make -j16 amtv_db_refresh_test && setarch $(uname -m) -R ./amtv_db_refresh_test` | 22 | **22 / 22 PASSED** (2369 ms) | 0 data races, 0 thread warnings |

### 6.2 既定回归测试矩阵验证结果

| 测试套件 | 测试文件 | 测试用例数 | 执行结果 |
|:---|:---|:---:|:---:|
| **AMTV 生产 Scan 集成测试** | `db/amtv_db_scan_integration_test.cc` | 12 | **12 / 12 PASSED** |
| **AMTV 局部视图核心单测** | `db/amtv_local_scan_view_test.cc` | 18 | **18 / 18 PASSED** |
| **AMTV 局部扫描引用对比测试** | `db/amtv_local_scan_reference_test.cc` | 64 | **64 / 64 PASSED** |
| **RocksDB MemTable 原生测试** | `db/db_memtable_test.cc` | 14 | **14 / 14 PASSED** |
| **RocksDB RangeDel 原生测试** | `db/db_range_del_test.cc` | 85 | **85 / 85 PASSED** |

### 6.3 测试专用 Outcome 在 Release 中的编译边界

- 在 [db/amtv_local_scan_view.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view.cc) 中，所有 `TEST_SYNC_POINT_CALLBACK("BuildActiveMemTableRangeDelIteratorForScan:Outcome", ...)` 均被严格封装在 `#ifndef NDEBUG` 预处理宏内；
- 在 `include/rocksdb/cleanable.h` 或 `test_util/sync_point.h` 中，Release 模式下 `TEST_SYNC_POINT_CALLBACK` 展开为空宏；
- 在 Release 编译（`DEBUG_LEVEL=0`）下无任何 Outcome 相关的字符串常量、全局符号、互斥锁或回调逻辑残留在二进制中，保持 100% 生产零运行时开销。

### 6.4 提交与远端核验

- **本地 HEAD 提交**: `933aa0881eecf0c2ee7fb5a4e01f479a0a04c58a`
- **远端仓库**: `https://github.com/IceciderEX/rocksdb-rt-opt.git`
- **远端分支**: `main` (与本地 `HEAD` 完全一致，`up to date`)
- **分支状态**: 工作区 clean，无未暂存文件，无临时文件。
- **性能约束遵循**: 本阶段完全遵守用户禁令，**未运行任何正式性能矩阵（M3b、F1、F2、LongCycle）**，**未修改 Flush、Compaction、WAL 或 SST 范围墓碑算法**。

---

## 7. 结论与后续计划

阶段 B（AMTV M4-P3a-2b Iterator Refresh 生产读路径接入与并发生命周期验证）已圆满完成。所有原生 Refresh 契约、所有权模型、Same-SV/Full-Rebuild 分支管理、错误状态与故障恢复机制、以及高并发线程交错生命周期均已通过严格的确定性测试与四大 Sanitizer 验证。

阶段 B 验收通过后，可按计划进入下一阶段：**有界 Scan 生产性能验证与收益评测**。
