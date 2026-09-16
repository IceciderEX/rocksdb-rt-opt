# M4-P3a-2a.1 证据闭环与非 OK 资源审计验收报告

- **阶段**: AMTV M4-P3a-2a.1 (阶段 A 验收)
- **基线 Commit**: `9bb1204f2`
- **阶段 A 固定 Commit SHA**: `adad7719b6079088dbcc031275380343b5861938`
- **远端同步状态**: 已推送到 `rt-opt/main`
- **测试环境矩阵**: Debug (Level 1), Release (Level 0), ASan (Clang 18 AddressSanitizer), TSAN (Clang 18 ThreadSanitizer)

---

## 一、A1：并发状态 Debug 断言删除与契约收敛

### 1. 删除内容与原因
在统一工厂 [db/amtv_local_scan_view.cc](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view.cc) 中彻底删除了：
```cpp
assert(!memtable->IsImmutable());
```
**原因**：
1. 即使传入对象合法来自当前 `SuperVersion::mem` 槽位，在读线程取得该 SuperVersion 引用后至迭代器实际创建/初始化期间，并发写/维护线程完全可能将该 MemTable 置换（switch）并调用 `memtable->MarkImmutable()`。
2. `IsImmutable()` 属于 relaxed atomic 读取，在并发场景下虽无 data race，但该断言在合法 MemTable 换代场景下会误触发崩溃。

### 2. 替代同步契约
1. **输入来源由调用点保证**：在 [db/amtv_local_scan_view.h](file:///home/wam/grad/rocksdb-v11.8.0/db/amtv_local_scan_view.h) 中显式规范：调用方传入的 `memtable` 必须直接来自当前 `SuperVersion` 的活跃可写槽位（`super_version->mem`）。
2. **读热路径零跨线程共享状态读取**：工厂内部不再调用 `memtable->IsImmutable()`，完全避免热读路径不必要的跨线程状态访问与竞争。
3. **Immutable MemTable 保持原生**：`super_version->imm` 的调用链保持 100% 原生不变（由 `super_version->imm->AddIterators` 处理），完全隔离局部视图。
4. **不修改 `is_immutable_` 内存序**：不引入额外的内存栅栏或更强内存序，维持 RocksDB 原生轻量同步语义。

---

## 二、A2：Candidate 真实执行局部视图证据闭环

为排除“Candidate 结果正确仅因始终回退原生”的疑点，在工厂各返回分支增加了仅在测试构建启用的 SyncPoint 回调：`"BuildActiveMemTableRangeDelIteratorForScan:Outcome"`，**生产热路径无全局原子统计**。

### 1. 8 种定义路径观测矩阵

| 状态标识 | 触发条件 | 验证场景 | 实际观测结果 |
|:---|:---|:---|:---:|
| `LOCAL_VIEW_NONEMPTY` | 窗口内成功构建非空局部范围墓碑视图 | 场景 03, 04, 05, 06, 07, 08, 09 (snap2-4), 10, 11 (ts15-35) | **100% 确认命中** |
| `LOCAL_VIEW_EMPTY` | 窗口内无墓碑（全部墓碑不相交或 MemTable 无墓碑） | 场景 01 (无墓碑), 场景 02 (全部墓碑在窗外) | **100% 确认命中 (非原生回退)** |
| `NATIVE_FALLBACK_DISABLED` | 配置选项 `amtv_enable_bounded_scan_view = false` | 场景 12a | **100% 确认命中** |
| `NATIVE_FALLBACK_UNBOUNDED` | 无界 Scan (`lower_bound` 或 `upper_bound` 为空) | 场景 12c | **100% 确认命中** |
| `NATIVE_FALLBACK_AMTV_UNAVAILABLE` | AMTV 状态不可用或 Snapshot 损坏 | 场景 12e (注入 AMTVUnavailable) | **100% 确认命中** |
| `NATIVE_FALLBACK_LOCAL_BUILD_FAILURE` | 局部视图构建失败但原生回退成功 | 场景 12b (注入 LocalBuildFail) | **100% 确认命中并获得原生迭代器** |
| `NATIVE_FALLBACK_OTHER` | 非法/倒置边界 ($L \ge U$) | 场景 12d | **100% 确认命中** |
| `FINAL_ERROR` | 局部构建失败且原生回退也失败 / 参数非法 | 场景 12f (双故障注入) | **100% 确认命中** |

### 2. 关键发现：`ArenaWrappedDBIter` 惰性初始化生命周期
在测试中发现，`db_cand_->NewIterator(ro)` 返回的是 `ArenaWrappedDBIter`，其内部 `NewInternalIterator` 采取**惰性初始化**策略（直到首次 `SeekToFirst()`、`Seek()` 或 `Prepare()` 时才调用 `EnsureInternalIteratorInitialized`）。
测试辅助函数 `NewCandidateIterator(ro)` 在构造后立即触发首次 Seek 并捕获内部初始化期间的 SyncPoint 回调，精确捕获每次迭代器初始化所命中的真实路径。

---

## 三、A3：最终非 OK 早退路径逐行审计

逐行审计生产读路径 [db/db_impl/db_impl.cc:2608-2618](file:///home/wam/grad/rocksdb-v11.8.0/db/db_impl/db_impl.cc#L2608-L2618)：
```cpp
// Collect iterator for mutable memtable
auto mem_iter = super_version->mem->NewIterator(
    read_options, super_version->GetSeqnoToTimeMapping(), arena,
    super_version->mutable_cf_options.prefix_extractor.get(),
    /*for_flush=*/false);
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
}
```

### 1. 六项审计结论

1. **`mem_iter` 由谁拥有**：
   `mem_iter` 由传入的 `arena` 分配（`arena->AllocateAligned(sizeof(MemTableIterator))`）。其生命周期完全由 Arena 管理。
2. **错误返回时是否需要显式析构**：
   **不需要**。`MemTableIterator` 内部的 SkipList 迭代器也是通过 Arena 分配（`arena_mode_ = true`），未持有任何需要在析构函数中手动释放的堆资源、互斥锁或系统资源。当包裹该调用的 `ArenaWrappedDBIter` 或外部 Arena 析构时，所有 Arena 内存一次性批量释放。
3. **是否持有 Arena 外资源**：
   **不持有**。`MemTableIterator` 仅持有指向 MemTable 数据结构的只读引用，不拥有任何外部堆内存、套接字、文件句柄或锁。
4. **`CleanupSuperVersion` 是否与同函数现有早退路径一致**：
   **完全一致**。与 `DBImpl::NewInternalIterator` 尾部的原生非 OK 早退路径（[db/db_impl/db_impl.cc:2670-2672](file:///home/wam/grad/rocksdb-v11.8.0/db/db_impl/db_impl.cc#L2670-L2672)）完全一致：
   ```cpp
   } else {
     CleanupSuperVersion(super_version);
   }
   return NewErrorInternalIterator<Slice>(s, arena);
   ```
5. **是否可能出现 SuperVersion 重复清理或遗漏清理**：
   **不可能**：
   - **防遗漏**：在 `DBImpl::NewInternalIterator` 入口处（Line 2582），通过 `GetReferencedSuperVersion` 使引用计数加 1。若在 Line 2616 早退，直接调用 `CleanupSuperVersion(super_version)`（`super_version->Unref()`），引用计数恢复，无遗漏。
   - **防重复**：`internal_iter->RegisterCleanup(CleanupSuperVersionHandle, cleanup, nullptr)` 位于所有子迭代器成功组装之后的 Line 2666。由于在 Line 2617 已经提前 return，`SuperVersionHandle` 尚未分配，清理回调亦未注册。上层迭代器析构时不会再次 unref SuperVersion。正好且必须清理一次。
6. **Error Iterator 是否继续引用被清理的 SuperVersion**：
   **不引用**。`NewErrorInternalIterator<Slice>(s, arena)` 返回的 `ErrorInternalIterator` 仅保存 `Status status_`，没有任何指针指向 `super_version` 或其子组件，不存在野指针与 UAF 隐患。

### 2. 双故障注入与 ASan 验证（Scenario 12f）
在 `amtv_db_scan_integration_test` 的 `Scenario12_FallbackMatrix` 中实现了测试专用双故障注入：
- 注入 `LocalBuildFail` 促使局部视图构建失败并尝试原生回退；
- 注入 `FallbackFail` 促使原生回退也返回 `Status::Corruption("Injected native fallback failure")`；
- 验证 Candidate 迭代器进入非 OK 分支：
  - `cand_last_outcome_ == "FINAL_ERROR"`；
  - `it_c->Valid() == false`；
  - `it_c->status().IsCorruption() == true`；
  - 在 AddressSanitizer（ASan）下执行：**零内存泄漏、零 UAF、零重复释放**。

---

## 四、A4：阶段 A 多环境全量验证结果

| 测试套件 | 环境 | 结果 | 耗时 / 备注 |
|:---|:---|:---:|:---|
| `amtv_db_scan_integration_test` | Debug (Level 1) | **12 / 12 PASS** | 445 ms，全 8 种 Outcome 断言匹配 |
| `amtv_db_scan_integration_test` | Release (Level 0) | **12 / 12 PASS** | 397 ms |
| `amtv_db_scan_integration_test` | ASan | **12 / 12 PASS** | 879 ms，零错误，零泄漏 |
| `amtv_db_scan_integration_test` | TSAN | **12 / 12 PASS** | 1672 ms，零 race，零告警 |
| `amtv_local_scan_view_test` | Debug | **PASS** | 单元测试基线 |
| `amtv_local_scan_reference_test` | Debug | **64 / 64 PASS** | 775 ms，全消费参考基线 |
| `db_range_del_test` | Debug | **85 / 85 PASS** | 3335 ms，原生范围删除全回归 |
| `db_memtable_test` | Debug | **14 / 14 PASS** | 430 ms，MemTable 原生全回归 |

---

## 五、阶段 A 准出与阶段 B 待执行内容

- **阶段 A 结论**：P3a-2a.1 证据闭环全部完成，并发 Debug 断言已删除，8 种 Outcome 经端到端断言严格确认，非 OK 资源清理经源码审计与 ASan 故障注入验证完全无泄漏无 UAF。
- **阶段 B（待开始）**：在获得验收许可后，进入 **P3a-2b Iterator Refresh 生产接入**：
  1. B1：先审计 `ArenaWrappedDBIter::Refresh` 原生契约（不得先写代码再反推）；
  2. B2：Same-SuperVersion 最小接入（先构造、后替换）；
  3. B3：Full-Rebuild 验证；
  4. B4：Refresh DB 级差分测试（14 项场景）；
  5. B5：并发确定性生命周期测试。
