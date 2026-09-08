//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include <atomic>
#include <chrono>
#include <algorithm>
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Strong-typed operation tag for audit bucket classification (M3a)
enum class AuditOpType : uint8_t {
  kNone = 0,
  kGetLive = 1,
  kScanPlannedIntersect = 2,
  kScanIntersect = 3,
  kScanNonIntersect = 4,
  kPut = 5,
  kDeleteRange = 6,
  kMax = 7
};

inline const char* AuditOpTypeName(AuditOpType op) {
  switch (op) {
    case AuditOpType::kNone: return "None";
    case AuditOpType::kGetLive: return "GetLive";
    case AuditOpType::kScanPlannedIntersect: return "Scan-PlannedIntersect";
    case AuditOpType::kScanIntersect: return "Scan-Intersect";
    case AuditOpType::kScanNonIntersect: return "Scan-NonIntersect";
    case AuditOpType::kPut: return "Put";
    case AuditOpType::kDeleteRange: return "DeleteRange";
    default: return "Unknown";
  }
}

}  // namespace ROCKSDB_NAMESPACE

#ifdef ROCKSDB_READ_PATH_AUDIT

namespace ROCKSDB_NAMESPACE {

struct ReadPathAuditStats {
  // 1. Range tombstone fragmented view materialization count and time (cumulative thread-side time)
  uint64_t range_tombstone_view_materialization_count = 0;
  uint64_t range_tombstone_view_materialization_nanos = 0;

  // Write-side cache invalidation count
  uint64_t memtable_cache_invalidation_count = 0;

  // 4. FragmentedRangeTombstoneListCache::reader_mutex statistics (cumulative thread-side time)
  uint64_t fragment_build_lock_attempt_count = 0;
  uint64_t fragment_build_lock_contended_count = 0;
  uint64_t fragment_build_lock_contended_wait_nanos = 0;
  uint64_t fragment_build_cache_race_hit_count = 0;

  // 3. Breakdown of iterator prepare and covering lookup time (active vs imm memtable)
  uint64_t active_mem_tombstone_iter_prepare_count = 0;
  uint64_t active_mem_tombstone_iter_prepare_nanos = 0;
  uint64_t active_mem_tombstone_cover_lookup_count = 0;
  uint64_t active_mem_tombstone_cover_lookup_nanos = 0;

  uint64_t imm_mem_tombstone_iter_prepare_count = 0;
  uint64_t imm_mem_tombstone_iter_prepare_nanos = 0;
  uint64_t imm_mem_tombstone_cover_lookup_count = 0;
  uint64_t imm_mem_tombstone_cover_lookup_nanos = 0;

  // Iterator construction source statistics (exclusive time)
  uint64_t active_mem_iter_construct_count = 0;
  uint64_t active_mem_iter_construct_nanos = 0;
  uint64_t imm_mem_iter_construct_count = 0;
  uint64_t imm_mem_iter_construct_nanos = 0;
  uint64_t sst_iter_construct_count = 0;
  uint64_t sst_iter_construct_nanos = 0;

  // 5. Scan event counters
  uint64_t scan_range_del_reseek_count = 0;       // Reseek count triggered by range tombstones
  uint64_t scan_boundary_advance_count = 0;       // Boundary advance count (START pop + END pop)
  uint64_t scan_range_del_child_next_count = 0;   // Underlying Next count triggered by range del branches
  uint64_t scan_covered_skip_count = 0;           // Keys skipped due to tombstone covering
  // 6. Active MemTable telemetry (generation and tombstone count at materialization/contention)
  uint64_t last_materialization_memtable_id = 0;
  uint64_t last_materialization_tombstone_count = 0;
  uint64_t last_contended_memtable_id = 0;
  uint64_t last_contended_tombstone_count = 0;

  void Reset() {
    *this = ReadPathAuditStats();
  }

  void MergeFrom(const ReadPathAuditStats& o) {
    range_tombstone_view_materialization_count += o.range_tombstone_view_materialization_count;
    range_tombstone_view_materialization_nanos += o.range_tombstone_view_materialization_nanos;
    memtable_cache_invalidation_count += o.memtable_cache_invalidation_count;
    fragment_build_lock_attempt_count += o.fragment_build_lock_attempt_count;
    fragment_build_lock_contended_count += o.fragment_build_lock_contended_count;
    fragment_build_lock_contended_wait_nanos += o.fragment_build_lock_contended_wait_nanos;
    fragment_build_cache_race_hit_count += o.fragment_build_cache_race_hit_count;
    active_mem_tombstone_iter_prepare_count += o.active_mem_tombstone_iter_prepare_count;
    active_mem_tombstone_iter_prepare_nanos += o.active_mem_tombstone_iter_prepare_nanos;
    active_mem_tombstone_cover_lookup_count += o.active_mem_tombstone_cover_lookup_count;
    active_mem_tombstone_cover_lookup_nanos += o.active_mem_tombstone_cover_lookup_nanos;
    imm_mem_tombstone_iter_prepare_count += o.imm_mem_tombstone_iter_prepare_count;
    imm_mem_tombstone_iter_prepare_nanos += o.imm_mem_tombstone_iter_prepare_nanos;
    imm_mem_tombstone_cover_lookup_count += o.imm_mem_tombstone_cover_lookup_count;
    imm_mem_tombstone_cover_lookup_nanos += o.imm_mem_tombstone_cover_lookup_nanos;
    active_mem_iter_construct_count += o.active_mem_iter_construct_count;
    active_mem_iter_construct_nanos += o.active_mem_iter_construct_nanos;
    imm_mem_iter_construct_count += o.imm_mem_iter_construct_count;
    imm_mem_iter_construct_nanos += o.imm_mem_iter_construct_nanos;
    sst_iter_construct_count += o.sst_iter_construct_count;
    sst_iter_construct_nanos += o.sst_iter_construct_nanos;
    scan_range_del_reseek_count += o.scan_range_del_reseek_count;
    scan_boundary_advance_count += o.scan_boundary_advance_count;
    scan_range_del_child_next_count += o.scan_range_del_child_next_count;
    scan_covered_skip_count += o.scan_covered_skip_count;
    if (o.last_materialization_memtable_id > 0) {
      last_materialization_memtable_id = o.last_materialization_memtable_id;
      last_materialization_tombstone_count = o.last_materialization_tombstone_count;
    }
    if (o.last_contended_memtable_id > 0) {
      last_contended_memtable_id = o.last_contended_memtable_id;
      last_contended_tombstone_count = o.last_contended_tombstone_count;
    }
  }
};

extern thread_local AuditOpType g_current_audit_op_type;
extern thread_local ReadPathAuditStats g_read_path_audit_stats_bucket[static_cast<size_t>(AuditOpType::kMax)];
extern std::atomic<bool> g_read_path_audit_enabled;

inline void SetCurrentAuditOpType(AuditOpType op) {
  g_current_audit_op_type = op;
}

inline AuditOpType GetCurrentAuditOpType() {
  return g_current_audit_op_type;
}

inline ReadPathAuditStats* GetReadPathAuditStats(AuditOpType op = AuditOpType::kNone) {
  size_t idx = (op == AuditOpType::kNone) ? static_cast<size_t>(g_current_audit_op_type) : static_cast<size_t>(op);
  if (idx >= static_cast<size_t>(AuditOpType::kMax)) idx = 0;
  return &g_read_path_audit_stats_bucket[idx];
}

inline void ResetAllAuditStats() {
  for (size_t i = 0; i < static_cast<size_t>(AuditOpType::kMax); ++i) {
    g_read_path_audit_stats_bucket[i].Reset();
  }
}

// RAII Scope Guard for setting and restoring thread-local operation tag
class AuditOpScope {
 public:
  explicit AuditOpScope(AuditOpType op) : prev_op_(GetCurrentAuditOpType()) {
    SetCurrentAuditOpType(op);
  }
  ~AuditOpScope() {
    SetCurrentAuditOpType(prev_op_);
  }
  AuditOpScope(const AuditOpScope&) = delete;
  AuditOpScope& operator=(const AuditOpScope&) = delete;
 private:
  AuditOpType prev_op_;
};

inline bool IsReadPathAuditEnabled() {
  return g_read_path_audit_enabled.load(std::memory_order_relaxed);
}

inline void SetReadPathAuditEnabled(bool enabled) {
  g_read_path_audit_enabled.store(enabled, std::memory_order_relaxed);
}

struct AuditScopeTimer {
  uint64_t* target_nanos_ = nullptr;
  uint64_t start_time_ = 0;

  AuditScopeTimer() = default;
  explicit AuditScopeTimer(uint64_t* target_nanos) : target_nanos_(target_nanos) {
    if (IsReadPathAuditEnabled() && target_nanos_ != nullptr) {
      start_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
    }
  }

  void Start(uint64_t* target_nanos) {
    target_nanos_ = target_nanos;
    if (IsReadPathAuditEnabled() && target_nanos_ != nullptr) {
      start_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
    }
  }

  void Stop() {
    if (start_time_ > 0 && target_nanos_ != nullptr) {
      uint64_t end_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      *target_nanos_ += (end_time - start_time_);
      start_time_ = 0;
    }
  }

  ~AuditScopeTimer() {
    Stop();
  }
};

}  // namespace ROCKSDB_NAMESPACE

#define g_read_path_audit_stats (*(ROCKSDB_NAMESPACE::GetReadPathAuditStats()))

#define AUDIT_COUNT_ADD(counter_name, val)                  \
  do {                                                      \
    if (ROCKSDB_NAMESPACE::IsReadPathAuditEnabled()) {      \
      ROCKSDB_NAMESPACE::GetReadPathAuditStats()            \
          ->counter_name += (val);                          \
    }                                                       \
  } while (0)

#else

namespace ROCKSDB_NAMESPACE {

struct ReadPathAuditStats {
  void Reset() {}
};

class AuditOpScope {
 public:
  explicit AuditOpScope(AuditOpType) {}
  ~AuditOpScope() {}
  AuditOpScope(const AuditOpScope&) = delete;
  AuditOpScope& operator=(const AuditOpScope&) = delete;
};

inline void SetCurrentAuditOpType(AuditOpType) {}
inline AuditOpType GetCurrentAuditOpType() { return AuditOpType::kNone; }
inline void ResetAllAuditStats() {}

struct AuditScopeTimer {
  AuditScopeTimer() = default;
  explicit AuditScopeTimer(void*) {}
  ~AuditScopeTimer() { (void)this; }
  void Start(void*) {}
  void Stop() {}
};

inline bool IsReadPathAuditEnabled() { return false; }
inline void SetReadPathAuditEnabled(bool) {}

}  // namespace ROCKSDB_NAMESPACE

#define AUDIT_COUNT_ADD(counter_name, val) do {} while (0)

#endif

