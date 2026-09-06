//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include <atomic>
#include <chrono>
#include "rocksdb/rocksdb_namespace.h"

#ifdef ROCKSDB_READ_PATH_AUDIT

namespace ROCKSDB_NAMESPACE {

struct ReadPathAuditStats {
  // 1. Range tombstone fragmented view materialization count and time (exclusive time)
  uint64_t range_tombstone_view_materialization_count = 0;
  uint64_t range_tombstone_view_materialization_nanos = 0;

  // Write-side cache invalidation count
  uint64_t memtable_cache_invalidation_count = 0;

  // 4. Lock attempts, queuing contention, and race hit statistics (exclusive time)
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
};

extern thread_local ReadPathAuditStats g_read_path_audit_stats;
extern std::atomic<bool> g_read_path_audit_enabled;

inline ReadPathAuditStats* GetReadPathAuditStats() {
  return &g_read_path_audit_stats;
}

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

#define AUDIT_COUNT_ADD(counter_name, val)                  \
  do {                                                      \
    if (ROCKSDB_NAMESPACE::IsReadPathAuditEnabled()) {      \
      ROCKSDB_NAMESPACE::g_read_path_audit_stats            \
          .counter_name += (val);                           \
    }                                                       \
  } while (0)

#else

namespace ROCKSDB_NAMESPACE {

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
