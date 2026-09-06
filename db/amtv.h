//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "util/atomic.h"

namespace ROCKSDB_NAMESPACE {

// OpenDeltaEntry stores a range deletion with full native semantics:
// InternalKey (user_key, sequence, kTypeRangeDeletion), end_key, sequence, and timestamps.
// All keys independently own their memory to prevent UAF.
struct OpenDeltaEntry {
  InternalKey ikey;     // user_key + seq + kTypeRangeDeletion
  std::string end_key;  // end user key (with timestamp if enabled)
  SequenceNumber seq = 0;

  OpenDeltaEntry() = default;
  OpenDeltaEntry(const Slice& start_user_key, const Slice& end_user_key,
                 SequenceNumber s)
      : ikey(start_user_key, s, kTypeRangeDeletion),
        end_key(end_user_key.data(), end_user_key.size()),
        seq(s) {}

  Slice user_start_key() const { return ikey.user_key(); }
  Slice user_end_key() const { return Slice(end_key); }
  SequenceNumber sequence() const { return seq; }

  ParsedInternalKey parsed_start_key() const {
    return ParsedInternalKey(ikey.user_key(), seq, kTypeRangeDeletion);
  }
  ParsedInternalKey parsed_end_key() const {
    return ParsedInternalKey(Slice(end_key), kMaxSequenceNumber,
                             kTypeRangeDeletion);
  }

  Slice timestamp(size_t ts_sz) const {
    if (ts_sz == 0) {
      return Slice();
    }
    return ExtractTimestampFromUserKey(ikey.user_key(), ts_sz);
  }
};

// OpenDelta maintains unfragmented range tombstones (< amtv_delta_tombstones, e.g. 64)
// with copy-on-write semantics and independent memory ownership.
class OpenDelta {
 public:
  OpenDelta() = default;
  explicit OpenDelta(std::vector<OpenDeltaEntry> entries)
      : entries_(std::move(entries)) {}

  void AddEntry(const Slice& start_user_key, const Slice& end_user_key,
                SequenceNumber seq);

  size_t size() const { return entries_.size(); }
  bool empty() const { return entries_.empty(); }
  const std::vector<OpenDeltaEntry>& entries() const { return entries_; }

  // Check covering sequence number for user_key.
  // Returns max sequence number covering user_key with seq <= read_seq, or 0 if none.
  SequenceNumber MaxCoveringTombstoneSeqnum(
      const Slice& user_key, const Comparator* ucmp, SequenceNumber read_seq,
      std::string* out_ts = nullptr,
      const Slice* ts_upper_bound = nullptr) const;

  // Build a native FragmentedRangeTombstoneList from the delta entries.
  std::unique_ptr<FragmentedRangeTombstoneList>
  BuildFragmentedRangeTombstoneList(const InternalKeyComparator& icmp) const;

  // Deep clone ensuring memory independence.
  std::shared_ptr<OpenDelta> Clone() const;

 private:
  std::vector<OpenDeltaEntry> entries_;
};

// Test hook: counter to verify zero materialization of OpenDelta during Get/MultiGet.
extern std::atomic<uint64_t> test_open_delta_materialize_count;

// Immutable AMTVRun representing a sealed run of range tombstones.
struct AMTVRun {
  uint64_t run_id = 0;
  uint32_t level = 0;               // 0, 1, 2, ...
  uint64_t source_chunk_count = 1;  // Number of base delta chunks (uint64_t)
  bool is_partial = false;          // True if sealed from a non-full Open Delta
  std::vector<OpenDeltaEntry> raw_entries;
  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list;
  uint64_t tombstone_count = 0;
  SequenceNumber min_seq = kMaxSequenceNumber;
  SequenceNumber max_seq = 0;

  AMTVRun() = default;
  AMTVRun(uint64_t id, uint32_t lvl, uint64_t chunk_count, bool partial,
          std::vector<OpenDeltaEntry> entries,
          const InternalKeyComparator& icmp);
  AMTVRun(uint64_t id, std::vector<OpenDeltaEntry> entries,
          const InternalKeyComparator& icmp)
      : AMTVRun(id, 0, 1, false, std::move(entries), icmp) {}
};

// Returns true if r1 and r2 are eligible for binary size-tiered merge.
inline bool CanMergeRuns(const AMTVRun& r1, const AMTVRun& r2) {
  return !r1.is_partial && !r2.is_partial && r1.level == r2.level &&
         r1.source_chunk_count == r2.source_chunk_count;
}

// Find lowest level pair of same-level, non-partial runs with smallest run_ids.
bool FindMergePair(
    const std::vector<std::shared_ptr<const AMTVRun>>& sealed_runs,
    std::shared_ptr<const AMTVRun>* out_run_a,
    std::shared_ptr<const AMTVRun>* out_run_b);

// Returns true if there exists at least one pair of mergeable runs.
bool HasMergeablePair(
    const std::vector<std::shared_ptr<const AMTVRun>>& sealed_runs);

// Immutable snapshot of AMTV state published atomically.
struct AMTVSnapshot {
  uint64_t memtable_generation = 0;
  uint64_t publish_epoch = 0;

  // M2b: sealed_runs holds immutable shared_ptr<const AMTVRun>.
  // Shallow copying during snapshot creation is O(runs) pointer copies,
  // zero copying of raw entries.
  std::vector<std::shared_ptr<const AMTVRun>> sealed_runs;

  // Open delta: unsealed range tombstones (< delta_tombstones_limit)
  std::shared_ptr<const OpenDelta> open_delta;

  // Fallback safety flag: if true, Get/MultiGet must bypass multi-source adapter
  // and fall back to canonical native range tombstone query.
  bool fallback_required = false;

  // Total tombstone count when fallback was triggered
  uint64_t tombstones_at_fallback = 0;

  uint32_t sealed_run_count() const {
    return static_cast<uint32_t>(sealed_runs.size());
  }
  uint32_t sealed_layer_count() const {
    return sealed_run_count();
  }

  uint64_t total_tombstones() const {
    uint64_t count = 0;
    for (const auto& r : sealed_runs) {
      if (r) {
        count += r->tombstone_count;
      }
    }
    if (open_delta) {
      count += open_delta->size();
    }
    return count;
  }
};

// Multi-source semantic adapter for M1/M2.
// Aggregates Sealed Runs + Open Delta without custom sweep-line.
class AMTVMultiSourceAdapter {
 public:
  AMTVMultiSourceAdapter(std::shared_ptr<const AMTVSnapshot> snapshot,
                         const InternalKeyComparator* icmp)
      : snapshot_(std::move(snapshot)), icmp_(icmp) {}

  // Max covering sequence number across Sealed Runs and Open Delta.
  SequenceNumber MaxCoveringTombstoneSeqnum(
      const Slice& user_key, SequenceNumber read_seq,
      std::string* out_ts = nullptr,
      const Slice* ts_upper_bound = nullptr) const;

  // Add Sealed Runs and Open Delta to a native ReadRangeDelAggregator.
  // Mandatory reference prevents dangling pointer hazards when open_delta creates temporary fragmented lists.
  void AddToRangeDelAggregator(
      ReadRangeDelAggregator* agg, SequenceNumber read_seq,
      std::vector<std::unique_ptr<FragmentedRangeTombstoneList>>&
          pinned_open_lists) const;

  const AMTVSnapshot* snapshot() const { return snapshot_.get(); }

 private:
  std::shared_ptr<const AMTVSnapshot> snapshot_;
  const InternalKeyComparator* icmp_;
};

// Lifecycle state of AMTV background merge task (M2c.1)
enum class MergeTaskState {
  kIdle,
  kSubmitting,
  kQueued,
  kRunning
};

// AMTVState manages the lifecycle and atomic publication of AMTVSnapshot.
class AMTVState : public std::enable_shared_from_this<AMTVState> {
 public:
  explicit AMTVState(uint64_t memtable_generation = 0,
                     uint32_t delta_tombstones_limit = 64,
                     uint32_t merge_soft_limit = 2,
                     uint32_t hard_layer_limit = 8,
                     const InternalKeyComparator* icmp = nullptr,
                     Env* env = nullptr);
  ~AMTVState();

  // Read path: acquire snapshot lock-free.
  std::shared_ptr<const AMTVSnapshot> GetSnapshot() const {
    return AtomicSharedPtrLoad(&snapshot_, std::memory_order_acquire);
  }

  // Write path: append tombstone and publish snapshot atomically.
  void AddTombstone(const Slice& start_user_key, const Slice& end_user_key,
                    SequenceNumber seq, const InternalKeyComparator& icmp);

  // Freeze open delta into a new sealed AMTVRun.
  void FreezeOpenDelta(const InternalKeyComparator& icmp);

  // Background merge scheduling & task
  void MaybeScheduleMerge();
  static void BGMergeWrapper(void* arg);
  static void BGMergeUnschedule(void* arg);
  void OnTaskUnscheduled();
  void BGMergeTask();

  // TEST-ONLY: For deterministic testing. Not used in production path.
  bool TEST_RunMergeSynchronously();
  bool RunMergeSynchronously() { return TEST_RunMergeSynchronously(); }

  // Explicit drain and cancellation protocol (P0-1, M2c.1)
  void CancelAndDrain();

  // Precise stability check and wait protocol (P0-2, M2c.1)
  bool IsMergeStable() const;
  void WaitForMergeStable();

  // Lifecycle transitions
  void MarkImmutable() {
    is_immutable_.store(true, std::memory_order_release);
  }
  void Invalidate() {
    is_invalidated_.store(true, std::memory_order_release);
  }
  bool is_immutable() const {
    return is_immutable_.load(std::memory_order_relaxed);
  }
  bool is_invalidated() const {
    return is_invalidated_.load(std::memory_order_relaxed);
  }

  // Telemetry & metrics (recorded on low-frequency paths: seal, merge, fallback)
  void RecordGetFallback() {
    fallback_to_native_count_.fetch_add(1, std::memory_order_relaxed);
  }

  uint32_t peak_sealed_runs() const {
    return peak_sealed_layers_.load(std::memory_order_relaxed);
  }
  uint32_t peak_sealed_layers() const {
    return peak_sealed_runs();
  }
  uint64_t get_fallback_to_native_count() const {
    return fallback_to_native_count_.load(std::memory_order_relaxed);
  }
  uint64_t tombstones_at_fallback() const {
    return tombstones_at_fallback_.load(std::memory_order_relaxed);
  }
  uint32_t runs_at_fallback() const {
    return runs_at_fallback_.load(std::memory_order_relaxed);
  }
  bool is_fallback_required() const {
    return fallback_required_.load(std::memory_order_relaxed);
  }
  bool is_merge_in_progress() const;
  MergeTaskState task_state() const;
  int queued_tasks() const;
  int running_tasks() const;

  uint64_t merge_requested() const {
    return merge_requested_.load(std::memory_order_relaxed);
  }
  uint64_t merge_completed() const {
    return merge_completed_.load(std::memory_order_relaxed);
  }
  uint64_t merge_discarded() const {
    return merge_discarded_.load(std::memory_order_relaxed);
  }
  uint64_t merge_unscheduled() const {
    return merge_unscheduled_.load(std::memory_order_relaxed);
  }
  uint64_t merge_input_run_count() const {
    return merge_input_run_count_.load(std::memory_order_relaxed);
  }
  uint64_t merge_input_tombstones() const {
    return merge_input_tombstones_.load(std::memory_order_relaxed);
  }
  uint64_t merge_wall_time_nanos() const {
    return merge_wall_time_nanos_.load(std::memory_order_relaxed);
  }
  uint64_t merge_cpu_time_nanos() const {
    return merge_cpu_time_nanos_.load(std::memory_order_relaxed);
  }
  uint64_t task_queue_wait_time_nanos() const {
    return task_queue_wait_time_nanos_.load(std::memory_order_relaxed);
  }
  // Byte proxy metrics: only accounts for sizeof(OpenDeltaEntry) * entry_count.
  // Does not include string heap memory, FragmentedRangeTombstoneList, vector capacity, or coexisting snapshots.
  uint64_t raw_entries_struct_bytes_peak() const {
    return raw_entries_struct_bytes_peak_.load(std::memory_order_relaxed);
  }
  uint64_t in_flight_merge_struct_bytes_peak() const {
    return in_flight_merge_struct_bytes_peak_.load(std::memory_order_relaxed);
  }
  uint64_t raw_entries_bytes_peak() const {
    return raw_entries_struct_bytes_peak();
  }
  uint64_t in_flight_merge_bytes_peak() const {
    return in_flight_merge_struct_bytes_peak();
  }
  uint64_t fallback_event_count() const {
    return fallback_event_count_.load(std::memory_order_relaxed);
  }

  uint64_t memtable_generation() const { return memtable_generation_; }
  uint32_t delta_tombstones_limit() const { return delta_tombstones_limit_; }
  uint32_t merge_soft_limit() const { return merge_soft_limit_; }
  uint32_t hard_layer_limit() const { return hard_layer_limit_; }
  uint32_t hard_run_limit() const { return hard_layer_limit_; }

  std::string priority_used() const;
  std::string GetAuditSummary(uint64_t original_tombstones = 0) const;

 private:
  const uint64_t memtable_generation_;
  const uint32_t delta_tombstones_limit_;
  const uint32_t merge_soft_limit_;
  const uint32_t hard_layer_limit_;
  const InternalKeyComparator* icmp_{nullptr};
  std::unique_ptr<InternalKeyComparator> fallback_icmp_;
  Env* env_{nullptr};

  // Synchronizes write-side mutations. Never entered by readers.
  mutable port::Mutex write_mutex_;

  // Next run_id to assign to a newly sealed AMTVRun. Protected by write_mutex_ or atomic.
  std::atomic<uint64_t> next_run_id_{1};

  // Atomic pointer holding current immutable snapshot.
  std::shared_ptr<const AMTVSnapshot> snapshot_;

  // Concurrency & Lifecycle flags
  std::atomic<bool> merge_in_progress_{false};
  std::atomic<bool> is_immutable_{false};
  std::atomic<bool> is_invalidated_{false};

  // Fast write-side atomic flag to immediately stop delta accumulation
  std::atomic<bool> fallback_required_{false};
  std::atomic<uint64_t> tombstones_at_fallback_{0};
  std::atomic<uint32_t> peak_sealed_layers_{0};
  std::atomic<uint64_t> fallback_to_native_count_{0};

  // Task synchronization and lifecycle tracking (P0-1, P0-2, M2c.1)
  mutable port::Mutex task_mu_;
  mutable port::CondVar task_cond_;
  MergeTaskState task_state_{MergeTaskState::kIdle};
  Env::Priority last_scheduled_priority_{Env::Priority::LOW};
  std::string priority_used_{"UNKNOWN"};
  uint64_t last_scheduled_time_nanos_{0};

  // M2c Audit metrics
  std::atomic<uint64_t> merge_requested_{0};
  std::atomic<uint64_t> merge_completed_{0};
  std::atomic<uint64_t> merge_discarded_{0};
  std::atomic<uint64_t> merge_unscheduled_{0};
  std::atomic<uint64_t> merge_input_run_count_{0};
  std::atomic<uint64_t> merge_input_tombstones_{0};
  std::atomic<uint64_t> merge_wall_time_nanos_{0};
  std::atomic<uint64_t> merge_cpu_time_nanos_{0};
  std::atomic<uint64_t> task_queue_wait_time_nanos_{0};
  std::atomic<uint64_t> fallback_event_count_{0};
  std::atomic<uint32_t> runs_at_fallback_{0};
  std::atomic<uint64_t> raw_entries_struct_bytes_peak_{0};
  std::atomic<uint64_t> in_flight_merge_struct_bytes_peak_{0};

  // Per-level breakdown (protected by write_mutex_)
  std::map<uint32_t, uint64_t> merge_count_per_level_;
  std::map<uint32_t, uint64_t> merge_input_tombstones_per_level_;
  std::map<uint32_t, uint32_t> peak_run_level_histogram_;
};

}  // namespace ROCKSDB_NAMESPACE
