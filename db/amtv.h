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

// Immutable snapshot of AMTV state published atomically.
struct AMTVSnapshot {
  uint64_t memtable_generation = 0;
  uint64_t publish_epoch = 0;

  // Stable base: fragmented range tombstone list (can be nullptr if empty)
  std::shared_ptr<FragmentedRangeTombstoneList> base;

  // Sealed deltas: frozen fragmented range tombstone lists
  std::vector<std::shared_ptr<FragmentedRangeTombstoneList>> sealed_deltas;

  // Open delta: unsealed range tombstones (< delta_tombstones_limit)
  std::shared_ptr<const OpenDelta> open_delta;

  // Fallback safety flag: if true, Get/MultiGet must bypass multi-source adapter
  // and fall back to canonical native range tombstone query.
  bool fallback_required = false;

  // Total tombstone count when fallback was triggered
  uint64_t tombstones_at_fallback = 0;

  uint32_t sealed_layer_count() const {
    return static_cast<uint32_t>(sealed_deltas.size());
  }

  uint64_t total_tombstones() const {
    uint64_t count = 0;
    if (base) {
      count += base->num_unfragmented_tombstones();
    }
    for (const auto& sd : sealed_deltas) {
      if (sd) {
        count += sd->num_unfragmented_tombstones();
      }
    }
    if (open_delta) {
      count += open_delta->size();
    }
    return count;
  }
};

// Multi-source semantic adapter for M1a/M1b.
// Aggregates Base + Sealed Deltas + Open Delta without custom sweep-line.
class AMTVMultiSourceAdapter {
 public:
  AMTVMultiSourceAdapter(std::shared_ptr<const AMTVSnapshot> snapshot,
                         const InternalKeyComparator* icmp)
      : snapshot_(std::move(snapshot)), icmp_(icmp) {}

  // Max covering sequence number across Base, Sealed Deltas, and Open Delta.
  SequenceNumber MaxCoveringTombstoneSeqnum(
      const Slice& user_key, SequenceNumber read_seq,
      std::string* out_ts = nullptr,
      const Slice* ts_upper_bound = nullptr) const;

  // Add Base, Sealed Deltas, and Open Delta to a native ReadRangeDelAggregator.
  void AddToRangeDelAggregator(
      ReadRangeDelAggregator* agg, SequenceNumber read_seq,
      std::vector<std::unique_ptr<FragmentedRangeTombstoneList>>*
          pinned_open_lists = nullptr) const;

  const AMTVSnapshot* snapshot() const { return snapshot_.get(); }

 private:
  std::shared_ptr<const AMTVSnapshot> snapshot_;
  const InternalKeyComparator* icmp_;
};

// AMTVState manages the lifecycle and atomic publication of AMTVSnapshot.
class AMTVState {
 public:
  explicit AMTVState(uint64_t memtable_generation = 0,
                     uint32_t delta_tombstones_limit = 64,
                     uint32_t merge_soft_limit = 4,
                     uint32_t hard_layer_limit = 8);
  ~AMTVState() = default;

  // Read path: acquire snapshot lock-free.
  std::shared_ptr<const AMTVSnapshot> GetSnapshot() const {
    return AtomicSharedPtrLoad(&snapshot_, std::memory_order_acquire);
  }

  // Write path: append tombstone and publish snapshot atomically.
  void AddTombstone(const Slice& start_user_key, const Slice& end_user_key,
                    SequenceNumber seq, const InternalKeyComparator& icmp);

  // Set base explicitly (used during state initialization or testing).
  void SetBase(std::shared_ptr<FragmentedRangeTombstoneList> base);

  // Freeze open delta into sealed delta.
  void FreezeOpenDelta(const InternalKeyComparator& icmp);

  // Telemetry & metrics (recorded only upon fallback to protect hot paths)
  void RecordGetFallback(uint64_t tombstones = 0) {
    fallback_to_native_count_.fetch_add(1, std::memory_order_relaxed);
    if (tombstones > 0) {
      total_fallback_tombstones_.fetch_add(tombstones, std::memory_order_relaxed);
      last_fallback_tombstones_.store(tombstones, std::memory_order_relaxed);
    }
  }

  uint32_t peak_sealed_layers() const {
    return peak_sealed_layers_.load(std::memory_order_relaxed);
  }
  uint64_t get_fallback_to_native_count() const {
    return fallback_to_native_count_.load(std::memory_order_relaxed);
  }
  uint64_t total_fallback_tombstones() const {
    return total_fallback_tombstones_.load(std::memory_order_relaxed);
  }
  uint64_t last_fallback_tombstones() const {
    return last_fallback_tombstones_.load(std::memory_order_relaxed);
  }
  uint64_t tombstones_at_fallback() const {
    return tombstones_at_fallback_.load(std::memory_order_relaxed);
  }
  bool is_fallback_required() const {
    return fallback_required_.load(std::memory_order_relaxed);
  }

  uint64_t memtable_generation() const { return memtable_generation_; }
  uint32_t delta_tombstones_limit() const { return delta_tombstones_limit_; }
  uint32_t merge_soft_limit() const { return merge_soft_limit_; }
  uint32_t hard_layer_limit() const { return hard_layer_limit_; }

 private:
  const uint64_t memtable_generation_;
  const uint32_t delta_tombstones_limit_;
  const uint32_t merge_soft_limit_;
  const uint32_t hard_layer_limit_;

  // Synchronizes write-side mutations. Never entered by readers.
  mutable port::Mutex write_mutex_;

  // Atomic pointer holding current immutable snapshot.
  std::shared_ptr<const AMTVSnapshot> snapshot_;

  // Fast write-side atomic flag to immediately stop delta accumulation
  std::atomic<bool> fallback_required_{false};
  std::atomic<uint64_t> tombstones_at_fallback_{0};
  std::atomic<uint32_t> peak_sealed_layers_{0};
  std::atomic<uint64_t> fallback_to_native_count_{0};
  std::atomic<uint64_t> total_fallback_tombstones_{0};
  std::atomic<uint64_t> last_fallback_tombstones_{0};
};

}  // namespace ROCKSDB_NAMESPACE
