//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
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
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class MemTable;

// Mode indicating how the active MemTable range tombstone iterator was constructed.
enum class AMTVScanMode {
  // Legitimate result: MemTable currently has no range tombstones.
  // out_iter is nullptr, and status is Status::OK().
  kEmpty,

  // Successfully constructed localized bounded range del view using AMTV sidecar.
  kLocal,

  // Either eligibility criteria were not met or local construction failed;
  // successfully constructed and fell back to the canonical native full iterator.
  kNativeFallback,
};

// Reasons why the scan view fell back to canonical native full range del view.
enum class AMTVScanFallbackReason {
  kNone,
  kDisabled,
  kImmutableMemTable,
  kUnbounded,
  kInvertedBounds,
  kAMTVUnavailable,
  kSnapshotUnavailable,
  kLocalBuildFailed,
};

// Diagnostic metadata collected during iterator construction.
struct AMTVScanBuildMetadata {
  AMTVScanMode mode = AMTVScanMode::kEmpty;
  AMTVScanFallbackReason fallback_reason = AMTVScanFallbackReason::kNone;
  uint64_t raw_tombstone_count = 0;
  uint64_t candidate_tombstone_count = 0;
};

// Owns the persistent deep-copied bounds, InternalKeys, and FragmentedRangeTombstoneList
// for a localized range tombstone scan view.
//
// Lifecycle:
// A shared_ptr to this state is tied via aliasing constructor to the
// std::shared_ptr<const FragmentedRangeTombstoneList> held inside
// FragmentedRangeTombstoneIterator. As long as TruncatedRangeDelIterator (Slot 0)
// lives, this state lives. When Slot 0 is replaced or destroyed, this state is destroyed.
struct LocalRangeDelViewState {
  std::string lower_bound_bytes;
  std::string upper_bound_bytes;
  std::unique_ptr<InternalKey> smallest_ikey;
  std::unique_ptr<InternalKey> largest_ikey;
  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list;
};

// Determines if a raw range tombstone [user_start_key, user_end_key) intersects
// with the bounded scan window [lower_bound, upper_bound).
// Both user keys and bounds are compared according to the given user comparator.
bool AMTVScanIsCandidateIntersecting(const Slice& user_start_key,
                                     const Slice& user_end_key,
                                     const Slice* lower_bound,
                                     const Slice* upper_bound,
                                     const Comparator* ucmp);

// Unified factory to build active MemTable TruncatedRangeDelIterator for scan paths.
//
// Contract:
// 1. Immediately executes out_iter->reset() upon entry (or returns InvalidArgument if out_iter is null).
// 2. Returns Status::OK() + (*out_iter == nullptr) when the MemTable currently has no range tombstones
//    (a legitimate, non-error result).
// 3. When ineligible or when local build fails, constructs the current native full iterator inside
//    the factory and returns Status::OK() with metadata indicating kNativeFallback.
// 4. Returns non-OK Status only when unable to provide any valid RangeDel iterator.
// 5. Refresh callers must unconditionally execute *slot0 = std::move(new_iter) on Status::OK(),
//    even if new_iter is nullptr (clearing old slot 0).
Status BuildActiveMemTableRangeDelIteratorForScan(
    MemTable* memtable,
    const ReadOptions& read_options,
    SequenceNumber read_seq,
    const InternalKeyComparator& icmp,
    bool enable_bounded_scan_view,
    std::unique_ptr<TruncatedRangeDelIterator>* out_iter,
    AMTVScanBuildMetadata* build_meta = nullptr);

}  // namespace ROCKSDB_NAMESPACE
