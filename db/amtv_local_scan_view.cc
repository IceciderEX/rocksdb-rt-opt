//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv_local_scan_view.h"

#include <algorithm>

#include "db/amtv.h"
#include "db/memtable.h"
#include "test_util/sync_point.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

bool AMTVScanIsCandidateIntersecting(const Slice& user_start_key,
                                     const Slice& user_end_key,
                                     const Slice* lower_bound,
                                     const Slice* upper_bound,
                                     const Comparator* ucmp) {
  if (lower_bound == nullptr || upper_bound == nullptr) {
    return true;
  }
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);

  // Filter 0: If L >= U, the scan window [L, U) is empty.
  if (ucmp->CompareWithoutTimestamp(*lower_bound, has_ts, *upper_bound,
                                    has_ts) >= 0) {
    return false;
  }

  // Filter 1: start < U (user_start_key < upper_bound)
  if (ucmp->CompareWithoutTimestamp(user_start_key, has_ts, *upper_bound,
                                    has_ts) >= 0) {
    return false;
  }

  // Filter 2: end > L (user_end_key > lower_bound)
  if (ucmp->CompareWithoutTimestamp(user_end_key, has_ts, *lower_bound,
                                    has_ts) <= 0) {
    return false;
  }

  return true;
}

Status BuildActiveMemTableRangeDelIteratorForScan(
    MemTable* memtable, const ReadOptions& read_options,
    SequenceNumber read_seq, const InternalKeyComparator& icmp,
    bool enable_bounded_scan_view,
    std::unique_ptr<TruncatedRangeDelIterator>* out_iter,
    AMTVScanBuildMetadata* build_meta) {
  if (out_iter == nullptr) {
    return Status::InvalidArgument("out_iter must not be null");
  }
  out_iter->reset();

  if (build_meta != nullptr) {
    *build_meta = AMTVScanBuildMetadata();
  }

  if (memtable == nullptr) {
    return Status::InvalidArgument("memtable must not be null");
  }

  if (read_options.ignore_range_deletions) {
    if (build_meta != nullptr) {
      build_meta->mode = AMTVScanMode::kEmpty;
      build_meta->fallback_reason = AMTVScanFallbackReason::kNone;
    }
    return Status::OK();
  }

  // Helper lambda for fallback to canonical native full range del view
  auto fallback_to_native = [&](AMTVScanFallbackReason reason) -> Status {
    if (build_meta != nullptr) {
      build_meta->fallback_reason = reason;
    }
    std::unique_ptr<FragmentedRangeTombstoneIterator> native_iter(
        memtable->NewRangeTombstoneIterator(read_options, read_seq,
                                            /*immutable_memtable=*/false));
    if (native_iter == nullptr || native_iter->empty()) {
      if (build_meta != nullptr) {
        build_meta->mode = AMTVScanMode::kEmpty;
      }
      *out_iter = nullptr;
      return Status::OK();
    }
    *out_iter = std::make_unique<TruncatedRangeDelIterator>(
        std::move(native_iter), &icmp, /*smallest=*/nullptr,
        /*largest=*/nullptr);
    if (build_meta != nullptr) {
      build_meta->mode = AMTVScanMode::kNativeFallback;
    }
    return Status::OK();
  };

  // 1. Eligibility: Feature flag & AMTV enabled on column family
  if (!enable_bounded_scan_view || !memtable->IsAMTVEnabled()) {
    return fallback_to_native(AMTVScanFallbackReason::kDisabled);
  }

  // 2. Eligibility: Must be mutable active MemTable
  if (memtable->IsImmutable()) {
    return fallback_to_native(AMTVScanFallbackReason::kImmutableMemTable);
  }

  // 3. Eligibility: Both lower and upper bounds must be present
  const Slice* lower_bound = read_options.iterate_lower_bound;
  const Slice* upper_bound = read_options.iterate_upper_bound;
  if (lower_bound == nullptr || upper_bound == nullptr) {
    return fallback_to_native(AMTVScanFallbackReason::kUnbounded);
  }

  // 4. Eligibility: Valid positive window [L, U) where L < U
  const auto* ucmp = icmp.user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);
  if (ucmp->CompareWithoutTimestamp(*lower_bound, has_ts, *upper_bound,
                                    has_ts) >= 0) {
    return fallback_to_native(AMTVScanFallbackReason::kInvertedBounds);
  }

  // 5. Eligibility: AMTV state and background merge health
  AMTVState* amtv_state = memtable->GetAMTVState();
  if (amtv_state == nullptr || amtv_state->is_fallback_required()) {
    return fallback_to_native(AMTVScanFallbackReason::kAMTVUnavailable);
  }

  // 6. Eligibility: Snapshot acquisition & fallback flag
  std::shared_ptr<const AMTVSnapshot> snapshot = amtv_state->GetSnapshot();
  if (snapshot == nullptr || snapshot->fallback_required) {
    return fallback_to_native(AMTVScanFallbackReason::kSnapshotUnavailable);
  }

  // 7. SyncPoint test hook for simulated local construction failure
  bool local_build_failed = false;
  TEST_SYNC_POINT_CALLBACK(
      "BuildActiveMemTableRangeDelIteratorForScan:LocalBuildFail",
      &local_build_failed);
  if (local_build_failed) {
    return fallback_to_native(AMTVScanFallbackReason::kLocalBuildFailed);
  }

  // 8. Extract intersecting raw candidate tombstones from snapshot
  uint64_t total_raw_count = 0;
  std::vector<OpenDeltaEntry> candidates;

  for (const auto& run : snapshot->sealed_runs) {
    if (run) {
      total_raw_count += run->raw_entries.size();
      for (const auto& entry : run->raw_entries) {
        if (AMTVScanIsCandidateIntersecting(entry.user_start_key(),
                                            entry.user_end_key(), lower_bound,
                                            upper_bound, ucmp)) {
          candidates.push_back(entry);
        }
      }
    }
  }

  if (snapshot->open_delta) {
    total_raw_count += snapshot->open_delta->size();
    for (const auto& entry : snapshot->open_delta->entries()) {
      if (AMTVScanIsCandidateIntersecting(entry.user_start_key(),
                                          entry.user_end_key(), lower_bound,
                                          upper_bound, ucmp)) {
        candidates.push_back(entry);
      }
    }
  }

  if (build_meta != nullptr) {
    build_meta->raw_tombstone_count = total_raw_count;
    build_meta->candidate_tombstone_count = candidates.size();
  }

  // If there are no raw tombstones or no intersecting candidates in the window,
  // return OK with null iterator (legitimate empty result).
  if (total_raw_count == 0 || candidates.empty()) {
    if (build_meta != nullptr) {
      build_meta->mode = AMTVScanMode::kEmpty;
      build_meta->fallback_reason = AMTVScanFallbackReason::kNone;
    }
    *out_iter = nullptr;
    return Status::OK();
  }

  // 9. Build LocalRangeDelViewState with deep-copied bounds
  auto state = std::make_shared<LocalRangeDelViewState>();
  state->lower_bound_bytes = lower_bound->ToString();
  state->upper_bound_bytes = upper_bound->ToString();

  std::string smallest_buf = state->lower_bound_bytes;
  std::string largest_buf = state->upper_bound_bytes;
  if (ts_sz > 0) {
    if (smallest_buf.size() < ts_sz) {
      smallest_buf.append(std::string(ts_sz, '\xff'));
    }
    if (largest_buf.size() < ts_sz) {
      largest_buf.append(std::string(ts_sz, '\xff'));
    }
  }
  state->smallest_ikey = std::make_unique<InternalKey>(
      smallest_buf, kMaxSequenceNumber, kTypeRangeDeletion);
  state->largest_ikey = std::make_unique<InternalKey>(
      largest_buf, kMaxSequenceNumber, kTypeRangeDeletion);

  std::sort(candidates.begin(), candidates.end(),
            [&icmp](const OpenDeltaEntry& a, const OpenDeltaEntry& b) {
              return icmp.Compare(a.ikey.Encode(), b.ikey.Encode()) < 0;
            });

  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(candidates.size());
  values.reserve(candidates.size());
  for (const auto& e : candidates) {
    keys.emplace_back(e.ikey.Encode().ToString());
    values.emplace_back(e.end_key);
  }

  auto vec_iter = std::make_unique<VectorIterator>(
      std::move(keys), std::move(values), &icmp);
  state->fragmented_list = std::make_shared<FragmentedRangeTombstoneList>(
      std::move(vec_iter), icmp);

  if (state->fragmented_list->empty()) {
    if (build_meta != nullptr) {
      build_meta->mode = AMTVScanMode::kEmpty;
      build_meta->fallback_reason = AMTVScanFallbackReason::kNone;
    }
    *out_iter = nullptr;
    return Status::OK();
  }

  // 10. Aliasing shared_ptr: binds lifetime of 'state' to 'aliased_list'
  // inside FragmentedRangeTombstoneIterator.
  std::shared_ptr<FragmentedRangeTombstoneList> aliased_list(
      state, state->fragmented_list.get());

  auto frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
      aliased_list, icmp, read_seq, read_options.timestamp);

  *out_iter = std::make_unique<TruncatedRangeDelIterator>(
      std::move(frag_iter), &icmp, state->smallest_ikey.get(),
      state->largest_ikey.get());

  if (build_meta != nullptr) {
    build_meta->mode = AMTVScanMode::kLocal;
    build_meta->fallback_reason = AMTVScanFallbackReason::kNone;
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
