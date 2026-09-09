//  Copyright (c) 2026-present. All rights reserved.
//  Test-only implementation and test suite for AMTV M4-P1b-0: Run-Internal
//  Prefix-Max-End Interval Index Prototype with Boundary Clipping and 3-Way Oracle.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#include <algorithm>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "db/amtv.h"
#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

// ==========================================================================
// ClippedTombstoneFragment
// Represents a range tombstone fragment clipped to [L, U).
// ==========================================================================
struct ClippedTombstoneFragment {
  std::string start_key;  // User key (with max timestamp if UDT enabled)
  std::string end_key;    // User key (with max timestamp if UDT enabled)
  SequenceNumber seq = 0;
  std::string timestamp;  // UDT timestamp if enabled, or empty

  ClippedTombstoneFragment() = default;
  ClippedTombstoneFragment(std::string s, std::string e, SequenceNumber sq,
                           std::string ts = "")
      : start_key(std::move(s)),
        end_key(std::move(e)),
        seq(sq),
        timestamp(std::move(ts)) {}

  bool operator==(const ClippedTombstoneFragment& o) const {
    return start_key == o.start_key && end_key == o.end_key && seq == o.seq &&
           timestamp == o.timestamp;
  }

  std::string ToString() const {
    return "[" + start_key + ", " + end_key + ")@" + std::to_string(seq) +
           (timestamp.empty() ? "" : (", ts=" + timestamp));
  }
};

// ==========================================================================
// Stream Clipping Utilities (Forward & Backward)
// CRITICAL ARCHITECTURAL DISCLAIMER:
// GetClippedFragmentStreamForward and GetClippedFragmentStreamBackward are
// strictly TEST-ONLY output clipping adapters used to establish a bounded
// truth stream for differential oracle verification. They do NOT construct or
// constitute an online, bounded InternalIterator capable of being plugged
// into RocksDB's MergingIterator production read path.
//
// Safely clips a FragmentedRangeTombstoneIterator output stream to [L, U).
// Preserves sequence number, type, and user-defined timestamp.
// For user timestamps, uses RocksDB's canonical AppendKeyWithMaxTimestamp.
// ==========================================================================
inline std::vector<ClippedTombstoneFragment> GetClippedFragmentStreamForward(
    FragmentedRangeTombstoneIterator* iter, const Slice* lower_bound,
    const Slice* upper_bound, const InternalKeyComparator& icmp) {
  std::vector<ClippedTombstoneFragment> result;
  if (!iter || iter->empty()) {
    return result;
  }
  const auto* ucmp = icmp.user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);

  // Empty window check: L >= U produces empty stream
  if (lower_bound != nullptr && !lower_bound->empty() &&
      upper_bound != nullptr && !upper_bound->empty()) {
    if (ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound,
                                      false) >= 0) {
      return result;
    }
  }

  // Seek to L if provided, otherwise SeekToFirst
  if (lower_bound != nullptr && !lower_bound->empty()) {
    if (has_ts) {
      std::string l_with_max_ts;
      AppendKeyWithMaxTimestamp(&l_with_max_ts, *lower_bound, ts_sz);
      iter->Seek(l_with_max_ts);
    } else {
      iter->Seek(*lower_bound);
    }
  } else {
    iter->SeekToFirst();
  }

  while (iter->Valid()) {
    Slice cur_start = iter->start_key();
    Slice cur_end = iter->end_key();

    // If cur_start >= U, we have passed the upper bound
    if (upper_bound != nullptr && !upper_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(cur_start, has_ts, *upper_bound,
                                        false) >= 0) {
        break;
      }
    }

    // If cur_end <= L, skip fragments ending at or before lower bound
    if (lower_bound != nullptr && !lower_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(cur_end, has_ts, *lower_bound,
                                        false) <= 0) {
        iter->TopNext();
        continue;
      }
    }

    // Clip start: max(cur_start, L)
    std::string clipped_start;
    if (lower_bound != nullptr && !lower_bound->empty() &&
        ucmp->CompareWithoutTimestamp(cur_start, has_ts, *lower_bound,
                                      false) < 0) {
      if (has_ts) {
        AppendKeyWithMaxTimestamp(&clipped_start, *lower_bound, ts_sz);
      } else {
        clipped_start.assign(lower_bound->data(), lower_bound->size());
      }
    } else {
      clipped_start.assign(cur_start.data(), cur_start.size());
    }

    // Clip end: min(cur_end, U)
    std::string clipped_end;
    if (upper_bound != nullptr && !upper_bound->empty() &&
        ucmp->CompareWithoutTimestamp(cur_end, has_ts, *upper_bound,
                                      false) > 0) {
      if (has_ts) {
        AppendKeyWithMaxTimestamp(&clipped_end, *upper_bound, ts_sz);
      } else {
        clipped_end.assign(upper_bound->data(), upper_bound->size());
      }
    } else {
      clipped_end.assign(cur_end.data(), cur_end.size());
    }

    std::string ts_str;
    if (has_ts) {
      Slice ts = iter->timestamp();
      ts_str.assign(ts.data(), ts.size());
    }

    result.emplace_back(std::move(clipped_start), std::move(clipped_end),
                        iter->seq(), std::move(ts_str));
    iter->TopNext();
  }
  return result;
}

inline std::vector<ClippedTombstoneFragment> GetClippedFragmentStreamBackward(
    FragmentedRangeTombstoneIterator* iter, const Slice* lower_bound,
    const Slice* upper_bound, const InternalKeyComparator& icmp) {
  std::vector<ClippedTombstoneFragment> result;
  if (!iter || iter->empty()) {
    return result;
  }
  const auto* ucmp = icmp.user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);

  // Empty window check: L >= U produces empty stream
  if (lower_bound != nullptr && !lower_bound->empty() &&
      upper_bound != nullptr && !upper_bound->empty()) {
    if (ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound,
                                      false) >= 0) {
      return result;
    }
  }

  // SeekForPrev to U if provided, otherwise SeekToLast
  if (upper_bound != nullptr && !upper_bound->empty()) {
    if (has_ts) {
      std::string u_with_max_ts;
      AppendKeyWithMaxTimestamp(&u_with_max_ts, *upper_bound, ts_sz);
      iter->SeekForPrev(u_with_max_ts);
    } else {
      iter->SeekForPrev(*upper_bound);
    }
  } else {
    iter->SeekToLast();
  }

  while (iter->Valid()) {
    Slice cur_start = iter->start_key();
    Slice cur_end = iter->end_key();

    // If cur_end <= L, we have walked before the lower bound
    if (lower_bound != nullptr && !lower_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(cur_end, has_ts, *lower_bound,
                                        false) <= 0) {
        break;
      }
    }

    // If cur_start >= U, skip fragments starting at or after upper bound
    if (upper_bound != nullptr && !upper_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(cur_start, has_ts, *upper_bound,
                                        false) >= 0) {
        iter->TopPrev();
        continue;
      }
    }

    // Clip start: max(cur_start, L)
    std::string clipped_start;
    if (lower_bound != nullptr && !lower_bound->empty() &&
        ucmp->CompareWithoutTimestamp(cur_start, has_ts, *lower_bound,
                                      false) < 0) {
      if (has_ts) {
        AppendKeyWithMaxTimestamp(&clipped_start, *lower_bound, ts_sz);
      } else {
        clipped_start.assign(lower_bound->data(), lower_bound->size());
      }
    } else {
      clipped_start.assign(cur_start.data(), cur_start.size());
    }

    // Clip end: min(cur_end, U)
    std::string clipped_end;
    if (upper_bound != nullptr && !upper_bound->empty() &&
        ucmp->CompareWithoutTimestamp(cur_end, has_ts, *upper_bound,
                                      false) > 0) {
      if (has_ts) {
        AppendKeyWithMaxTimestamp(&clipped_end, *upper_bound, ts_sz);
      } else {
        clipped_end.assign(upper_bound->data(), upper_bound->size());
      }
    } else {
      clipped_end.assign(cur_end.data(), cur_end.size());
    }

    std::string ts_str;
    if (has_ts) {
      Slice ts = iter->timestamp();
      ts_str.assign(ts.data(), ts.size());
    }

    result.emplace_back(std::move(clipped_start), std::move(clipped_end),
                        iter->seq(), std::move(ts_str));
    iter->TopPrev();
  }
  return result;
}

// ==========================================================================
// AMTVLocalScanReferenceView
// M4-P1a: Test-only localized range tombstone reference view.
// Extracts only raw range tombstone entries intersecting [L, U) via a full
// safe linear traversal (start < U && end > L), and feeds them to the native
// FragmentedRangeTombstoneList to build a localized semantic view.
// ==========================================================================
class AMTVLocalScanReferenceView {
 public:
  AMTVLocalScanReferenceView(
      const AMTVSnapshot& snapshot,
      const Slice* lower_bound,
      const Slice* upper_bound,
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq,
      const Slice* ts_upper_bound = nullptr)
      : icmp_(icmp), read_seq_(read_seq) {
    std::vector<std::vector<OpenDeltaEntry>> runs_raw;
    for (const auto& run : snapshot.sealed_runs) {
      if (run) {
        runs_raw.push_back(run->raw_entries);
      }
    }
    std::vector<OpenDeltaEntry> delta_raw;
    if (snapshot.open_delta) {
      delta_raw = snapshot.open_delta->entries();
    }
    InitFromRawEntries(runs_raw, delta_raw, lower_bound, upper_bound,
                       ts_upper_bound);
  }

  AMTVLocalScanReferenceView(
      const std::vector<std::vector<OpenDeltaEntry>>& runs_raw,
      const std::vector<OpenDeltaEntry>& delta_raw,
      const Slice* lower_bound,
      const Slice* upper_bound,
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq,
      const Slice* ts_upper_bound = nullptr)
      : icmp_(icmp), read_seq_(read_seq) {
    InitFromRawEntries(runs_raw, delta_raw, lower_bound, upper_bound,
                       ts_upper_bound);
  }

  // Point query: returns maximum covering sequence number for user_key within read_seq,
  // or 0 if user_key is not covered.
  SequenceNumber MaxCoveringTombstoneSeqnum(const Slice& user_key) const {
    if (!iter_ || iter_->empty()) {
      return 0;
    }
    iter_->Seek(user_key);
    if (!iter_->Valid()) {
      return 0;
    }
    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key,
                                      has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(),
                                      has_ts) < 0) {
      return iter_->seq();
    }
    return 0;
  }

  // Point query: returns whether point key with sequence point_seq is deleted by a tombstone.
  bool ShouldDelete(const Slice& user_key, SequenceNumber point_seq) const {
    return MaxCoveringTombstoneSeqnum(user_key) > point_seq;
  }

  // Point query: returns whether point key with sequence point_seq is visible at read_seq.
  bool IsPointVisible(const Slice& user_key, SequenceNumber point_seq) const {
    if (point_seq > read_seq_) {
      return false;
    }
    return !ShouldDelete(user_key, point_seq);
  }

  // Returns timestamp of covering tombstone, or empty Slice if none.
  Slice CoveringTimestamp(const Slice& user_key) const {
    if (!iter_ || iter_->empty()) {
      return Slice();
    }
    iter_->Seek(user_key);
    if (!iter_->Valid()) {
      return Slice();
    }
    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key,
                                      has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(),
                                      has_ts) < 0) {
      return iter_->timestamp();
    }
    return Slice();
  }

  // Navigation interface
  void Seek(const Slice& target) { if (iter_) iter_->Seek(target); }
  void SeekForPrev(const Slice& target) { if (iter_) iter_->SeekForPrev(target); }
  void SeekToFirst() { if (iter_) iter_->SeekToFirst(); }
  void SeekToLast() { if (iter_) iter_->SeekToLast(); }
  void Next() { if (iter_) iter_->TopNext(); }
  void Prev() { if (iter_) iter_->TopPrev(); }
  bool Valid() const { return iter_ && iter_->Valid(); }
  ParsedInternalKey start_key() const { return iter_->parsed_start_key(); }
  ParsedInternalKey end_key() const { return iter_->parsed_end_key(); }
  SequenceNumber seq() const { return iter_->seq(); }
  Slice timestamp() const { return iter_->timestamp(); }
  bool empty() const { return !iter_ || iter_->empty(); }

  FragmentedRangeTombstoneIterator* iter() { return iter_.get(); }
  size_t num_selected_raw_entries() const { return num_selected_; }
  size_t num_total_raw_entries() const { return num_total_; }

  static bool IsIntersecting(const OpenDeltaEntry& entry,
                             const Slice* lower_bound,
                             const Slice* upper_bound,
                             const Comparator* ucmp) {
    const size_t ts_sz = ucmp->timestamp_size();
    const bool a_has_ts = (ts_sz > 0);

    // Filter 0: If both L and U are specified and L >= U, the interval [L, U) is empty.
    if (lower_bound != nullptr && !lower_bound->empty() &&
        upper_bound != nullptr && !upper_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound,
                                        false) >= 0) {
        return false;
      }
    }

    // Filter 1: start < U
    if (upper_bound != nullptr && !upper_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(entry.user_start_key(), a_has_ts,
                                         *upper_bound, false) >= 0) {
        return false;
      }
    }

    // Filter 2: end > L
    if (lower_bound != nullptr && !lower_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(entry.user_end_key(), a_has_ts,
                                         *lower_bound, false) <= 0) {
        return false;
      }
    }

    return true;
  }

 private:
  void InitFromRawEntries(
      const std::vector<std::vector<OpenDeltaEntry>>& runs_raw,
      const std::vector<OpenDeltaEntry>& delta_raw,
      const Slice* lower_bound,
      const Slice* upper_bound,
      const Slice* ts_upper_bound) {
    const auto* ucmp = icmp_.user_comparator();
    std::vector<OpenDeltaEntry> selected;

    num_total_ = 0;
    for (const auto& run : runs_raw) {
      for (const auto& entry : run) {
        num_total_++;
        if (IsIntersecting(entry, lower_bound, upper_bound, ucmp)) {
          selected.push_back(entry);
        }
      }
    }
    for (const auto& entry : delta_raw) {
      num_total_++;
      if (IsIntersecting(entry, lower_bound, upper_bound, ucmp)) {
        selected.push_back(entry);
      }
    }
    num_selected_ = selected.size();

    std::sort(selected.begin(), selected.end(),
              [this](const OpenDeltaEntry& a, const OpenDeltaEntry& b) {
                return icmp_.Compare(a.ikey.Encode(), b.ikey.Encode()) < 0;
              });

    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(selected.size());
    values.reserve(selected.size());
    for (const auto& e : selected) {
      keys.emplace_back(e.ikey.Encode().ToString());
      values.emplace_back(e.end_key);
    }

    auto iter = std::make_unique<VectorIterator>(std::move(keys),
                                                 std::move(values), &icmp_);
    fragmented_list_ =
        std::make_shared<FragmentedRangeTombstoneList>(std::move(iter), icmp_);
    iter_ = std::make_unique<FragmentedRangeTombstoneIterator>(
        fragmented_list_, icmp_, read_seq_, ts_upper_bound);
  }

  InternalKeyComparator icmp_;
  SequenceNumber read_seq_ = 0;
  size_t num_selected_ = 0;
  size_t num_total_ = 0;
  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list_;
  std::unique_ptr<FragmentedRangeTombstoneIterator> iter_;
};

// ==========================================================================
// AMTVCanonicalFullTruth
// Constructs full fragmented truth from ALL raw entries across all runs
// and open delta without pre-filtering by query bounds.
// ==========================================================================
class AMTVCanonicalFullTruth {
 public:
  AMTVCanonicalFullTruth(
      const std::vector<std::vector<OpenDeltaEntry>>& runs_raw,
      const std::vector<OpenDeltaEntry>& delta_raw,
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq,
      const Slice* ts_upper_bound = nullptr)
      : icmp_(icmp), read_seq_(read_seq) {
    std::vector<OpenDeltaEntry> all_entries;
    for (const auto& r : runs_raw) {
      for (const auto& e : r) {
        all_entries.push_back(e);
      }
    }
    for (const auto& e : delta_raw) {
      all_entries.push_back(e);
    }

    std::sort(all_entries.begin(), all_entries.end(),
              [&icmp](const OpenDeltaEntry& a, const OpenDeltaEntry& b) {
                return icmp.Compare(a.ikey.Encode(), b.ikey.Encode()) < 0;
              });

    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(all_entries.size());
    values.reserve(all_entries.size());
    for (const auto& e : all_entries) {
      keys.emplace_back(e.ikey.Encode().ToString());
      values.emplace_back(e.end_key);
    }

    auto iter = std::make_unique<VectorIterator>(std::move(keys),
                                                 std::move(values), &icmp);
    fragmented_list_ =
        std::make_shared<FragmentedRangeTombstoneList>(std::move(iter), icmp);
    iter_ = std::make_unique<FragmentedRangeTombstoneIterator>(
        fragmented_list_, icmp, read_seq, ts_upper_bound);
  }

  SequenceNumber MaxCoveringTombstoneSeqnum(const Slice& user_key) const {
    if (!iter_ || iter_->empty()) {
      return 0;
    }
    iter_->Seek(user_key);
    if (!iter_->Valid()) {
      return 0;
    }
    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key,
                                      has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(),
                                      has_ts) < 0) {
      return iter_->seq();
    }
    return 0;
  }

  bool ShouldDelete(const Slice& user_key, SequenceNumber point_seq) const {
    return MaxCoveringTombstoneSeqnum(user_key) > point_seq;
  }

  bool IsPointVisible(const Slice& user_key, SequenceNumber point_seq) const {
    if (point_seq > read_seq_) {
      return false;
    }
    return !ShouldDelete(user_key, point_seq);
  }

  Slice CoveringTimestamp(const Slice& user_key) const {
    if (!iter_ || iter_->empty()) {
      return Slice();
    }
    iter_->Seek(user_key);
    if (!iter_->Valid()) {
      return Slice();
    }
    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key,
                                      has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(),
                                      has_ts) < 0) {
      return iter_->timestamp();
    }
    return Slice();
  }

  FragmentedRangeTombstoneIterator* iter() { return iter_.get(); }

 private:
  InternalKeyComparator icmp_;
  SequenceNumber read_seq_ = 0;
  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list_;
  std::unique_ptr<FragmentedRangeTombstoneIterator> iter_;
};

// ==========================================================================
// AMTVIndependentPointwiseOracle
// Directly enumerates ALL raw range deletion entries across all runs and
// open delta, without invoking any Fragmenter or fragmented list.
// Serves as an independent mathematical reference Oracle.
// ==========================================================================
class AMTVIndependentPointwiseOracle {
 public:
  AMTVIndependentPointwiseOracle(
      const std::vector<std::vector<OpenDeltaEntry>>& runs_raw,
      const std::vector<OpenDeltaEntry>& delta_raw,
      const Comparator* ucmp,
      SequenceNumber read_seq,
      const Slice* ts_upper_bound = nullptr)
      : ucmp_(ucmp), read_seq_(read_seq), ts_upper_bound_(ts_upper_bound) {
    for (const auto& r : runs_raw) {
      for (const auto& e : r) {
        all_entries_.push_back(e);
      }
    }
    for (const auto& e : delta_raw) {
      all_entries_.push_back(e);
    }
  }

  SequenceNumber MaxCoveringTombstoneSeqnum(const Slice& user_key) const {
    SequenceNumber max_seq = 0;
    const size_t ts_sz = ucmp_->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    for (const auto& entry : all_entries_) {
      if (entry.sequence() > read_seq_) {
        continue;
      }
      if (has_ts && ts_upper_bound_ != nullptr) {
        Slice entry_ts = entry.timestamp(ts_sz);
        if (ucmp_->CompareTimestamp(entry_ts, *ts_upper_bound_) > 0) {
          continue;
        }
      }
      if (ucmp_->CompareWithoutTimestamp(entry.user_start_key(), has_ts,
                                         user_key, has_ts) <= 0 &&
          ucmp_->CompareWithoutTimestamp(user_key, has_ts,
                                         entry.user_end_key(), has_ts) < 0) {
        if (entry.sequence() > max_seq) {
          max_seq = entry.sequence();
        }
      }
    }
    return max_seq;
  }

  bool ShouldDelete(const Slice& user_key, SequenceNumber point_seq) const {
    return MaxCoveringTombstoneSeqnum(user_key) > point_seq;
  }

  bool IsPointVisible(const Slice& user_key, SequenceNumber point_seq) const {
    if (point_seq > read_seq_) {
      return false;
    }
    return !ShouldDelete(user_key, point_seq);
  }

  Slice CoveringTimestamp(const Slice& user_key) const {
    SequenceNumber max_seq = 0;
    Slice best_ts;
    const size_t ts_sz = ucmp_->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    for (const auto& entry : all_entries_) {
      if (entry.sequence() > read_seq_) {
        continue;
      }
      if (has_ts && ts_upper_bound_ != nullptr) {
        Slice entry_ts = entry.timestamp(ts_sz);
        if (ucmp_->CompareTimestamp(entry_ts, *ts_upper_bound_) > 0) {
          continue;
        }
      }
      if (ucmp_->CompareWithoutTimestamp(entry.user_start_key(), has_ts,
                                         user_key, has_ts) <= 0 &&
          ucmp_->CompareWithoutTimestamp(user_key, has_ts,
                                         entry.user_end_key(), has_ts) < 0) {
        if (has_ts) {
          Slice entry_ts = entry.timestamp(ts_sz);
          if (best_ts.empty() || ucmp_->CompareTimestamp(entry_ts, best_ts) > 0) {
            best_ts = entry_ts;
          }
        }
        if (entry.sequence() > max_seq) {
          max_seq = entry.sequence();
        }
      }
    }
    return best_ts;
  }

  private:
  const Comparator* ucmp_;
  SequenceNumber read_seq_;
  const Slice* ts_upper_bound_;
  std::vector<OpenDeltaEntry> all_entries_;
};

// ==========================================================================
// AMTVRunIntervalIndex
// M4-P1b-0: Test-only run-internal prefix-max-end interval index prototype.
// Does NOT modify production AMTVRun layout.
// Stores strictly size_t indices, zero string allocations or copies.
// ==========================================================================
class AMTVRunIntervalIndex {
 public:
  AMTVRunIntervalIndex(const std::vector<OpenDeltaEntry>& raw_entries,
                       const InternalKeyComparator& icmp)
      : raw_entries_(raw_entries), icmp_(icmp) {
    BuildIndex();
  }

  // Extracts candidate raw entries intersecting [L, U).
  // Test prototype convention: nullptr or empty Slice represents an unbounded boundary.
  // Records structural index boundaries [left, right) for structural auditing.
  void GetCandidates(const Slice* lower_bound, const Slice* upper_bound,
                     std::vector<OpenDeltaEntry>* out_candidates,
                     size_t* out_left = nullptr,
                     size_t* out_right = nullptr) const {
    if (out_left) *out_left = 0;
    if (out_right) *out_right = 0;
    if (raw_entries_.empty()) {
      return;
    }

    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    // Test prototype convention: if both L and U are non-null and non-empty, and L >= U,
    // the query interval [L, U) is empty.
    if (lower_bound != nullptr && !lower_bound->empty() &&
        upper_bound != nullptr && !upper_bound->empty()) {
      if (ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound,
                                        false) >= 0) {
        return;
      }
    }

    // Step 1: Binary search right bound on sorted_indices_
    // Find first entry with start >= U. If no U, right = N.
    size_t right = sorted_indices_.size();
    if (upper_bound != nullptr && !upper_bound->empty()) {
      auto it_right = std::lower_bound(
          sorted_indices_.begin(), sorted_indices_.end(), *upper_bound,
          [this, ucmp, has_ts](size_t idx, const Slice& u) {
            return ucmp->CompareWithoutTimestamp(
                       raw_entries_[idx].user_start_key(), has_ts, u, false) < 0;
          });
      right = static_cast<size_t>(std::distance(sorted_indices_.begin(), it_right));
    }

    // Step 2: Binary search left bound on prefix_max_end_index_
    // Find first position where prefix_max_end > L. If no L, left = 0.
    size_t left = 0;
    if (lower_bound != nullptr && !lower_bound->empty()) {
      auto it_left = std::lower_bound(
          prefix_max_end_index_.begin(), prefix_max_end_index_.end(), *lower_bound,
          [this, ucmp, has_ts](size_t entry_idx, const Slice& l) {
            return ucmp->CompareWithoutTimestamp(
                       raw_entries_[entry_idx].user_end_key(), has_ts, l, false) <= 0;
          });
      left = static_cast<size_t>(std::distance(prefix_max_end_index_.begin(), it_left));
    }

    if (left > right) {
      left = right;
    }

    if (out_left) *out_left = left;
    if (out_right) *out_right = right;

    // Step 3: Exact filtering on [left, right)
    for (size_t i = left; i < right; ++i) {
      size_t entry_idx = sorted_indices_[i];
      const auto& entry = raw_entries_[entry_idx];

      bool match = true;
      if (upper_bound != nullptr && !upper_bound->empty()) {
        if (ucmp->CompareWithoutTimestamp(entry.user_start_key(), has_ts,
                                           *upper_bound, false) >= 0) {
          match = false;
        }
      }
      if (lower_bound != nullptr && !lower_bound->empty()) {
        if (ucmp->CompareWithoutTimestamp(entry.user_end_key(), has_ts,
                                           *lower_bound, false) <= 0) {
          match = false;
        }
      }
      if (match) {
        out_candidates->push_back(entry);
      }
    }
  }

  size_t size() const { return raw_entries_.size(); }
  const std::vector<size_t>& sorted_indices() const { return sorted_indices_; }
  const std::vector<size_t>& prefix_max_end_index() const { return prefix_max_end_index_; }

 private:
  void BuildIndex() {
    const size_t n = raw_entries_.size();
    if (n == 0) return;

    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    // 1. Construct sorted directory of raw entry indices
    sorted_indices_.resize(n);
    for (size_t i = 0; i < n; ++i) {
      sorted_indices_[i] = i;
    }

    // Fixed sorting rule:
    // CompareWithoutTimestamp(start), tie-break with icmp_.Compare, then sequence descending
    std::sort(sorted_indices_.begin(), sorted_indices_.end(),
              [this, ucmp, has_ts](size_t i, size_t j) {
                const auto& a = raw_entries_[i];
                const auto& b = raw_entries_[j];
                int c = ucmp->CompareWithoutTimestamp(a.user_start_key(), has_ts,
                                                      b.user_start_key(), has_ts);
                if (c != 0) {
                  return c < 0;
                }
                int ic = icmp_.Compare(a.ikey.Encode(), b.ikey.Encode());
                if (ic != 0) {
                  return ic < 0;
                }
                if (a.sequence() != b.sequence()) {
                  return a.sequence() > b.sequence();
                }
                return ucmp->CompareWithoutTimestamp(a.user_end_key(), has_ts,
                                                     b.user_end_key(), has_ts) < 0;
              });

    // 2. Construct Prefix-Max-End array: points to raw entry index in [0, i] with max end key
    prefix_max_end_index_.resize(n);
    prefix_max_end_index_[0] = sorted_indices_[0];
    for (size_t i = 1; i < n; ++i) {
      size_t curr_entry = sorted_indices_[i];
      size_t prev_max_entry = prefix_max_end_index_[i - 1];
      int c = ucmp->CompareWithoutTimestamp(
          raw_entries_[curr_entry].user_end_key(), has_ts,
          raw_entries_[prev_max_entry].user_end_key(), has_ts);
      if (c > 0) {
        prefix_max_end_index_[i] = curr_entry;
      } else {
        prefix_max_end_index_[i] = prev_max_entry;
      }
    }
  }

  const std::vector<OpenDeltaEntry>& raw_entries_;
  const InternalKeyComparator& icmp_;
  std::vector<size_t> sorted_indices_;
  std::vector<size_t> prefix_max_end_index_;
};

// ==========================================================================
// CandidateEntryIdentity & MultiSet Verification
// Compares linear candidates and index candidates as normalized multisets
// with full entry identity (internal start key, end key, seq, timestamp).
// ==========================================================================
struct CandidateEntryIdentity {
  std::string internal_start_key;
  std::string end_key;
  SequenceNumber seq = 0;
  std::string timestamp;

  bool operator<(const CandidateEntryIdentity& o) const {
    if (internal_start_key != o.internal_start_key) return internal_start_key < o.internal_start_key;
    if (end_key != o.end_key) return end_key < o.end_key;
    if (seq != o.seq) return seq < o.seq;
    return timestamp < o.timestamp;
  }
  bool operator==(const CandidateEntryIdentity& o) const {
    return internal_start_key == o.internal_start_key &&
           end_key == o.end_key &&
           seq == o.seq &&
           timestamp == o.timestamp;
  }
};

inline void VerifyCandidateMultiSetsIdentical(
    const std::vector<OpenDeltaEntry>& linear_candidates,
    const std::vector<OpenDeltaEntry>& index_candidates,
    const Comparator* ucmp) {
  const size_t ts_sz = ucmp->timestamp_size();
  std::multiset<CandidateEntryIdentity> linear_set;
  for (const auto& e : linear_candidates) {
    std::string ts_str;
    if (ts_sz > 0) {
      Slice ts = e.timestamp(ts_sz);
      ts_str.assign(ts.data(), ts.size());
    }
    linear_set.insert({e.ikey.Encode().ToString(), e.end_key, e.sequence(), ts_str});
  }

  std::multiset<CandidateEntryIdentity> index_set;
  for (const auto& e : index_candidates) {
    std::string ts_str;
    if (ts_sz > 0) {
      Slice ts = e.timestamp(ts_sz);
      ts_str.assign(ts.data(), ts.size());
    }
    index_set.insert({e.ikey.Encode().ToString(), e.end_key, e.sequence(), ts_str});
  }

  ASSERT_EQ(linear_set.size(), index_set.size());
  ASSERT_EQ(linear_set, index_set);
}

inline void VerifyCandidateStreamEquivalence(
    const std::vector<OpenDeltaEntry>& linear_candidates,
    const std::vector<OpenDeltaEntry>& index_candidates,
    const Slice* L, const Slice* U,
    const InternalKeyComparator& icmp,
    SequenceNumber read_seq) {
  VerifyCandidateMultiSetsIdentical(linear_candidates, index_candidates, icmp.user_comparator());

  auto build_list = [&icmp](std::vector<OpenDeltaEntry> entries) {
    std::sort(entries.begin(), entries.end(),
              [&icmp](const OpenDeltaEntry& a, const OpenDeltaEntry& b) {
                return icmp.Compare(a.ikey.Encode(), b.ikey.Encode()) < 0;
              });
    std::vector<std::string> keys, vals;
    keys.reserve(entries.size());
    vals.reserve(entries.size());
    for (const auto& e : entries) {
      keys.push_back(e.ikey.Encode().ToString());
      vals.push_back(e.end_key);
    }
    auto viter = std::make_unique<VectorIterator>(std::move(keys), std::move(vals), &icmp);
    return std::make_shared<FragmentedRangeTombstoneList>(std::move(viter), icmp);
  };

  auto linear_list = build_list(linear_candidates);
  auto index_list = build_list(index_candidates);

  FragmentedRangeTombstoneIterator linear_iter(linear_list, icmp, read_seq);
  FragmentedRangeTombstoneIterator index_iter(index_list, icmp, read_seq);

  auto linear_fwd = GetClippedFragmentStreamForward(&linear_iter, L, U, icmp);
  auto index_fwd = GetClippedFragmentStreamForward(&index_iter, L, U, icmp);
  ASSERT_EQ(linear_fwd.size(), index_fwd.size());
  for (size_t i = 0; i < linear_fwd.size(); ++i) {
    ASSERT_EQ(linear_fwd[i], index_fwd[i]);
  }

  auto linear_bwd = GetClippedFragmentStreamBackward(&linear_iter, L, U, icmp);
  auto index_bwd = GetClippedFragmentStreamBackward(&index_iter, L, U, icmp);
  ASSERT_EQ(linear_bwd.size(), index_bwd.size());
  for (size_t i = 0; i < linear_bwd.size(); ++i) {
    ASSERT_EQ(linear_bwd[i], index_bwd[i]);
  }

  // Symmetric equivalence between forward and backward
  auto rev_fwd = linear_fwd;
  std::reverse(rev_fwd.begin(), rev_fwd.end());
  ASSERT_EQ(rev_fwd, linear_bwd);
}

inline std::vector<OpenDeltaEntry> GetLinearCandidates(
    const std::vector<std::vector<OpenDeltaEntry>>& runs_raw,
    const std::vector<OpenDeltaEntry>& delta_raw,
    const Slice* lower_bound, const Slice* upper_bound,
    const Comparator* ucmp) {
  std::vector<OpenDeltaEntry> result;
  for (const auto& run : runs_raw) {
    for (const auto& e : run) {
      if (AMTVLocalScanReferenceView::IsIntersecting(e, lower_bound, upper_bound, ucmp)) {
        result.push_back(e);
      }
    }
  }
  for (const auto& e : delta_raw) {
    if (AMTVLocalScanReferenceView::IsIntersecting(e, lower_bound, upper_bound, ucmp)) {
      result.push_back(e);
    }
  }
  return result;
}

inline std::vector<OpenDeltaEntry> GetIndexCandidates(
    const std::vector<std::vector<OpenDeltaEntry>>& runs_raw,
    const std::vector<OpenDeltaEntry>& delta_raw,
    const Slice* lower_bound, const Slice* upper_bound,
    const InternalKeyComparator& icmp,
    size_t* out_total_span = nullptr,
    std::vector<AMTVRunIntervalIndexAuditInfo>* out_run_audits = nullptr) {
  std::vector<OpenDeltaEntry> result;
  size_t tot_span = 0;
  for (const auto& run : runs_raw) {
    AMTVRun r(0, run, icmp);
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    r.CollectIntersectingRawEntryIndices(lower_bound, upper_bound, icmp,
                                         &indices, &audit);
    tot_span += audit.span;
    if (out_run_audits) {
      out_run_audits->push_back(audit);
    }
    for (size_t idx : indices) {
      result.push_back(run[idx]);
    }
  }
  const auto* ucmp = icmp.user_comparator();
  for (const auto& e : delta_raw) {
    if (AMTVLocalScanReferenceView::IsIntersecting(e, lower_bound, upper_bound,
                                                   ucmp)) {
      result.push_back(e);
    }
  }
  if (out_total_span) *out_total_span = tot_span;
  return result;
}

inline std::vector<OpenDeltaEntry> GetSidecarIndexCandidates(
    const std::vector<std::shared_ptr<const AMTVRun>>& sealed_runs,
    const std::vector<OpenDeltaEntry>& delta_raw,
    const Slice* lower_bound, const Slice* upper_bound,
    const InternalKeyComparator& icmp,
    size_t* out_total_span = nullptr,
    std::vector<AMTVRunIntervalIndexAuditInfo>* out_run_audits = nullptr) {
  std::vector<OpenDeltaEntry> result;
  size_t tot_span = 0;
  for (const auto& run : sealed_runs) {
    if (!run) continue;
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    run->CollectIntersectingRawEntryIndices(lower_bound, upper_bound, icmp,
                                            &indices, &audit);
    tot_span += audit.span;
    if (out_run_audits) {
      out_run_audits->push_back(audit);
    }
    for (size_t idx : indices) {
      result.push_back(run->raw_entries[idx]);
    }
  }
  const auto* ucmp = icmp.user_comparator();
  for (const auto& e : delta_raw) {
    if (AMTVLocalScanReferenceView::IsIntersecting(e, lower_bound, upper_bound,
                                                   ucmp)) {
      result.push_back(e);
    }
  }
  if (out_total_span) *out_total_span = tot_span;
  return result;
}

// ==========================================================================
// Test Fixture
// ==========================================================================
class AMTVLocalScanReferenceTest : public testing::Test {
 public:
  AMTVLocalScanReferenceTest()
      : bytewise_icmp_(BytewiseComparator()) {}

 protected:
  InternalKeyComparator bytewise_icmp_;

  // 3-Way Differential Comparison: Local View vs Full Truth vs Independent Oracle
  void Verify3WayPointwise(
      const AMTVLocalScanReferenceView& local_view,
      const AMTVCanonicalFullTruth& full_truth,
      const AMTVIndependentPointwiseOracle& independent_oracle,
      const Slice* L,
      const Slice* U,
      const std::vector<std::string>& probe_keys,
      const std::vector<SequenceNumber>& probe_seqs,
      const Comparator* ucmp) {
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    for (const auto& key : probe_keys) {
      if (L != nullptr && !L->empty() &&
          ucmp->CompareWithoutTimestamp(key, has_ts, *L, false) < 0) {
        continue;
      }
      if (U != nullptr && !U->empty() &&
          ucmp->CompareWithoutTimestamp(key, has_ts, *U, false) >= 0) {
        continue;
      }

      SequenceNumber full_seq = full_truth.MaxCoveringTombstoneSeqnum(key);
      SequenceNumber local_seq = local_view.MaxCoveringTombstoneSeqnum(key);
      SequenceNumber oracle_seq = independent_oracle.MaxCoveringTombstoneSeqnum(key);

      ASSERT_EQ(local_seq, full_seq)
          << "Mismatch (Local vs Full) for key: " << key;
      ASSERT_EQ(local_seq, oracle_seq)
          << "Mismatch (Local vs Oracle) for key: " << key;

      if (has_ts) {
        Slice full_ts = full_truth.CoveringTimestamp(key);
        Slice local_ts = local_view.CoveringTimestamp(key);
        Slice oracle_ts = independent_oracle.CoveringTimestamp(key);

        ASSERT_EQ(local_ts.ToString(), full_ts.ToString())
            << "Timestamp mismatch (Local vs Full) for key: " << key;
        ASSERT_EQ(local_ts.ToString(), oracle_ts.ToString())
            << "Timestamp mismatch (Local vs Oracle) for key: " << key;
      }

      for (SequenceNumber pseq : probe_seqs) {
        bool full_del = full_truth.ShouldDelete(key, pseq);
        bool local_del = local_view.ShouldDelete(key, pseq);
        bool oracle_del = independent_oracle.ShouldDelete(key, pseq);

        ASSERT_EQ(local_del, full_del)
            << "ShouldDelete (Local vs Full) mismatch for key: " << key
            << " at seq: " << pseq;
        ASSERT_EQ(local_del, oracle_del)
            << "ShouldDelete (Local vs Oracle) mismatch for key: " << key
            << " at seq: " << pseq;

        bool full_vis = full_truth.IsPointVisible(key, pseq);
        bool local_vis = local_view.IsPointVisible(key, pseq);
        bool oracle_vis = independent_oracle.IsPointVisible(key, pseq);

        ASSERT_EQ(local_vis, full_vis)
            << "Visibility (Local vs Full) mismatch for key: " << key
            << " at seq: " << pseq;
        ASSERT_EQ(local_vis, oracle_vis)
            << "Visibility (Local vs Oracle) mismatch for key: " << key
            << " at seq: " << pseq;
      }
    }
  }

  // Fragment Stream Equivalence: compares clipped streams forward and backward
  void VerifyClippedStreamEquivalence(
      AMTVLocalScanReferenceView& local_view,
      AMTVCanonicalFullTruth& full_truth,
      const Slice* L,
      const Slice* U,
      const InternalKeyComparator& icmp) {
    const auto* ucmp = icmp.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    auto local_stream_fwd =
        GetClippedFragmentStreamForward(local_view.iter(), L, U, icmp);
    auto full_stream_fwd =
        GetClippedFragmentStreamForward(full_truth.iter(), L, U, icmp);

    ASSERT_EQ(local_stream_fwd.size(), full_stream_fwd.size())
        << "Stream size mismatch (Forward)";
    for (size_t i = 0; i < local_stream_fwd.size(); ++i) {
      EXPECT_EQ(local_stream_fwd[i], full_stream_fwd[i])
          << "Fragment mismatch at index " << i
          << " Local: " << local_stream_fwd[i].ToString()
          << " Full: " << full_stream_fwd[i].ToString();

      // Verify bounds: start >= L, end <= U
      if (L != nullptr && !L->empty()) {
        EXPECT_GE(ucmp->CompareWithoutTimestamp(local_stream_fwd[i].start_key,
                                                has_ts, *L, false),
                  0);
      }
      if (U != nullptr && !U->empty()) {
        EXPECT_LE(ucmp->CompareWithoutTimestamp(local_stream_fwd[i].end_key,
                                                has_ts, *U, false),
                  0);
      }
    }

    auto local_stream_bwd =
        GetClippedFragmentStreamBackward(local_view.iter(), L, U, icmp);
    auto full_stream_bwd =
        GetClippedFragmentStreamBackward(full_truth.iter(), L, U, icmp);

    ASSERT_EQ(local_stream_bwd.size(), full_stream_bwd.size())
        << "Stream size mismatch (Backward)";
    for (size_t i = 0; i < local_stream_bwd.size(); ++i) {
      EXPECT_EQ(local_stream_bwd[i], full_stream_bwd[i])
          << "Backward fragment mismatch at index " << i;
    }

    // Forward and reversed backward streams must be bit-for-bit identical
    std::reverse(local_stream_bwd.begin(), local_stream_bwd.end());
    ASSERT_EQ(local_stream_fwd, local_stream_bwd)
        << "Symmetry mismatch between forward and reversed backward stream";
  }
};

// --------------------------------------------------------------------------
// 1. Left Boundary Spanning: start < L < end < U
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, LeftBoundarySpanning) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k60", 100)},
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k30";
  std::string U_str = "k80";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 200);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 200);
  AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 200);

  EXPECT_EQ(local_view.num_selected_raw_entries(), 1U);

  std::vector<std::string> probe_keys = {"k20", "k30", "k45", "k59",
                                         "k60", "k70", "k80", "k90"};
  std::vector<SequenceNumber> probe_seqs = {50, 100, 150};

  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                      probe_seqs, BytewiseComparator());
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 2. Right Boundary Spanning: L < start < U < end
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, RightBoundarySpanning) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k40", "k90", 100)},
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k20";
  std::string U_str = "k70";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 200);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 200);
  AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 200);

  EXPECT_EQ(local_view.num_selected_raw_entries(), 1U);

  std::vector<std::string> probe_keys = {"k15", "k20", "k30", "k40",
                                         "k55", "k69", "k70", "k85"};
  std::vector<SequenceNumber> probe_seqs = {50, 100, 150};

  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                      probe_seqs, BytewiseComparator());
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 3. Fully Spanning: start < L < U < end
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, FullySpanning) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k05", "k95", 100)},
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k20";
  std::string U_str = "k70";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 200);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 200);
  AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 200);

  EXPECT_EQ(local_view.num_selected_raw_entries(), 1U);

  std::vector<std::string> probe_keys = {"k10", "k20", "k35", "k50",
                                         "k69", "k70", "k90"};
  std::vector<SequenceNumber> probe_seqs = {50, 100, 150};

  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                      probe_seqs, BytewiseComparator());
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 4. Overlap, Nesting, and Adjacent Boundaries
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, OverlapNestingAdjacent) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {
          OpenDeltaEntry("k00", "k05", 10),
          OpenDeltaEntry("k10", "k90", 50),
          OpenDeltaEntry("k20", "k55", 80),
          OpenDeltaEntry("k30", "k60", 120),
          OpenDeltaEntry("k40", "k50", 200),
          OpenDeltaEntry("k70", "k80", 70),
          OpenDeltaEntry("k80", "k95", 90),
          OpenDeltaEntry("k96", "k99", 15),
      },
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k25";
  std::string U_str = "k85";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 250);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 250);
  AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 250);

  EXPECT_EQ(local_view.num_total_raw_entries(), 8U);
  EXPECT_EQ(local_view.num_selected_raw_entries(), 6U);

  std::vector<std::string> probe_keys = {
      "k02", "k15", "k25", "k28", "k30", "k35", "k40", "k45",
      "k50", "k52", "k55", "k58", "k65", "k70", "k75", "k80",
      "k82", "k84", "k85", "k90", "k97",
  };
  std::vector<SequenceNumber> probe_seqs = {40, 60, 75, 85, 100, 150, 220};

  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                      probe_seqs, BytewiseComparator());
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 5. Multi-Layer Interaction Across Sealed Runs and Open Delta
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, MultiRunAndOpenDelta) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k50", 30), OpenDeltaEntry("k60", "k90", 40)},
      {OpenDeltaEntry("k20", "k70", 60)},
      {OpenDeltaEntry("k35", "k80", 80)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k40", "k65", 110),
  };

  std::string L_str = "k25";
  std::string U_str = "k75";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 150);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 150);
  AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 150);

  std::vector<std::string> probe_keys = {
      "k20", "k25", "k30", "k35", "k40", "k45", "k50",
      "k55", "k60", "k65", "k70", "k74", "k75", "k80",
  };
  std::vector<SequenceNumber> probe_seqs = {20, 50, 70, 90, 100, 120};

  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                      probe_seqs, BytewiseComparator());
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 6. DeleteRange Followed by Put Resurrection and Point Visibility
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, DeleteRangeFollowedByPutResurrection) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k20", "k70", 50)},
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k25";
  std::string U_str = "k75";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 100);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 100);
  AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 100);

  EXPECT_TRUE(local_view.ShouldDelete("k30", 30));
  EXPECT_FALSE(local_view.IsPointVisible("k30", 30));

  EXPECT_FALSE(local_view.ShouldDelete("k30", 70));
  EXPECT_TRUE(local_view.IsPointVisible("k30", 70));

  EXPECT_TRUE(local_view.ShouldDelete("k50", 40));
  EXPECT_FALSE(local_view.IsPointVisible("k50", 40));

  EXPECT_FALSE(local_view.ShouldDelete("k50", 50));
  EXPECT_TRUE(local_view.IsPointVisible("k50", 50));

  EXPECT_FALSE(local_view.ShouldDelete("k50", 80));
  EXPECT_TRUE(local_view.IsPointVisible("k50", 80));

  EXPECT_FALSE(local_view.ShouldDelete("k80", 40));
  EXPECT_TRUE(local_view.IsPointVisible("k80", 40));

  std::vector<std::string> probe_keys = {"k25", "k30", "k50", "k70", "k74"};
  std::vector<SequenceNumber> probe_seqs = {30, 40, 50, 70, 80};

  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                      probe_seqs, BytewiseComparator());
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 7. Multiple Snapshots Coexistence
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, MultipleSnapshots) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k90", 40)},
      {OpenDeltaEntry("k20", "k80", 70)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30", "k70", 100),
  };

  std::string L_str = "k15";
  std::string U_str = "k85";
  Slice L(L_str);
  Slice U(U_str);

  // Snapshot 1 at seq 50 (only sees run 0 @ 40)
  {
    AMTVLocalScanReferenceView view_s1(runs, delta, &L, &U, bytewise_icmp_, 50);
    AMTVCanonicalFullTruth truth_s1(runs, delta, bytewise_icmp_, 50);
    AMTVIndependentPointwiseOracle oracle_s1(runs, delta, BytewiseComparator(), 50);
    EXPECT_EQ(view_s1.MaxCoveringTombstoneSeqnum("k50"), 40U);
    EXPECT_EQ(truth_s1.MaxCoveringTombstoneSeqnum("k50"), 40U);
    EXPECT_EQ(oracle_s1.MaxCoveringTombstoneSeqnum("k50"), 40U);
  }

  // Snapshot 2 at seq 80 (sees run 0 @ 40 and run 1 @ 70)
  {
    AMTVLocalScanReferenceView view_s2(runs, delta, &L, &U, bytewise_icmp_, 80);
    AMTVCanonicalFullTruth truth_s2(runs, delta, bytewise_icmp_, 80);
    AMTVIndependentPointwiseOracle oracle_s2(runs, delta, BytewiseComparator(), 80);
    EXPECT_EQ(view_s2.MaxCoveringTombstoneSeqnum("k50"), 70U);
    EXPECT_EQ(truth_s2.MaxCoveringTombstoneSeqnum("k50"), 70U);
    EXPECT_EQ(oracle_s2.MaxCoveringTombstoneSeqnum("k50"), 70U);
  }

  // Snapshot 3 at seq 120 (sees all three: delta @ 100)
  {
    AMTVLocalScanReferenceView view_s3(runs, delta, &L, &U, bytewise_icmp_, 120);
    AMTVCanonicalFullTruth truth_s3(runs, delta, bytewise_icmp_, 120);
    AMTVIndependentPointwiseOracle oracle_s3(runs, delta, BytewiseComparator(), 120);
    EXPECT_EQ(view_s3.MaxCoveringTombstoneSeqnum("k50"), 100U);
    EXPECT_EQ(truth_s3.MaxCoveringTombstoneSeqnum("k50"), 100U);
    EXPECT_EQ(oracle_s3.MaxCoveringTombstoneSeqnum("k50"), 100U);
  }
}

// Helper for UDT comparator without external testutil dependency
static const Comparator* GetBytewiseComparatorWithU64Ts() {
  ConfigOptions config_options;
  const Comparator* user_comparator = nullptr;
  Status s = Comparator::CreateFromString(
      config_options, "leveldb.BytewiseComparator.u64ts", &user_comparator);
  s.PermitUncheckedError();
  return user_comparator;
}

// --------------------------------------------------------------------------
// 8. User Defined Timestamp Enabled
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, UserDefinedTimestamp) {
  const Comparator* ucmp = GetBytewiseComparatorWithU64Ts();
  ASSERT_NE(ucmp, nullptr);
  InternalKeyComparator ts_icmp(ucmp);

  const size_t ts_sz = sizeof(uint64_t);
  std::string dummy_ts(ts_sz, '\0');

  std::string ts100, ts200;
  PutFixed64(&ts100, 100);
  PutFixed64(&ts200, 200);

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10" + ts100, "k60" + ts100, 50)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30" + ts200, "k80" + ts200, 80),
  };

  std::string L_str = "k20";
  std::string U_str = "k70";
  Slice L(L_str);
  Slice U(U_str);

  // Case 8a: No ts_upper_bound (both visible)
  {
    AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, ts_icmp, 100, nullptr);
    AMTVCanonicalFullTruth full_truth(runs, delta, ts_icmp, 100, nullptr);
    AMTVIndependentPointwiseOracle oracle(runs, delta, ucmp, 100, nullptr);

    std::vector<std::string> probe_keys = {
        "k15" + dummy_ts, "k25" + dummy_ts, "k35" + dummy_ts,
        "k50" + dummy_ts, "k65" + dummy_ts, "k75" + dummy_ts,
    };
    std::vector<SequenceNumber> probe_seqs = {40, 60, 90};

    Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                        probe_seqs, ucmp);
    VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, ts_icmp);
  }

  // Case 8b: ts_upper_bound = 150 (Delta with ts 200 filtered out)
  {
    std::string ts150;
    PutFixed64(&ts150, 150);
    Slice ts_ub(ts150);

    AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, ts_icmp, 100, &ts_ub);
    AMTVCanonicalFullTruth full_truth(runs, delta, ts_icmp, 100, &ts_ub);
    AMTVIndependentPointwiseOracle oracle(runs, delta, ucmp, 100, &ts_ub);

    std::vector<std::string> probe_keys = {
        "k15" + dummy_ts, "k25" + dummy_ts, "k35" + dummy_ts,
        "k50" + dummy_ts, "k65" + dummy_ts, "k75" + dummy_ts,
    };
    std::vector<SequenceNumber> probe_seqs = {40, 60, 90};

    Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                        probe_seqs, ucmp);
    VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, ts_icmp);

    EXPECT_EQ(local_view.MaxCoveringTombstoneSeqnum("k50" + dummy_ts), 50U);
    EXPECT_EQ(full_truth.MaxCoveringTombstoneSeqnum("k50" + dummy_ts), 50U);
    EXPECT_EQ(oracle.MaxCoveringTombstoneSeqnum("k50" + dummy_ts), 50U);
  }
}

// --------------------------------------------------------------------------
// 9. Empty, Short, and Full Windows
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, EmptyShortFullWindows) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k20", "k50", 50), OpenDeltaEntry("k60", "k90", 60)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30", "k70", 70),
  };

  // Case 9a: Empty Window L == U
  {
    std::string bound = "k40";
    Slice L(bound);
    Slice U(bound);
    AMTVLocalScanReferenceView view(runs, delta, &L, &U, bytewise_icmp_, 100);
    EXPECT_EQ(view.num_selected_raw_entries(), 0U);
    EXPECT_TRUE(view.empty());

    auto stream_fwd = GetClippedFragmentStreamForward(view.iter(), &L, &U, bytewise_icmp_);
    EXPECT_TRUE(stream_fwd.empty());
  }

  // Case 9b: Inverted Window L > U
  {
    std::string l_str = "k50";
    std::string u_str = "k30";
    Slice L(l_str);
    Slice U(u_str);
    AMTVLocalScanReferenceView view(runs, delta, &L, &U, bytewise_icmp_, 100);
    EXPECT_EQ(view.num_selected_raw_entries(), 0U);
    EXPECT_TRUE(view.empty());

    auto stream_fwd = GetClippedFragmentStreamForward(view.iter(), &L, &U, bytewise_icmp_);
    EXPECT_TRUE(stream_fwd.empty());
  }

  // Case 9c: Full window (L = nullptr, U = nullptr)
  {
    AMTVLocalScanReferenceView view(runs, delta, nullptr, nullptr, bytewise_icmp_, 100);
    AMTVCanonicalFullTruth truth(runs, delta, bytewise_icmp_, 100);
    AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 100);

    EXPECT_EQ(view.num_selected_raw_entries(), 3U);
    EXPECT_EQ(view.num_total_raw_entries(), 3U);

    std::vector<std::string> probe_keys = {"k10", "k25", "k45", "k65", "k85", "k95"};
    std::vector<SequenceNumber> probe_seqs = {40, 65, 80};

    Verify3WayPointwise(view, truth, oracle, nullptr, nullptr, probe_keys,
                        probe_seqs, BytewiseComparator());
    VerifyClippedStreamEquivalence(view, truth, nullptr, nullptr, bytewise_icmp_);
  }
}

// --------------------------------------------------------------------------
// 10. Bidirectional Seek and Scan
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, BidirectionalSeekAndScan) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {
          OpenDeltaEntry("k10", "k30", 50),
          OpenDeltaEntry("k25", "k45", 80),
          OpenDeltaEntry("k50", "k70", 60),
          OpenDeltaEntry("k65", "k90", 90),
      },
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k20";
  std::string U_str = "k80";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 100);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 100);

  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 11. Clipped Fragment Stream Equivalence (Forward & Backward)
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, ClippedFragmentStreamEquivalence) {
  // Complex terrain with overlaps, nesting, and spanning
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {
          OpenDeltaEntry("k05", "k25", 20),
          OpenDeltaEntry("k15", "k45", 60),
          OpenDeltaEntry("k30", "k70", 40),
          OpenDeltaEntry("k50", "k85", 80),
          OpenDeltaEntry("k60", "k95", 50),
      },
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k40", "k65", 90),
  };

  std::string L_str = "k20";
  std::string U_str = "k80";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 100);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 100);

  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 12. Three-Way Independent Oracle Equivalence
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, ThreeWayIndependentOracleEquivalence) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k40", 30), OpenDeltaEntry("k50", "k80", 50)},
      {OpenDeltaEntry("k20", "k60", 70)},
      {OpenDeltaEntry("k35", "k75", 90)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30", "k65", 110),
  };

  std::string L_str = "k15";
  std::string U_str = "k70";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_, 120);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 120);
  AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), 120);

  std::vector<std::string> probe_keys;
  for (int i = 10; i <= 75; i += 2) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    probe_keys.emplace_back(buf);
  }
  std::vector<SequenceNumber> probe_seqs = {20, 40, 60, 80, 100, 115, 130};

  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                      probe_seqs, BytewiseComparator());
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// 13. Real AMTVSnapshot Input and Background Merge Stability
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, RealAMTVSnapshotInputAndMergeStability) {
  Env::Default()->SetBackgroundThreads(1, Env::Priority::LOW);

  // Construct real AMTVState
  auto amtv_state = std::make_shared<AMTVState>(
      0 /* gen */, 4 /* delta_limit */, 2 /* merge_soft_limit */,
      8 /* hard_limit */, &bytewise_icmp_, Env::Default());

  // Add tombstones: epoch 1
  amtv_state->AddTombstone("k10", "k50", 20, bytewise_icmp_);
  amtv_state->AddTombstone("k30", "k70", 40, bytewise_icmp_);
  amtv_state->AddTombstone("k60", "k90", 60, bytewise_icmp_);
  amtv_state->AddTombstone("k20", "k40", 80, bytewise_icmp_);  // Seals into Run 0
  amtv_state->AddTombstone("k25", "k65", 100, bytewise_icmp_);
  amtv_state->AddTombstone("k45", "k85", 120, bytewise_icmp_);

  // Capture Old Snapshot
  std::shared_ptr<const AMTVSnapshot> old_snapshot = amtv_state->GetSnapshot();
  ASSERT_NE(old_snapshot, nullptr);
  EXPECT_EQ(old_snapshot->sealed_run_count(), 1U);
  EXPECT_EQ(old_snapshot->open_delta->size(), 2U);

  // Add more tombstones: epoch 2
  amtv_state->AddTombstone("k15", "k55", 140, bytewise_icmp_);
  amtv_state->AddTombstone("k35", "k75", 160, bytewise_icmp_);
  // Wait for background merge of Run 0 and Run 1 to complete
  amtv_state->WaitForMergeStable();
  EXPECT_GE(amtv_state->merge_completed(), 1U);

  // Add new tombstone into post-merge Open Delta
  amtv_state->AddTombstone("k50", "k80", 180, bytewise_icmp_);

  // Capture New Snapshot
  std::shared_ptr<const AMTVSnapshot> new_snapshot = amtv_state->GetSnapshot();
  ASSERT_NE(new_snapshot, nullptr);
  EXPECT_EQ(new_snapshot->sealed_run_count(), 1U);
  EXPECT_EQ(new_snapshot->open_delta->size(), 1U);

  // Set up localized query window [k20, k75)
  std::string L_str = "k20";
  std::string U_str = "k75";
  Slice L(L_str);
  Slice U(U_str);

  // Old Snapshot localized view
  AMTVLocalScanReferenceView old_view(*old_snapshot, &L, &U, bytewise_icmp_, 200);
  // New Snapshot localized view
  AMTVLocalScanReferenceView new_view(*new_snapshot, &L, &U, bytewise_icmp_, 200);

  // Verification 1: Old snapshot does not see post-merge tombstones (seq 140, 160, 180)
  // In old snapshot, at k50, max covering seq is 120 (from [k45, k85)@120 in delta)
  EXPECT_EQ(old_view.MaxCoveringTombstoneSeqnum("k50"), 120U);

  // In new snapshot, at k50, max covering seq is 180 (from [k50, k80)@180 in new delta)
  EXPECT_EQ(new_view.MaxCoveringTombstoneSeqnum("k50"), 180U);

  // Verification 2: Check isolation - old view remains completely stable and unaffected
  EXPECT_EQ(old_view.MaxCoveringTombstoneSeqnum("k50"), 120U);
  EXPECT_FALSE(old_view.ShouldDelete("k50", 150));
  EXPECT_TRUE(new_view.ShouldDelete("k50", 150));

  // Verification 3: Stream equivalence on old view
  AMTVCanonicalFullTruth old_truth(
      {old_snapshot->sealed_runs[0]->raw_entries},
      old_snapshot->open_delta->entries(), bytewise_icmp_, 200);
  VerifyClippedStreamEquivalence(old_view, old_truth, &L, &U, bytewise_icmp_);

  // Candidate multi-set & stream equivalence on old snapshot
  auto linear_cands_old = GetLinearCandidates(
      {old_snapshot->sealed_runs[0]->raw_entries},
      old_snapshot->open_delta->entries(), &L, &U, BytewiseComparator());
  auto index_cands_old = GetIndexCandidates(
      {old_snapshot->sealed_runs[0]->raw_entries},
      old_snapshot->open_delta->entries(), &L, &U, bytewise_icmp_);
  VerifyCandidateMultiSetsIdentical(linear_cands_old, index_cands_old, BytewiseComparator());
  VerifyCandidateStreamEquivalence(linear_cands_old, index_cands_old, &L, &U, bytewise_icmp_, 200);

  // Independent Oracle 3-way check on old snapshot
  AMTVIndependentPointwiseOracle old_oracle(
      {old_snapshot->sealed_runs[0]->raw_entries},
      old_snapshot->open_delta->entries(), BytewiseComparator(), 200);
  std::vector<std::string> probe_keys = {"k10", "k20", "k25", "k30", "k35", "k45", "k50", "k60", "k70", "k75"};
  std::vector<SequenceNumber> probe_seqs = {10, 50, 90, 110, 150, 200};
  Verify3WayPointwise(old_view, old_truth, old_oracle, &L, &U, probe_keys, probe_seqs, BytewiseComparator());

  // Verification 4: Stream equivalence on new view
  std::vector<std::vector<OpenDeltaEntry>> new_runs_raw;
  for (const auto& r : new_snapshot->sealed_runs) {
    if (r) new_runs_raw.push_back(r->raw_entries);
  }
  AMTVCanonicalFullTruth new_truth(
      new_runs_raw, new_snapshot->open_delta->entries(), bytewise_icmp_, 200);
  VerifyClippedStreamEquivalence(new_view, new_truth, &L, &U, bytewise_icmp_);

  // Candidate multi-set & stream equivalence on new snapshot
  auto linear_cands_new = GetLinearCandidates(
      new_runs_raw, new_snapshot->open_delta->entries(), &L, &U, BytewiseComparator());
  auto index_cands_new = GetIndexCandidates(
      new_runs_raw, new_snapshot->open_delta->entries(), &L, &U, bytewise_icmp_);
  VerifyCandidateMultiSetsIdentical(linear_cands_new, index_cands_new, BytewiseComparator());
  VerifyCandidateStreamEquivalence(linear_cands_new, index_cands_new, &L, &U, bytewise_icmp_, 200);

  // Independent Oracle 3-way check on new snapshot
  AMTVIndependentPointwiseOracle new_oracle(
      new_runs_raw, new_snapshot->open_delta->entries(), BytewiseComparator(), 200);
  Verify3WayPointwise(new_view, new_truth, new_oracle, &L, &U, probe_keys, probe_seqs, BytewiseComparator());

  amtv_state->CancelAndDrain();
  Env::Default()->SetBackgroundThreads(0, Env::Priority::LOW);
}

// --------------------------------------------------------------------------
// 14. Randomized Differential Testing (1,000 Trials)
// Uses GoogleTest SCOPED_TRACE for rich failure reproduction context.
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, RandomizedDifferential1000Trials) {
  const uint64_t kMasterSeed = 1337;
  std::mt19937_64 rng(kMasterSeed);

  for (int trial = 0; trial < 1000; ++trial) {
    int num_runs = std::uniform_int_distribution<int>(0, 4)(rng);
    std::vector<std::vector<OpenDeltaEntry>> runs;
    SequenceNumber next_seq = 10;
    SequenceNumber min_seq = 10;

    // Window [L, U)
    int l_idx = std::uniform_int_distribution<int>(20, 60)(rng);
    int u_idx = l_idx + std::uniform_int_distribution<int>(10, 50)(rng);
    char l_buf[16], u_buf[16];
    snprintf(l_buf, sizeof(l_buf), "key%04d", l_idx);
    snprintf(u_buf, sizeof(u_buf), "key%04d", u_idx);
    Slice L(l_buf);
    Slice U(u_buf);

    for (int r = 0; r < num_runs; ++r) {
      int count = std::uniform_int_distribution<int>(1, 8)(rng);
      std::vector<OpenDeltaEntry> run_entries;
      for (int i = 0; i < count; ++i) {
        int topo_type = std::uniform_int_distribution<int>(0, 4)(rng);
        int k1 = 0, k2 = 0;
        if (topo_type == 0) {
          k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(l_idx + 1, u_idx - 1)(rng);
        } else if (topo_type == 1) {
          k1 = std::uniform_int_distribution<int>(l_idx + 1, u_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(u_idx + 1, u_idx + 30)(rng);
        } else if (topo_type == 2) {
          k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(u_idx + 1, u_idx + 30)(rng);
        } else {
          k1 = std::uniform_int_distribution<int>(0, 120)(rng);
          int len = std::uniform_int_distribution<int>(5, 40)(rng);
          k2 = k1 + len;
        }

        char buf1[16], buf2[16];
        snprintf(buf1, sizeof(buf1), "key%04d", k1);
        snprintf(buf2, sizeof(buf2), "key%04d", k2);
        next_seq += std::uniform_int_distribution<SequenceNumber>(1, 10)(rng);
        run_entries.emplace_back(buf1, buf2, next_seq);
      }
      runs.push_back(run_entries);
    }

    int delta_count = std::uniform_int_distribution<int>(0, 6)(rng);
    std::vector<OpenDeltaEntry> delta_entries;
    for (int i = 0; i < delta_count; ++i) {
      int topo_type = std::uniform_int_distribution<int>(0, 3)(rng);
      int k1 = 0, k2 = 0;
      if (topo_type == 0) {
        k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
        k2 = std::uniform_int_distribution<int>(l_idx + 1, u_idx)(rng);
      } else if (topo_type == 1) {
        k1 = std::uniform_int_distribution<int>(l_idx, u_idx - 1)(rng);
        k2 = std::uniform_int_distribution<int>(u_idx + 1, u_idx + 30)(rng);
      } else {
        k1 = std::uniform_int_distribution<int>(0, 120)(rng);
        int len = std::uniform_int_distribution<int>(5, 40)(rng);
        k2 = k1 + len;
      }
      char buf1[16], buf2[16];
      snprintf(buf1, sizeof(buf1), "key%04d", k1);
      snprintf(buf2, sizeof(buf2), "key%04d", k2);
      next_seq += std::uniform_int_distribution<SequenceNumber>(1, 10)(rng);
      delta_entries.emplace_back(buf1, buf2, next_seq);
    }

    SequenceNumber max_seq = next_seq;

    int seq_category = std::uniform_int_distribution<int>(0, 2)(rng);
    SequenceNumber read_seq = 0;
    const char* category_str = "early";
    if (seq_category == 0) {
      read_seq = (min_seq > 1) ? (min_seq / 2) : 1;
      category_str = "early";
    } else if (seq_category == 1) {
      read_seq = std::uniform_int_distribution<SequenceNumber>(min_seq, max_seq)(rng);
      category_str = "mid";
    } else {
      read_seq = max_seq + 10;
      category_str = "late";
    }

    SCOPED_TRACE(testing::Message()
                 << "master_seed=" << kMasterSeed << ", trial=" << trial
                 << ", category=" << category_str << ", read_seq=" << read_seq
                 << ", L=" << l_buf << ", U=" << u_buf);

    AMTVLocalScanReferenceView local_view(runs, delta_entries, &L, &U,
                                         bytewise_icmp_, read_seq);
    AMTVCanonicalFullTruth full_truth(runs, delta_entries, bytewise_icmp_,
                                      read_seq);
    AMTVIndependentPointwiseOracle oracle(runs, delta_entries,
                                          BytewiseComparator(), read_seq);

    // Probe points within [L, U)
    std::vector<std::string> probe_keys;
    for (int p = l_idx; p < u_idx; p += 3) {
      char p_buf[16];
      snprintf(p_buf, sizeof(p_buf), "key%04d", p);
      probe_keys.emplace_back(p_buf);
    }
    std::vector<SequenceNumber> probe_seqs = {1, read_seq / 2, read_seq,
                                             max_seq + 5};

    Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                        probe_seqs, BytewiseComparator());
    VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U,
                                   bytewise_icmp_);
  }
}

// --------------------------------------------------------------------------
// 15. P1b: Earliest Start Super-Long Tombstone with Many Disjoint Short Tombstones
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_Counterexample_SuperLongTombstoneWithDisjointShort) {
  // Super-long tombstone [k00, k99)@100
  // Followed by numerous disjoint short tombstones outside [k40, k60)
  std::vector<OpenDeltaEntry> raw_entries = {
      OpenDeltaEntry("k00", "k99", 100),
      OpenDeltaEntry("k05", "k15", 10),
      OpenDeltaEntry("k10", "k20", 20),
      OpenDeltaEntry("k20", "k30", 30),
      OpenDeltaEntry("k25", "k35", 40),
      OpenDeltaEntry("k70", "k80", 50),
      OpenDeltaEntry("k80", "k90", 60),
  };

  Slice L("k40");
  Slice U("k60");

  size_t left = 0, right = 0;
  AMTVRunIntervalIndex index(raw_entries, bytewise_icmp_);
  std::vector<OpenDeltaEntry> index_cands;
  index.GetCandidates(&L, &U, &index_cands, &left, &right);

  auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());

  // Structural assertion: left must be 0 because index 0 has max_end == k99 > L(k40)
  EXPECT_EQ(left, 0U);
  EXPECT_EQ(right, 5U); // first start >= k60 is k70 at index 5

  VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
  ASSERT_EQ(index_cands.size(), 1U);
  EXPECT_EQ(index_cands[0].user_start_key(), "k00");
  EXPECT_EQ(index_cands[0].user_end_key(), "k99");

  VerifyCandidateStreamEquivalence(linear_cands, index_cands, &L, &U, bytewise_icmp_, 200);
}

// --------------------------------------------------------------------------
// 16. P1b: Same Start Key with Different End Key and Sequence Numbers
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_Counterexample_SameStartDifferentEndAndSeq) {
  std::vector<OpenDeltaEntry> raw_entries = {
      OpenDeltaEntry("k30", "k40", 80),
      OpenDeltaEntry("k30", "k50", 50),
      OpenDeltaEntry("k30", "k60", 30),
      OpenDeltaEntry("k30", "k70", 40),
      OpenDeltaEntry("k60", "k90", 20),
  };

  Slice L("k45");
  Slice U("k55");

  auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
  size_t left = 0, right = 0;
  AMTVRunIntervalIndex index(raw_entries, bytewise_icmp_);
  std::vector<OpenDeltaEntry> index_cands;
  index.GetCandidates(&L, &U, &index_cands, &left, &right);

  VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
  // k30..k50, k30..k60, k30..k70 intersect [k45, k55)
  EXPECT_EQ(index_cands.size(), 3U);
  VerifyCandidateStreamEquivalence(linear_cands, index_cands, &L, &U, bytewise_icmp_, 200);
}

// --------------------------------------------------------------------------
// 17. P1b: Multiple Prefix Maximums Alternating
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_Counterexample_MultiplePrefixMaximumsAlternating) {
  std::vector<OpenDeltaEntry> raw_entries = {
      OpenDeltaEntry("k10", "k50", 10),
      OpenDeltaEntry("k20", "k30", 20),
      OpenDeltaEntry("k30", "k80", 30),
      OpenDeltaEntry("k40", "k60", 40),
      OpenDeltaEntry("k50", "k95", 50),
      OpenDeltaEntry("k60", "k70", 60),
  };

  AMTVRunIntervalIndex index(raw_entries, bytewise_icmp_);

  std::vector<std::pair<std::string, std::string>> windows = {
      {"k05", "k15"},
      {"k25", "k35"},
      {"k35", "k45"},
      {"k55", "k75"},
      {"k85", "k99"},
      {"k00", "k99"},
  };

  for (const auto& w : windows) {
    Slice L(w.first);
    Slice U(w.second);
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    VerifyCandidateStreamEquivalence(linear_cands, index_cands, &L, &U, bytewise_icmp_, 200);
  }
}

// --------------------------------------------------------------------------
// 18. P1b: Boundary Exact Alignment (L/U equal to start/end)
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_Counterexample_BoundaryExactAlignment) {
  std::vector<OpenDeltaEntry> raw_entries = {
      OpenDeltaEntry("k20", "k50", 100),
  };
  AMTVRunIntervalIndex index(raw_entries, bytewise_icmp_);

  // Case 1: L == start ("k20"), U == end ("k50") -> matches
  {
    Slice L("k20"), U("k50");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 1U);
  }

  // Case 2: L == end ("k50"), U == "k70" -> end <= L, does NOT match (half-open)
  {
    Slice L("k50"), U("k70");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 0U);
  }

  // Case 3: L == "k10", U == start ("k20") -> start >= U, does NOT match (half-open)
  {
    Slice L("k10"), U("k20");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 0U);
  }
}

// --------------------------------------------------------------------------
// 19. P1b: Empty and Unbounded Windows
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_Counterexample_EmptyAndUnboundedWindows) {
  std::vector<OpenDeltaEntry> raw_entries = {
      OpenDeltaEntry("k10", "k40", 10),
      OpenDeltaEntry("k30", "k70", 20),
      OpenDeltaEntry("k60", "k90", 30),
  };
  AMTVRunIntervalIndex index(raw_entries, bytewise_icmp_);

  // Empty window L >= U
  {
    Slice L("k60"), U("k20");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 0U);
  }
  {
    Slice L("k40"), U("k40");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 0U);
  }

  // Unbounded: L = nullptr, U = nullptr
  {
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, nullptr, nullptr, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(nullptr, nullptr, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 3U);
  }

  // Unbounded left: L = nullptr, U = "k50"
  {
    Slice U("k50");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, nullptr, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(nullptr, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 2U);
  }

  // Unbounded right: L = "k50", U = nullptr
  {
    Slice L("k50");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, nullptr, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, nullptr, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 2U);
  }

  // Empty Slice convention
  {
    Slice L("");
    Slice U("");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 3U);
  }
}

// --------------------------------------------------------------------------
// 20a. P1b: User Defined Timestamp with Interval Index (Without Ts Upper Bound)
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_Counterexample_UserDefinedTimestamp_WithoutTsUpperBound) {
  const Comparator* ucmp = GetBytewiseComparatorWithU64Ts();
  ASSERT_NE(ucmp, nullptr);
  InternalKeyComparator ts_icmp(ucmp);

  std::string ts100, ts200, ts300;
  PutFixed64(&ts100, 100);
  PutFixed64(&ts200, 200);
  PutFixed64(&ts300, 300);

  std::vector<OpenDeltaEntry> raw_entries = {
      OpenDeltaEntry("k10" + ts100, "k50" + ts100, 50),
      OpenDeltaEntry("k20" + ts200, "k70" + ts200, 40),
      OpenDeltaEntry("k40" + ts300, "k90" + ts300, 30),
      OpenDeltaEntry("k75" + ts100, "k95" + ts100, 20),
  };

  Slice L("k25");
  Slice U("k60");

  auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, ucmp);
  size_t left = 0, right = 0;
  AMTVRunIntervalIndex index(raw_entries, ts_icmp);
  std::vector<OpenDeltaEntry> index_cands;
  index.GetCandidates(&L, &U, &index_cands, &left, &right);

  VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, ucmp);
  EXPECT_EQ(index_cands.size(), 3U); // k10..k50, k20..k70, k40..k90

  VerifyCandidateStreamEquivalence(linear_cands, index_cands, &L, &U, ts_icmp, 100);
}

// --------------------------------------------------------------------------
// 20b. P1b: User Defined Timestamp with Interval Index (With Ts Upper Bound)
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_Counterexample_UserDefinedTimestamp_WithTsUpperBound) {
  const Comparator* ucmp = GetBytewiseComparatorWithU64Ts();
  ASSERT_NE(ucmp, nullptr);
  InternalKeyComparator ts_icmp(ucmp);

  std::string ts100, ts200, ts300, ts400;
  PutFixed64(&ts100, 100);
  PutFixed64(&ts200, 200);
  PutFixed64(&ts300, 300);
  PutFixed64(&ts400, 400);

  // In RocksDB with UDT, sequence numbers and timestamps are monotonically non-decreasing
  // across writes (higher seq corresponds to higher or equal timestamp).
  std::vector<OpenDeltaEntry> raw_entries = {
      OpenDeltaEntry("k10" + ts100, "k50" + ts100, 20),
      OpenDeltaEntry("k20" + ts200, "k70" + ts200, 40),
      OpenDeltaEntry("k40" + ts300, "k90" + ts300, 60),
      OpenDeltaEntry("k75" + ts400, "k95" + ts400, 80),
  };

  Slice L("k25");
  Slice U("k60");
  Slice ts_upper(ts200); // Filter out tombstones with ts > 200 (e.g. ts300, ts400)

  // Spatial candidate extraction selects all 3 intersecting entries
  auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, ucmp);
  size_t left = 0, right = 0;
  AMTVRunIntervalIndex index(raw_entries, ts_icmp);
  std::vector<OpenDeltaEntry> index_cands;
  index.GetCandidates(&L, &U, &index_cands, &left, &right);

  VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, ucmp);
  EXPECT_EQ(index_cands.size(), 3U);

  // Local view and full truth with ts_upper_bound filter
  AMTVLocalScanReferenceView local_view({raw_entries}, {}, &L, &U, ts_icmp, 100, &ts_upper);
  AMTVCanonicalFullTruth full_truth({raw_entries}, {}, ts_icmp, 100, &ts_upper);
  AMTVIndependentPointwiseOracle oracle({raw_entries}, {}, ucmp, 100, &ts_upper);

  const size_t ts_sz = sizeof(uint64_t);
  std::string dummy_ts(ts_sz, '\0');
  std::vector<std::string> probe_keys;
  for (int p = 25; p <= 58; p += 3) {
    char p_buf[16];
    snprintf(p_buf, sizeof(p_buf), "k%02d", p);
    probe_keys.emplace_back(std::string(p_buf) + dummy_ts);
  }
  std::vector<SequenceNumber> probe_seqs = {10, 25, 35, 45, 60};
  Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys, probe_seqs, ucmp);

  // Stream equivalence with ts_upper_bound
  VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U, ts_icmp);
}

// --------------------------------------------------------------------------
// 21. P1b: Randomized Differential Testing (10,000 Trials)
// Simultaneously verifies:
// 1. Linear candidate multiset == Index candidate multiset
// 2. Linear clipped stream == Index clipped stream (forward & backward symmetric)
// 3. 3-Way independent Pointwise Oracle
// Collects and outputs structural audit statistics (no latency/throughput claims).
// Correctly tracks \sum span_i across all runs in each trial.
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b_RandomizedDifferential10000Trials) {
  const uint64_t kMasterSeed = 20260909;
  std::mt19937_64 rng(kMasterSeed);

  uint64_t total_raw_count_sum = 0;
  uint64_t total_cands_count_sum = 0;
  uint64_t total_window_span_sum = 0;

  for (int trial = 0; trial < 10000; ++trial) {
    int num_runs = std::uniform_int_distribution<int>(1, 4)(rng);
    std::vector<std::vector<OpenDeltaEntry>> runs;
    SequenceNumber next_seq = 10;
    SequenceNumber min_seq = 10;

    int l_idx = std::uniform_int_distribution<int>(20, 60)(rng);
    int u_idx = l_idx + std::uniform_int_distribution<int>(10, 50)(rng);
    char l_buf[16], u_buf[16];
    snprintf(l_buf, sizeof(l_buf), "key%04d", l_idx);
    snprintf(u_buf, sizeof(u_buf), "key%04d", u_idx);
    Slice L(l_buf);
    Slice U(u_buf);

    size_t trial_raw_count = 0;
    for (int r = 0; r < num_runs; ++r) {
      int count = std::uniform_int_distribution<int>(1, 8)(rng);
      std::vector<OpenDeltaEntry> run_entries;
      for (int i = 0; i < count; ++i) {
        int topo_type = std::uniform_int_distribution<int>(0, 4)(rng);
        int k1 = 0, k2 = 0;
        if (topo_type == 0) {
          k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(l_idx + 1, u_idx - 1)(rng);
        } else if (topo_type == 1) {
          k1 = std::uniform_int_distribution<int>(l_idx + 1, u_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(u_idx + 1, u_idx + 30)(rng);
        } else if (topo_type == 2) {
          k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(u_idx + 1, u_idx + 30)(rng);
        } else {
          k1 = std::uniform_int_distribution<int>(0, 120)(rng);
          int len = std::uniform_int_distribution<int>(5, 40)(rng);
          k2 = k1 + len;
        }

        char buf1[16], buf2[16];
        snprintf(buf1, sizeof(buf1), "key%04d", k1);
        snprintf(buf2, sizeof(buf2), "key%04d", k2);
        next_seq += std::uniform_int_distribution<SequenceNumber>(1, 10)(rng);
        run_entries.emplace_back(buf1, buf2, next_seq);
      }
      trial_raw_count += run_entries.size();
      runs.push_back(run_entries);
    }

    int delta_count = std::uniform_int_distribution<int>(0, 5)(rng);
    std::vector<OpenDeltaEntry> delta_entries;
    for (int i = 0; i < delta_count; ++i) {
      int k1 = std::uniform_int_distribution<int>(0, 120)(rng);
      int len = std::uniform_int_distribution<int>(5, 40)(rng);
      int k2 = k1 + len;
      char buf1[16], buf2[16];
      snprintf(buf1, sizeof(buf1), "key%04d", k1);
      snprintf(buf2, sizeof(buf2), "key%04d", k2);
      next_seq += std::uniform_int_distribution<SequenceNumber>(1, 10)(rng);
      delta_entries.emplace_back(buf1, buf2, next_seq);
    }
    trial_raw_count += delta_entries.size();

    SequenceNumber max_seq = next_seq;
    int seq_category = std::uniform_int_distribution<int>(0, 2)(rng);
    SequenceNumber read_seq = 0;
    const char* category_str = "early";
    if (seq_category == 0) {
      read_seq = (min_seq > 1) ? (min_seq / 2) : 1;
      category_str = "early";
    } else if (seq_category == 1) {
      read_seq = std::uniform_int_distribution<SequenceNumber>(min_seq, max_seq)(rng);
      category_str = "mid";
    } else {
      read_seq = max_seq + 10;
      category_str = "late";
    }

    size_t trial_span = 0;
    std::vector<AMTVRunIntervalIndexAuditInfo> run_audits;
    auto linear_cands = GetLinearCandidates(runs, delta_entries, &L, &U, BytewiseComparator());
    auto index_cands = GetIndexCandidates(runs, delta_entries, &L, &U, bytewise_icmp_, &trial_span, &run_audits);

    total_raw_count_sum += trial_raw_count;
    total_cands_count_sum += index_cands.size();
    total_window_span_sum += trial_span;

    SCOPED_TRACE(testing::Message()
                 << "master_seed=" << kMasterSeed << ", trial=" << trial
                 << ", category=" << category_str << ", read_seq=" << read_seq
                 << ", L=" << l_buf << ", U=" << u_buf
                 << ", total_span=" << trial_span
                 << ", cands=" << index_cands.size() << ", total_raw=" << trial_raw_count);

    // 1. Assert multiset candidate identity
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());

    // 2. Assert clipped fragment stream equivalence (forward, backward, symmetric)
    VerifyCandidateStreamEquivalence(linear_cands, index_cands, &L, &U, bytewise_icmp_, read_seq);

    // 3. Assert 3-way independent Pointwise Oracle equivalence
    AMTVLocalScanReferenceView local_view(runs, delta_entries, &L, &U, bytewise_icmp_, read_seq);
    AMTVCanonicalFullTruth full_truth(runs, delta_entries, bytewise_icmp_, read_seq);
    AMTVIndependentPointwiseOracle oracle(runs, delta_entries, BytewiseComparator(), read_seq);

    std::vector<std::string> probe_keys;
    for (int p = l_idx; p < u_idx; p += 4) {
      char p_buf[16];
      snprintf(p_buf, sizeof(p_buf), "key%04d", p);
      probe_keys.emplace_back(p_buf);
    }
    std::vector<SequenceNumber> probe_seqs = {1, read_seq / 2, read_seq, max_seq + 5};
    Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys, probe_seqs, BytewiseComparator());
  }

  // Structural Audit Summary Output (no performance claims)
  std::cout << "[STRUCTURAL AUDIT P1b] 10,000 trials completed successfully.\n"
            << "  Total raw entries evaluated:     " << total_raw_count_sum << "\n"
            << "  Total candidates selected:       " << total_cands_count_sum << "\n"
            << "  Average raw entries/trial:       " << (double)total_raw_count_sum / 10000.0 << "\n"
            << "  Average candidates/trial:        " << (double)total_cands_count_sum / 10000.0 << "\n"
            << "  Average total span (Σspan_i)/trial: " << (double)total_window_span_sum / 10000.0 << "\n";
}

// --------------------------------------------------------------------------
// 22. M4-P1b-1: Real AMTVState Sidecar Index Integration & Merge Lifecycle
// Verifies:
// 1. Immutable sidecar interval index built inside AMTVRun
// 2. Old snapshot and new snapshot after background merge
// 3. Sidecar candidates == linear candidates (multiset identical)
// 4. Sidecar clipped stream == linear clipped stream (bidirectional symmetric)
// 5. 3-Way independent Pointwise Oracle equivalence
// 6. Memory address independence: old run index pointers vs merged run index pointers
// 7. Structural audit: N, index bytes, left, right, span, candidate count
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b1_RealAMTVSnapshot_SidecarIndexAndMergeLifecycle) {
  Env::Default()->SetBackgroundThreads(1, Env::Priority::LOW);

  // Construct real AMTVState with small delta_limit=4, merge_soft_limit=2
  auto amtv_state = std::make_shared<AMTVState>(
      0 /* gen */, 4 /* delta_limit */, 2 /* merge_soft_limit */,
      8 /* hard_limit */, &bytewise_icmp_, Env::Default());

  // Epoch 1: Add tombstones
  // Seal Run 0 (4 entries, including super-long k00..k99)
  amtv_state->AddTombstone("k10", "k50", 20, bytewise_icmp_);
  amtv_state->AddTombstone("k30", "k70", 40, bytewise_icmp_);
  amtv_state->AddTombstone("k60", "k90", 60, bytewise_icmp_);
  amtv_state->AddTombstone("k00", "k99", 80, bytewise_icmp_); // Run 0 sealed

  // Seal Run 1 (4 entries, including multiple entries starting at k30)
  amtv_state->AddTombstone("k30", "k40", 100, bytewise_icmp_);
  amtv_state->AddTombstone("k30", "k60", 110, bytewise_icmp_);
  amtv_state->AddTombstone("k45", "k85", 120, bytewise_icmp_);
  amtv_state->AddTombstone("k20", "k40", 130, bytewise_icmp_); // Run 1 sealed

  // Capture Old Snapshot: contains Run 0 and Run 1
  std::shared_ptr<const AMTVSnapshot> old_snapshot = amtv_state->GetSnapshot();
  ASSERT_NE(old_snapshot, nullptr);
  ASSERT_EQ(old_snapshot->sealed_run_count(), 2U);

  const auto& old_run0 = old_snapshot->sealed_runs[0];
  const auto& old_run1 = old_snapshot->sealed_runs[1];
  ASSERT_NE(old_run0, nullptr);
  ASSERT_NE(old_run1, nullptr);

  // Record raw memory addresses of old run indices
  const void* old_run0_index_ptr = old_run0->sorted_indices.data();
  const void* old_run1_index_ptr = old_run1->sorted_indices.data();
  EXPECT_NE(old_run0_index_ptr, nullptr);
  EXPECT_NE(old_run1_index_ptr, nullptr);

  // Wait for background binary merge of Run 0 and Run 1 to complete into Run 2
  amtv_state->WaitForMergeStable();
  EXPECT_GE(amtv_state->merge_completed(), 1U);

  // Epoch 2: Add tombstones into post-merge Open Delta
  amtv_state->AddTombstone("k25", "k65", 150, bytewise_icmp_);
  amtv_state->AddTombstone("k50", "k80", 180, bytewise_icmp_);

  // Capture New Snapshot: contains Merged Run 2 and 2 entries in Open Delta
  std::shared_ptr<const AMTVSnapshot> new_snapshot = amtv_state->GetSnapshot();
  ASSERT_NE(new_snapshot, nullptr);
  ASSERT_EQ(new_snapshot->sealed_run_count(), 1U);
  ASSERT_EQ(new_snapshot->open_delta->size(), 2U);

  const auto& new_run2 = new_snapshot->sealed_runs[0];
  ASSERT_NE(new_run2, nullptr);
  const void* new_run2_index_ptr = new_run2->sorted_indices.data();

  // Verification 1: Memory Address Independence (Zero mutation of old snapshot runs)
  EXPECT_NE(old_run0_index_ptr, new_run2_index_ptr);
  EXPECT_NE(old_run1_index_ptr, new_run2_index_ptr);
  EXPECT_EQ(old_snapshot->sealed_runs[0]->sorted_indices.data(), old_run0_index_ptr);
  EXPECT_EQ(old_snapshot->sealed_runs[1]->sorted_indices.data(), old_run1_index_ptr);

  // Test across multiple distinct query windows:
  // 1. Standard window: [k20, k75)
  // 2. Disjoint window: [k05, k15)
  // 3. Dense overlap window: [k35, k65)
  // 4. Empty window: [k60, k20)
  // 5. Unbounded window: [nullptr, nullptr)
  struct WindowTestCase {
    const char* l_str;
    const char* u_str;
  };
  std::vector<WindowTestCase> test_windows = {
      {"k20", "k75"},
      {"k05", "k15"},
      {"k35", "k65"},
      {"k60", "k20"},
      {nullptr, nullptr},
  };

  std::vector<std::string> probe_keys = {"k00", "k10", "k20", "k25", "k30", "k35",
                                         "k45", "k50", "k60", "k70", "k80", "k95"};
  std::vector<SequenceNumber> probe_seqs = {10, 50, 90, 115, 145, 175, 200};

  std::cout << "\n[P1b-1 STRUCTURAL & MEMORY AUDIT]\n";

  for (const auto& w : test_windows) {
    Slice L_slice, U_slice;
    const Slice* pL = nullptr;
    const Slice* pU = nullptr;
    if (w.l_str) {
      L_slice = Slice(w.l_str);
      pL = &L_slice;
    }
    if (w.u_str) {
      U_slice = Slice(w.u_str);
      pU = &U_slice;
    }

    std::string w_name = "[" + std::string(w.l_str ? w.l_str : "null") + ", " +
                         std::string(w.u_str ? w.u_str : "null") + ")";

    // --- OLD SNAPSHOT VERIFICATION ---
    size_t old_total_span = 0;
    std::vector<AMTVRunIntervalIndexAuditInfo> old_audits;
    auto sidecar_cands_old = GetSidecarIndexCandidates(
        old_snapshot->sealed_runs, old_snapshot->open_delta->entries(),
        pL, pU, bytewise_icmp_, &old_total_span, &old_audits);
    std::vector<std::vector<OpenDeltaEntry>> old_runs_raw = {
        old_run0->raw_entries, old_run1->raw_entries};
    auto linear_cands_old = GetLinearCandidates(
        old_runs_raw, old_snapshot->open_delta->entries(), pL, pU, BytewiseComparator());

    VerifyCandidateMultiSetsIdentical(linear_cands_old, sidecar_cands_old, BytewiseComparator());
    VerifyCandidateStreamEquivalence(linear_cands_old, sidecar_cands_old, pL, pU, bytewise_icmp_, 200);

    AMTVLocalScanReferenceView old_view(*old_snapshot, pL, pU, bytewise_icmp_, 200);
    AMTVCanonicalFullTruth old_truth(old_runs_raw, old_snapshot->open_delta->entries(), bytewise_icmp_, 200);
    AMTVIndependentPointwiseOracle old_oracle(old_runs_raw, old_snapshot->open_delta->entries(), BytewiseComparator(), 200);
    Verify3WayPointwise(old_view, old_truth, old_oracle, pL, pU, probe_keys, probe_seqs, BytewiseComparator());

    // --- NEW SNAPSHOT VERIFICATION ---
    size_t new_total_span = 0;
    std::vector<AMTVRunIntervalIndexAuditInfo> new_audits;
    auto sidecar_cands_new = GetSidecarIndexCandidates(
        new_snapshot->sealed_runs, new_snapshot->open_delta->entries(),
        pL, pU, bytewise_icmp_, &new_total_span, &new_audits);
    std::vector<std::vector<OpenDeltaEntry>> new_runs_raw = {new_run2->raw_entries};
    auto linear_cands_new = GetLinearCandidates(
        new_runs_raw, new_snapshot->open_delta->entries(), pL, pU, BytewiseComparator());

    VerifyCandidateMultiSetsIdentical(linear_cands_new, sidecar_cands_new, BytewiseComparator());
    VerifyCandidateStreamEquivalence(linear_cands_new, sidecar_cands_new, pL, pU, bytewise_icmp_, 200);

    AMTVLocalScanReferenceView new_view(*new_snapshot, pL, pU, bytewise_icmp_, 200);
    AMTVCanonicalFullTruth new_truth(new_runs_raw, new_snapshot->open_delta->entries(), bytewise_icmp_, 200);
    AMTVIndependentPointwiseOracle new_oracle(new_runs_raw, new_snapshot->open_delta->entries(), BytewiseComparator(), 200);
    Verify3WayPointwise(new_view, new_truth, new_oracle, pL, pU, probe_keys, probe_seqs, BytewiseComparator());

    // Log structural audit for window
    std::cout << "Window: " << w_name << "\n"
              << "  Old Snapshot (Runs=2, Level=0): Σspan=" << old_total_span
              << ", Cands=" << sidecar_cands_old.size()
              << " [Run0: N=" << old_audits[0].raw_entries_count
              << ", bytes=" << old_audits[0].index_bytes
              << ", [" << old_audits[0].left << "," << old_audits[0].right << ")"
              << ", span=" << old_audits[0].span
              << "; Run1: N=" << old_audits[1].raw_entries_count
              << ", bytes=" << old_audits[1].index_bytes
              << ", [" << old_audits[1].left << "," << old_audits[1].right << ")"
              << ", span=" << old_audits[1].span << "]\n"
              << "  New Snapshot (Run=1, Level=1, chunks=2): Σspan=" << new_total_span
              << ", Cands=" << sidecar_cands_new.size()
              << " [Run2: N=" << new_audits[0].raw_entries_count
              << ", bytes=" << new_audits[0].index_bytes
              << ", [" << new_audits[0].left << "," << new_audits[0].right << ")"
              << ", span=" << new_audits[0].span << "]\n";
  }

  amtv_state->CancelAndDrain();
  Env::Default()->SetBackgroundThreads(0, Env::Priority::LOW);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
