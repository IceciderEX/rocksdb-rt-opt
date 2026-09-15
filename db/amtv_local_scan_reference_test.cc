//  Copyright (c) 2026-present. All rights reserved.
//  Test-only implementation and test suite for AMTV M4-P1b-0: Run-Internal
//  Prefix-Max-End Interval Index Prototype with Boundary Clipping and 3-Way Oracle.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "db/amtv.h"
#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "table/merging_iterator.h"
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
    if (lower_bound != nullptr && upper_bound != nullptr) {
      if (ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound,
                                        false) >= 0) {
        return false;
      }
    }

    // Filter 1: start < U
    if (upper_bound != nullptr) {
      if (ucmp->CompareWithoutTimestamp(entry.user_start_key(), a_has_ts,
                                         *upper_bound, false) >= 0) {
        return false;
      }
    }

    // Filter 2: end > L
    if (lower_bound != nullptr) {
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
      const Comparator* ucmp = BytewiseComparator(),
      SequenceNumber read_seq = kMaxSequenceNumber,
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
// ScopedEnvBackgroundThreads
// RAII guard for ensuring that background threads set on an Env are restored
// on test exit or assertion failure.
// ==========================================================================
class ScopedEnvBackgroundThreads {
 public:
  ScopedEnvBackgroundThreads(Env* env, int num_threads, Env::Priority pri)
      : env_(env ? env : Env::Default()),
        pri_(pri),
        orig_threads_(env_->GetBackgroundThreads(pri)) {
    env_->SetBackgroundThreads(num_threads, pri_);
  }
  ~ScopedEnvBackgroundThreads() {
    env_->SetBackgroundThreads(orig_threads_, pri_);
  }
  ScopedEnvBackgroundThreads(const ScopedEnvBackgroundThreads&) = delete;
  ScopedEnvBackgroundThreads& operator=(const ScopedEnvBackgroundThreads&) = delete;

 private:
  Env* env_;
  Env::Priority pri_;
  int orig_threads_;
};

// ==========================================================================
// AMTVRunIntervalIndex
// Test helper wrapping AMTVRunSidecarIndex for unit testing and backward compatibility.
// ==========================================================================
class AMTVRunIntervalIndex {
 public:
  AMTVRunIntervalIndex(const std::vector<OpenDeltaEntry>& raw_entries,
                       const InternalKeyComparator& icmp)
      : raw_entries_(raw_entries), icmp_(icmp), index_(raw_entries, icmp) {}

  void GetCandidates(const Slice* lower_bound, const Slice* upper_bound,
                     std::vector<OpenDeltaEntry>* out_candidates,
                     size_t* out_left = nullptr,
                     size_t* out_right = nullptr) const {
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    index_.CollectIntersectingIndices(raw_entries_, lower_bound, upper_bound,
                                     icmp_, &indices, &audit);
    if (out_left) *out_left = audit.left;
    if (out_right) *out_right = audit.right;
    if (out_candidates) {
      for (size_t idx : indices) {
        out_candidates->push_back(raw_entries_[idx]);
      }
    }
  }

  size_t size() const { return raw_entries_.size(); }
  const std::vector<size_t>& sorted_indices() const { return index_.sorted_indices(); }
  const std::vector<size_t>& prefix_max_end_index() const { return index_.prefix_max_end_index(); }
  const AMTVRunSidecarIndex& sidecar_index() const { return index_; }

 private:
  const std::vector<OpenDeltaEntry>& raw_entries_;
  const InternalKeyComparator& icmp_;
  AMTVRunSidecarIndex index_;
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
// AMTV M4-P2a: Test-Only Bounded Local Range Tombstone View Infrastructure
// STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.
// ==========================================================================

// 1. WindowSpec
// Represents an explicit bounding window [lower_bound, upper_bound).
// Strictly distinguishes std::nullopt (unbounded) from "" (valid 0-byte user key).
struct WindowSpec {
  std::optional<std::string> lower_bound;
  std::optional<std::string> upper_bound;

  WindowSpec() = default;
  WindowSpec(std::optional<std::string> l, std::optional<std::string> u)
      : lower_bound(std::move(l)), upper_bound(std::move(u)) {}

  bool IsBounded() const {
    return lower_bound.has_value() && upper_bound.has_value();
  }

  bool IsValidBounded(const Comparator* ucmp) const {
    if (!IsBounded()) return false;
    assert(ucmp != nullptr);
    return ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound, false) < 0;
  }
};

// 2. OwnedRawRangeTombstone
// Single canonical truth representation for a raw tombstone.
// Holds original user keys with original timestamps (if UDT enabled) and sequence.
// STRICT RULE: No redundant InternalKeys, duplicate sequences, or disconnected timestamps.
struct OwnedRawRangeTombstone {
  std::string start_key;
  std::string end_key;
  SequenceNumber seq = 0;

  OwnedRawRangeTombstone() = default;
  OwnedRawRangeTombstone(std::string s, std::string e, SequenceNumber sq)
      : start_key(std::move(s)), end_key(std::move(e)), seq(sq) {}

  bool operator==(const OwnedRawRangeTombstone& o) const {
    return start_key == o.start_key && end_key == o.end_key && seq == o.seq;
  }

  std::pair<InternalKey, Slice> Serialize() const {
    RangeTombstone rt(start_key, end_key, seq);
    return rt.Serialize();
  }
};

// Forward declarations
class LocalRangeDelView;
class WindowGuard;

// 3. LocalRangeDelViewState
// Phase 1 (Staging) -> Phase 2 (Publication as shared_ptr<const State>).
struct LocalRangeDelViewState {
  WindowSpec window;
  bool is_bounded = false;
  bool is_empty_window = false;
  bool fallback_to_full = false;
  size_t candidate_count = 0;

  // Staging collections (frozen upon publication)
  std::vector<OwnedRawRangeTombstone> raw_candidates;
  std::vector<std::string> serialized_keys;
  std::vector<std::string> serialized_values;

  // Native TruncatedRangeDelIterator bounds
  std::string smallest_key_buf;
  std::string largest_key_buf;
  std::unique_ptr<InternalKey> smallest_ikey;
  std::unique_ptr<InternalKey> largest_ikey;

  // Native Fragmented Range Tombstone List (shared ownership)
  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list;
};

// 4. WindowGuard
// STRICTLY TEST-ONLY VALIDATOR: Used in M4-P2a for boundary assert validation.
// In M4-P2b (MergingIterator compatibility), MergingIterator receives pure native
// TruncatedRangeDelIterator without this wrapper, so out-of-bounds seeks follow native
// clamping / invalidation rules rather than returning InvalidArgument.
class WindowGuard : public InternalIterator {
 public:
  WindowGuard(std::unique_ptr<TruncatedRangeDelIterator> trunc_iter,
              std::shared_ptr<const LocalRangeDelViewState> state,
              const Comparator* ucmp)
      : trunc_iter_(std::move(trunc_iter)),
        state_(std::move(state)),
        ucmp_(ucmp),
        ts_sz_(ucmp ? ucmp->timestamp_size() : 0) {}

  bool Valid() const override {
    return status_.ok() && is_valid_;
  }

  Status status() const override {
    return status_;
  }

  void SeekToFirst() override {
    status_ = Status::OK();
    if (state_->is_empty_window) {
      is_valid_ = false;
      return;
    }
    trunc_iter_->SeekToFirst();
    UpdateValidity();
  }

  void SeekToLast() override {
    status_ = Status::OK();
    if (state_->is_empty_window) {
      is_valid_ = false;
      return;
    }
    trunc_iter_->SeekToLast();
    if (!trunc_iter_->Valid() && state_->is_bounded && !state_->fallback_to_full) {
      trunc_iter_->Prev();
    }
    UpdateValidity();
  }

  void Seek(const Slice& target) override {
    status_ = Status::OK();
    if (state_->is_empty_window) {
      is_valid_ = false;
      return;
    }
    const bool target_has_ts = (ts_sz_ > 0 && target.size() >= ts_sz_);
    if (state_->is_bounded && !state_->fallback_to_full) {
      const auto& L = state_->window.lower_bound;
      const auto& U = state_->window.upper_bound;
      if (L.has_value() &&
          ucmp_->CompareWithoutTimestamp(target, target_has_ts, *L, false) < 0) {
        status_ = Status::InvalidArgument("Seek target < lower_bound");
        is_valid_ = false;
        return;
      }
      if (U.has_value() &&
          ucmp_->CompareWithoutTimestamp(target, target_has_ts, *U, false) >= 0) {
        status_ = Status::InvalidArgument("Seek target >= upper_bound");
        is_valid_ = false;
        return;
      }
    }
    std::string target_buf;
    Slice effective_target = target;
    if (ts_sz_ > 0 && !target_has_ts) {
      AppendKeyWithMaxTimestamp(&target_buf, target, ts_sz_);
      effective_target = target_buf;
    }
    trunc_iter_->Seek(effective_target);
    UpdateValidity();
  }

  void SeekForPrev(const Slice& target) override {
    status_ = Status::OK();
    if (state_->is_empty_window) {
      is_valid_ = false;
      return;
    }
    const bool target_has_ts = (ts_sz_ > 0 && target.size() >= ts_sz_);
    if (state_->is_bounded && !state_->fallback_to_full) {
      const auto& L = state_->window.lower_bound;
      const auto& U = state_->window.upper_bound;
      if (L.has_value() &&
          ucmp_->CompareWithoutTimestamp(target, target_has_ts, *L, false) < 0) {
        status_ = Status::InvalidArgument("SeekForPrev target < lower_bound");
        is_valid_ = false;
        return;
      }
      if (U.has_value() &&
          ucmp_->CompareWithoutTimestamp(target, target_has_ts, *U, false) > 0) {
        status_ = Status::InvalidArgument("SeekForPrev target > upper_bound");
        is_valid_ = false;
        return;
      }
    }
    std::string target_buf;
    Slice effective_target = target;
    if (ts_sz_ > 0 && !target_has_ts) {
      AppendKeyWithMaxTimestamp(&target_buf, target, ts_sz_);
      effective_target = target_buf;
    }
    trunc_iter_->SeekForPrev(effective_target);
    if (!trunc_iter_->Valid() && state_->is_bounded && !state_->fallback_to_full) {
      trunc_iter_->Prev();
    }
    UpdateValidity();
  }

  void Next() override {
    assert(Valid());
    trunc_iter_->Next();
    UpdateValidity();
  }

  void Prev() override {
    assert(Valid());
    trunc_iter_->Prev();
    UpdateValidity();
  }

  Slice key() const override {
    assert(Valid());
    current_key_buf_ = InternalKey(
        trunc_iter_->start_key().user_key,
        trunc_iter_->seq(),
        kTypeRangeDeletion).Encode().ToString();
    return current_key_buf_;
  }

  Slice value() const override {
    assert(Valid());
    return trunc_iter_->end_key().user_key;
  }

  Slice start_key() const {
    assert(Valid());
    return trunc_iter_->start_key().user_key;
  }

  Slice end_key() const {
    assert(Valid());
    return trunc_iter_->end_key().user_key;
  }

  SequenceNumber seq() const {
    assert(Valid());
    return trunc_iter_->seq();
  }

  Slice timestamp() const {
    assert(Valid());
    assert(ts_sz_ > 0);
    return trunc_iter_->timestamp();
  }

  ClippedTombstoneFragment Fragment() const {
    assert(Valid());
    std::string ts_str;
    if (ts_sz_ > 0) {
      Slice ts = trunc_iter_->timestamp();
      ts_str.assign(ts.data(), ts.size());
    }
    return ClippedTombstoneFragment(
        trunc_iter_->start_key().user_key.ToString(),
        trunc_iter_->end_key().user_key.ToString(),
        trunc_iter_->seq(),
        std::move(ts_str));
  }

 private:
  void UpdateValidity() {
    if (!status_.ok()) {
      is_valid_ = false;
      return;
    }
    is_valid_ = trunc_iter_->Valid();
    if (is_valid_) {
      const bool has_ts = (ts_sz_ > 0);
      if (ucmp_->CompareWithoutTimestamp(
              trunc_iter_->start_key().user_key, has_ts,
              trunc_iter_->end_key().user_key, has_ts) >= 0) {
        is_valid_ = false;
      }
    }
  }

  std::unique_ptr<TruncatedRangeDelIterator> trunc_iter_;
  std::shared_ptr<const LocalRangeDelViewState> state_;
  const Comparator* ucmp_;
  size_t ts_sz_ = 0;
  Status status_ = Status::OK();
  bool is_valid_ = false;
  mutable std::string current_key_buf_;
};

// 5. LocalRangeDelIteratorHandle
// Holds the immutable State and WindowGuard.
class LocalRangeDelIteratorHandle {
 public:
  LocalRangeDelIteratorHandle(
      std::shared_ptr<const LocalRangeDelViewState> state,
      std::unique_ptr<WindowGuard> guard)
      : state_(std::move(state)), guard_(std::move(guard)) {}

  WindowGuard* guard() { return guard_.get(); }
  const WindowGuard* guard() const { return guard_.get(); }

  std::shared_ptr<const LocalRangeDelViewState> state() const {
    return state_;
  }

  std::vector<ClippedTombstoneFragment> ForwardStream() {
    std::vector<ClippedTombstoneFragment> result;
    guard_->SeekToFirst();
    while (guard_->Valid()) {
      result.push_back(guard_->Fragment());
      guard_->Next();
    }
    return result;
  }

  std::vector<ClippedTombstoneFragment> BackwardStream() {
    std::vector<ClippedTombstoneFragment> result;
    guard_->SeekToLast();
    while (guard_->Valid()) {
      result.push_back(guard_->Fragment());
      guard_->Prev();
    }
    return result;
  }

 private:
  std::shared_ptr<const LocalRangeDelViewState> state_;
  std::unique_ptr<WindowGuard> guard_;
};

// 6. LocalRangeDelView
// Builds immutable state from AMTV snapshot runs and delta, and produces IteratorHandles.
class LocalRangeDelView {
 public:
  using State = LocalRangeDelViewState;

  LocalRangeDelView(
      const std::vector<std::shared_ptr<const AMTVRun>>& sealed_runs,
      const std::vector<OpenDeltaEntry>& delta_raw,
      const WindowSpec& window,
      const InternalKeyComparator& icmp) {
    InitFromRunsAndDelta(sealed_runs, delta_raw, window, icmp);
  }

  LocalRangeDelView(
      const std::vector<std::vector<OpenDeltaEntry>>& runs_raw,
      const std::vector<OpenDeltaEntry>& delta_raw,
      const WindowSpec& window,
      const InternalKeyComparator& icmp) {
    std::vector<std::shared_ptr<const AMTVRun>> sealed_runs;
    for (size_t i = 0; i < runs_raw.size(); ++i) {
      sealed_runs.push_back(std::make_shared<AMTVRun>(i + 1, runs_raw[i], icmp));
    }
    InitFromRunsAndDelta(sealed_runs, delta_raw, window, icmp);
  }

  std::shared_ptr<const State> state() const { return state_; }

  std::unique_ptr<LocalRangeDelIteratorHandle> CreateIteratorHandle(
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq = kMaxSequenceNumber,
      const Slice* ts_upper_bound = nullptr) const {
    std::unique_ptr<FragmentedRangeTombstoneIterator> frag_iter;
    if (!state_->is_empty_window && !state_->raw_candidates.empty()) {
      frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
          state_->fragmented_list, icmp, read_seq, ts_upper_bound);
    } else {
      auto empty_v = std::make_unique<VectorIterator>(
          std::vector<std::string>{}, std::vector<std::string>{}, &icmp);
      auto empty_list = std::make_shared<FragmentedRangeTombstoneList>(
          std::move(empty_v), icmp);
      frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
          empty_list, icmp, read_seq, ts_upper_bound);
    }

    std::unique_ptr<TruncatedRangeDelIterator> trunc_iter;
    if (state_->is_bounded && !state_->fallback_to_full && !state_->is_empty_window) {
      trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp,
          state_->smallest_ikey.get(), state_->largest_ikey.get());
    } else {
      trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp, nullptr, nullptr);
    }

    auto guard = std::make_unique<WindowGuard>(
        std::move(trunc_iter), state_, icmp.user_comparator());

    return std::make_unique<LocalRangeDelIteratorHandle>(state_, std::move(guard));
  }

  // Phase P2b/P2c: Directly produces a pure native TruncatedRangeDelIterator
  // without any WindowGuard wrapper. Suitable for direct ingestion by MergingIterator.
  // Uses aliased shared_ptr to ensure state_ (and its internal keys/buffers) remains
  // alive for the entire lifetime of TruncatedRangeDelIterator even if LocalRangeDelView
  // is destroyed.
  std::unique_ptr<TruncatedRangeDelIterator> CreateNativeTruncatedRangeDelIterator(
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq = kMaxSequenceNumber,
      const Slice* ts_upper_bound = nullptr) const {
    std::unique_ptr<FragmentedRangeTombstoneIterator> frag_iter;
    if (!state_->is_empty_window && !state_->raw_candidates.empty()) {
      std::shared_ptr<FragmentedRangeTombstoneList> aliased_list(
          state_, state_->fragmented_list.get());
      frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
          aliased_list, icmp, read_seq, ts_upper_bound);
    } else {
      auto empty_v = std::make_unique<VectorIterator>(
          std::vector<std::string>{}, std::vector<std::string>{}, &icmp);
      auto empty_list = std::make_shared<FragmentedRangeTombstoneList>(
          std::move(empty_v), icmp);
      std::shared_ptr<FragmentedRangeTombstoneList> aliased_empty(
          state_, empty_list.get());
      frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
          aliased_empty, icmp, read_seq, ts_upper_bound);
      frag_iter->RegisterCleanup(
          [](void* arg, void*) {
            delete static_cast<std::shared_ptr<FragmentedRangeTombstoneList>*>(arg);
          },
          new std::shared_ptr<FragmentedRangeTombstoneList>(empty_list), nullptr);
    }

    if (state_->is_bounded && !state_->fallback_to_full && !state_->is_empty_window) {
      return std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp,
          state_->smallest_ikey.get(), state_->largest_ikey.get());
    } else {
      return std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp, nullptr, nullptr);
    }
  }

 private:
  void InitFromRunsAndDelta(
      const std::vector<std::shared_ptr<const AMTVRun>>& sealed_runs,
      const std::vector<OpenDeltaEntry>& delta_raw,
      const WindowSpec& window,
      const InternalKeyComparator& icmp) {
    auto state = std::make_shared<State>();
    state->window = window;

    const auto* ucmp = icmp.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    if (!window.IsBounded()) {
      state->fallback_to_full = true;
      state->is_bounded = false;
    } else {
      state->is_bounded = true;
      if (!window.IsValidBounded(ucmp)) {
        state->is_empty_window = true;
      }
    }

    if (state->is_empty_window) {
      state->candidate_count = 0;
    } else if (state->fallback_to_full) {
      for (const auto& run : sealed_runs) {
        if (!run) continue;
        for (const auto& e : run->raw_entries) {
          state->raw_candidates.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
        }
      }
      for (const auto& e : delta_raw) {
        state->raw_candidates.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
      }
      state->candidate_count = state->raw_candidates.size();
    } else {
      Slice L_slice = *window.lower_bound;
      Slice U_slice = *window.upper_bound;
      for (const auto& run : sealed_runs) {
        if (!run) continue;
        std::vector<size_t> indices;
        AMTVRunIntervalIndexAuditInfo audit;
        run->CollectIntersectingRawEntryIndices(&L_slice, &U_slice, icmp,
                                                &indices, &audit);
        for (size_t idx : indices) {
          const auto& e = run->raw_entries[idx];
          state->raw_candidates.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
        }
      }
      for (const auto& e : delta_raw) {
        if (AMTVLocalScanReferenceView::IsIntersecting(e, &L_slice, &U_slice, ucmp)) {
          state->raw_candidates.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
        }
      }
      state->candidate_count = state->raw_candidates.size();
    }

    // Sort raw_candidates by InternalKeyComparator
    std::sort(state->raw_candidates.begin(), state->raw_candidates.end(),
              [&icmp](const OwnedRawRangeTombstone& a, const OwnedRawRangeTombstone& b) {
                auto key_a = a.Serialize().first;
                auto key_b = b.Serialize().first;
                return icmp.Compare(key_a.Encode(), key_b.Encode()) < 0;
              });

    // Serialize
    state->serialized_keys.reserve(state->raw_candidates.size());
    state->serialized_values.reserve(state->raw_candidates.size());
    for (const auto& c : state->raw_candidates) {
      auto kv = c.Serialize();
      state->serialized_keys.push_back(kv.first.Encode().ToString());
      state->serialized_values.push_back(kv.second.ToString());
    }

    // Prepare smallest and largest keys for TruncatedRangeDelIterator
    if (state->is_bounded && !state->is_empty_window) {
      if (has_ts) {
        AppendKeyWithMaxTimestamp(&state->smallest_key_buf, *window.lower_bound, ts_sz);
        AppendKeyWithMaxTimestamp(&state->largest_key_buf, *window.upper_bound, ts_sz);
      } else {
        state->smallest_key_buf = *window.lower_bound;
        state->largest_key_buf = *window.upper_bound;
      }
      state->smallest_ikey = std::make_unique<InternalKey>(
          state->smallest_key_buf, kMaxSequenceNumber, kTypeRangeDeletion);
      state->largest_ikey = std::make_unique<InternalKey>(
          state->largest_key_buf, kMaxSequenceNumber, kTypeRangeDeletion);
    }

    // Build native FragmentedRangeTombstoneList
    auto v_iter = std::make_unique<VectorIterator>(
        state->serialized_keys, state->serialized_values, &icmp);
    state->fragmented_list = std::make_shared<FragmentedRangeTombstoneList>(
        std::move(v_iter), icmp);

    state_ = std::move(state);
  }

  std::shared_ptr<const State> state_;
};

// 7. CanonicalRangeDelTruth
// Constructs global truth using all raw tombstones and native TruncatedRangeDelIterator.
class CanonicalRangeDelTruth {
 public:
  CanonicalRangeDelTruth(
      const std::vector<OwnedRawRangeTombstone>& all_tombstones,
      const WindowSpec& window,
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq = kMaxSequenceNumber,
      const Slice* ts_upper_bound = nullptr)
      : window_(window), icmp_(icmp), read_seq_(read_seq), ts_upper_bound_(ts_upper_bound) {
    const auto* ucmp = icmp.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);

    auto sorted_tombstones = all_tombstones;
    std::sort(sorted_tombstones.begin(), sorted_tombstones.end(),
              [&icmp](const OwnedRawRangeTombstone& a, const OwnedRawRangeTombstone& b) {
                auto key_a = a.Serialize().first;
                auto key_b = b.Serialize().first;
                return icmp.Compare(key_a.Encode(), key_b.Encode()) < 0;
              });

    std::vector<std::string> keys, values;
    keys.reserve(sorted_tombstones.size());
    values.reserve(sorted_tombstones.size());
    for (const auto& t : sorted_tombstones) {
      auto kv = t.Serialize();
      keys.push_back(kv.first.Encode().ToString());
      values.push_back(kv.second.ToString());
    }

    auto v_iter = std::make_unique<VectorIterator>(keys, values, &icmp_);
    frag_list_ = std::make_shared<FragmentedRangeTombstoneList>(
        std::move(v_iter), icmp_);

    if (window_.IsBounded() && window_.IsValidBounded(ucmp)) {
      if (has_ts) {
        AppendKeyWithMaxTimestamp(&smallest_key_buf_, *window_.lower_bound, ts_sz);
        AppendKeyWithMaxTimestamp(&largest_key_buf_, *window_.upper_bound, ts_sz);
      } else {
        smallest_key_buf_ = *window_.lower_bound;
        largest_key_buf_ = *window_.upper_bound;
      }
      smallest_ikey_ = std::make_unique<InternalKey>(
          smallest_key_buf_, kMaxSequenceNumber, kTypeRangeDeletion);
      largest_ikey_ = std::make_unique<InternalKey>(
          largest_key_buf_, kMaxSequenceNumber, kTypeRangeDeletion);
    }
  }

  std::vector<ClippedTombstoneFragment> ForwardStream(
      const Slice* ts_upper_bound = nullptr) const {
    const Slice* effective_ts = ts_upper_bound ? ts_upper_bound : ts_upper_bound_;
    std::vector<ClippedTombstoneFragment> result;
    auto frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
        frag_list_, icmp_, read_seq_, effective_ts);
    std::unique_ptr<TruncatedRangeDelIterator> trunc_iter;
    if (smallest_ikey_ && largest_ikey_) {
      trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp_, smallest_ikey_.get(), largest_ikey_.get());
    } else {
      trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp_, nullptr, nullptr);
    }
    trunc_iter->SeekToFirst();
    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);
    while (trunc_iter->Valid()) {
      if (ucmp->CompareWithoutTimestamp(
              trunc_iter->start_key().user_key, has_ts,
              trunc_iter->end_key().user_key, has_ts) >= 0) {
        break;
      }
      std::string ts_str;
      if (ts_sz > 0) {
        Slice ts = trunc_iter->timestamp();
        ts_str.assign(ts.data(), ts.size());
      }
      result.emplace_back(
          trunc_iter->start_key().user_key.ToString(),
          trunc_iter->end_key().user_key.ToString(),
          trunc_iter->seq(),
          std::move(ts_str));
      trunc_iter->Next();
    }
    return result;
  }

  std::vector<ClippedTombstoneFragment> BackwardStream(
      const Slice* ts_upper_bound = nullptr) const {
    const Slice* effective_ts = ts_upper_bound ? ts_upper_bound : ts_upper_bound_;
    std::vector<ClippedTombstoneFragment> result;
    auto frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
        frag_list_, icmp_, read_seq_, effective_ts);
    std::unique_ptr<TruncatedRangeDelIterator> trunc_iter;
    if (smallest_ikey_ && largest_ikey_) {
      trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp_, smallest_ikey_.get(), largest_ikey_.get());
    } else {
      trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp_, nullptr, nullptr);
    }
    trunc_iter->SeekToLast();
    if (!trunc_iter->Valid() && smallest_ikey_ && largest_ikey_) {
      trunc_iter->Prev();
    }
    const auto* ucmp = icmp_.user_comparator();
    const size_t ts_sz = ucmp->timestamp_size();
    const bool has_ts = (ts_sz > 0);
    while (trunc_iter->Valid()) {
      if (ucmp->CompareWithoutTimestamp(
              trunc_iter->start_key().user_key, has_ts,
              trunc_iter->end_key().user_key, has_ts) >= 0) {
        break;
      }
      std::string ts_str;
      if (ts_sz > 0) {
        Slice ts = trunc_iter->timestamp();
        ts_str.assign(ts.data(), ts.size());
      }
      result.emplace_back(
          trunc_iter->start_key().user_key.ToString(),
          trunc_iter->end_key().user_key.ToString(),
          trunc_iter->seq(),
          std::move(ts_str));
      trunc_iter->Prev();
    }
    return result;
  }

 private:
  WindowSpec window_;
  InternalKeyComparator icmp_;
  SequenceNumber read_seq_;
  const Slice* ts_upper_bound_ = nullptr;
  std::string smallest_key_buf_;
  std::string largest_key_buf_;
  std::unique_ptr<InternalKey> smallest_ikey_;
  std::unique_ptr<InternalKey> largest_ikey_;
  std::shared_ptr<FragmentedRangeTombstoneList> frag_list_;
};

// 8. VerifyFourWayDifferential
// Verifies four-way differential equivalence: Local vs Canonical vs Independent Oracle vs Symmetry.
inline void VerifyFourWayDifferential(
    LocalRangeDelIteratorHandle* handle,
    CanonicalRangeDelTruth* canonical_truth,
    const AMTVIndependentPointwiseOracle& independent_oracle,
    const WindowSpec& window,
    const std::vector<std::string>& probe_keys,
    const std::vector<SequenceNumber>& probe_seqs,
    const InternalKeyComparator& icmp,
    const Slice* ts_upper_bound = nullptr) {
  auto local_fwd = handle->ForwardStream();
  auto canonical_fwd = canonical_truth->ForwardStream(ts_upper_bound);

  ASSERT_EQ(local_fwd.size(), canonical_fwd.size())
      << "Stream size mismatch (Forward): Local=" << local_fwd.size()
      << " Canonical=" << canonical_fwd.size();
  for (size_t i = 0; i < local_fwd.size(); ++i) {
    EXPECT_EQ(local_fwd[i], canonical_fwd[i])
        << "Fragment mismatch at index " << i
        << "\nLocal: " << local_fwd[i].ToString()
        << "\nCanonical: " << canonical_fwd[i].ToString();
  }

  auto local_bwd = handle->BackwardStream();
  auto canonical_bwd = canonical_truth->BackwardStream(ts_upper_bound);
  ASSERT_EQ(local_bwd.size(), canonical_bwd.size())
      << "Stream size mismatch (Backward)";
  for (size_t i = 0; i < local_bwd.size(); ++i) {
    EXPECT_EQ(local_bwd[i], canonical_bwd[i])
        << "Backward fragment mismatch at index " << i;
  }

  // Symmetry: forward stream must match reversed backward stream
  auto reversed_bwd = local_bwd;
  std::reverse(reversed_bwd.begin(), reversed_bwd.end());
  ASSERT_EQ(local_fwd, reversed_bwd)
      << "Symmetry mismatch between forward and reversed backward stream";

  // Boundary check: all fragments must satisfy L <= start < end <= U
  const auto* ucmp = icmp.user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);
  for (const auto& frag : local_fwd) {
    if (window.lower_bound.has_value()) {
      EXPECT_GE(ucmp->CompareWithoutTimestamp(frag.start_key, has_ts,
                                              *window.lower_bound, false), 0);
    }
    if (window.upper_bound.has_value()) {
      EXPECT_LE(ucmp->CompareWithoutTimestamp(frag.end_key, has_ts,
                                              *window.upper_bound, false), 0);
    }
  }

  // Pointwise Oracle check
  for (const auto& key : probe_keys) {
    if (window.lower_bound.has_value() &&
        ucmp->CompareWithoutTimestamp(key, has_ts, *window.lower_bound, false) < 0) {
      continue;
    }
    if (window.upper_bound.has_value() &&
        ucmp->CompareWithoutTimestamp(key, has_ts, *window.upper_bound, false) >= 0) {
      continue;
    }

    SequenceNumber local_max_seq = 0;
    for (const auto& frag : local_fwd) {
      if (ucmp->CompareWithoutTimestamp(frag.start_key, has_ts, key, has_ts) <= 0 &&
          ucmp->CompareWithoutTimestamp(key, has_ts, frag.end_key, has_ts) < 0) {
        if (frag.seq > local_max_seq) {
          local_max_seq = frag.seq;
        }
      }
    }

    SequenceNumber oracle_max_seq = independent_oracle.MaxCoveringTombstoneSeqnum(key);
    EXPECT_EQ(local_max_seq, oracle_max_seq)
        << "Pointwise Oracle max seq mismatch for key: " << key;

    for (SequenceNumber pseq : probe_seqs) {
      bool local_del = (local_max_seq > pseq);
      bool oracle_del = independent_oracle.ShouldDelete(key, pseq);
      EXPECT_EQ(local_del, oracle_del)
          << "ShouldDelete mismatch for key " << key << " at seq " << pseq;
    }
  }
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
  ScopedEnvBackgroundThreads scoped_bg(Env::Default(), 1, Env::Priority::LOW);

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
  ASSERT_TRUE(amtv_state->WaitForMergeStable(10000000));
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

  // Empty Slice convention: ["", "") is an empty window (L == U), returning 0 candidates
  {
    Slice L("");
    Slice U("");
    auto linear_cands = GetLinearCandidates({raw_entries}, {}, &L, &U, BytewiseComparator());
    std::vector<OpenDeltaEntry> index_cands;
    index.GetCandidates(&L, &U, &index_cands);
    VerifyCandidateMultiSetsIdentical(linear_cands, index_cands, BytewiseComparator());
    EXPECT_EQ(index_cands.size(), 0U);
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
  ScopedEnvBackgroundThreads scoped_bg(Env::Default(), 1, Env::Priority::LOW);

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
  const void* old_run0_index_ptr = old_run0->sidecar_index().sorted_indices().data();
  const void* old_run1_index_ptr = old_run1->sidecar_index().sorted_indices().data();
  EXPECT_NE(old_run0_index_ptr, nullptr);
  EXPECT_NE(old_run1_index_ptr, nullptr);

  // Wait for background binary merge of Run 0 and Run 1 to complete into Run 2
  ASSERT_TRUE(amtv_state->WaitForMergeStable(10000000));
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
  const void* new_run2_index_ptr = new_run2->sidecar_index().sorted_indices().data();

  // Verification 1: Memory Address Independence (Zero mutation of old snapshot runs)
  EXPECT_NE(old_run0_index_ptr, new_run2_index_ptr);
  EXPECT_NE(old_run1_index_ptr, new_run2_index_ptr);
  EXPECT_EQ(old_snapshot->sealed_runs[0]->sidecar_index().sorted_indices().data(), old_run0_index_ptr);
  EXPECT_EQ(old_snapshot->sealed_runs[1]->sidecar_index().sorted_indices().data(), old_run1_index_ptr);

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

    // Log structural audit for window: distinguish snapshot total candidates from per-run candidates
    size_t delta_cands = sidecar_cands_new.size() - new_audits[0].candidate_count;
    std::cout << "Window: " << w_name << "\n"
              << "  Old Snapshot (Runs=2, Level=0): TotalCands=" << sidecar_cands_old.size()
              << " [Run0: N=" << old_audits[0].raw_entries_count
              << ", Cands=" << old_audits[0].candidate_count
              << ", bytes=" << old_audits[0].index_bytes
              << ", [" << old_audits[0].left << "," << old_audits[0].right << ")"
              << ", span=" << old_audits[0].span
              << "; Run1: N=" << old_audits[1].raw_entries_count
              << ", Cands=" << old_audits[1].candidate_count
              << ", bytes=" << old_audits[1].index_bytes
              << ", [" << old_audits[1].left << "," << old_audits[1].right << ")"
              << ", span=" << old_audits[1].span << "]\n"
              << "  New Snapshot (Run=1, Level=1, chunks=2): TotalCands=" << sidecar_cands_new.size()
              << " [Run2: N=" << new_audits[0].raw_entries_count
              << ", Cands=" << new_audits[0].candidate_count
              << ", bytes=" << new_audits[0].index_bytes
              << ", [" << new_audits[0].left << "," << new_audits[0].right << ")"
              << ", span=" << new_audits[0].span
              << "; Delta: N=" << new_snapshot->open_delta->size()
              << ", Cands=" << delta_cands << "]\n";
  }

  amtv_state->CancelAndDrain();
}

// --------------------------------------------------------------------------
// 23. M4-P1b-1.1: Container Reuse & Overwrite Semantics Test
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, CollectIntersectingRawEntryIndices_ContainerReuse) {
  std::vector<OpenDeltaEntry> entries;
  entries.emplace_back("k10", "k30", 10);
  entries.emplace_back("k20", "k40", 20);
  entries.emplace_back("k30", "k50", 30);
  entries.emplace_back("k40", "k60", 40);
  AMTVRun run(1, std::move(entries), bytewise_icmp_);

  std::string L = "k25", U = "k45";
  Slice L_slice(L), U_slice(U);

  // Pre-fill indices container with dirty dummy values
  std::vector<size_t> indices = {999, 888, 777, 666, 555};
  AMTVRunIntervalIndexAuditInfo audit;

  // Query [k25, k45)
  run.CollectIntersectingRawEntryIndices(&L_slice, &U_slice, bytewise_icmp_, &indices, &audit);

  // Overwrite contract verification
  EXPECT_LE(indices.size(), run.raw_entries.size());
  EXPECT_EQ(indices.size(), audit.candidate_count);
  EXPECT_EQ(audit.candidate_count, 4U);

  for (size_t idx : indices) {
    EXPECT_LT(idx, run.raw_entries.size());
    EXPECT_NE(idx, 999U);
    EXPECT_NE(idx, 888U);
  }

  // Second call with empty window [k50, k20) reusing the same dirty container:
  // must clear and output 0
  std::string L_inv = "k50", U_inv = "k20";
  Slice L_inv_s(L_inv), U_inv_s(U_inv);
  run.CollectIntersectingRawEntryIndices(&L_inv_s, &U_inv_s, bytewise_icmp_, &indices, &audit);
  EXPECT_TRUE(indices.empty());
  EXPECT_EQ(audit.candidate_count, 0U);
}

// --------------------------------------------------------------------------
// 24. M4-P1b-1.1: Sidecar Index Mathematical Invariants Test
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, AMTVRun_SidecarIndexInvariants) {
  // Case 1: Empty run
  std::vector<OpenDeltaEntry> empty_entries;
  AMTVRun empty_run(1, empty_entries, bytewise_icmp_);
  std::string err;
  EXPECT_TRUE(empty_run.sidecar_index().VerifyInvariants(empty_run.raw_entries, bytewise_icmp_, &err)) << err;
  EXPECT_EQ(empty_run.index_bytes(), 0U);

  // Case 2: Single tombstone run
  std::vector<OpenDeltaEntry> single_entries = {OpenDeltaEntry("k10", "k20", 100)};
  AMTVRun single_run(2, single_entries, bytewise_icmp_);
  EXPECT_TRUE(single_run.sidecar_index().VerifyInvariants(single_run.raw_entries, bytewise_icmp_, &err)) << err;
  EXPECT_EQ(single_run.index_bytes(), 16U); // 2 * 1 * sizeof(size_t) = 16 bytes

  // Case 3: Multiple tombstones with complex interleaving
  std::vector<OpenDeltaEntry> entries = {
      OpenDeltaEntry("k50", "k60", 50),
      OpenDeltaEntry("k10", "k90", 10), // super-long
      OpenDeltaEntry("k20", "k30", 20),
      OpenDeltaEntry("k30", "k40", 30),
      OpenDeltaEntry("k10", "k20", 15), // same start as k10..k90, different end & seq
      OpenDeltaEntry("k70", "k80", 70),
  };
  AMTVRun run(3, entries, bytewise_icmp_);
  EXPECT_TRUE(run.sidecar_index().VerifyInvariants(run.raw_entries, bytewise_icmp_, &err)) << err;
  EXPECT_EQ(run.index_bytes(), entries.size() * 16U);

  const auto& sorted = run.sidecar_index().sorted_indices();
  const auto& pmax = run.sidecar_index().prefix_max_end_index();
  ASSERT_EQ(sorted.size(), entries.size());
  ASSERT_EQ(pmax.size(), entries.size());

  // Monotonicity of sorted_indices
  for (size_t i = 1; i < sorted.size(); ++i) {
    int c = BytewiseComparator()->Compare(
        run.raw_entries[sorted[i - 1]].user_start_key(),
        run.raw_entries[sorted[i]].user_start_key());
    EXPECT_LE(c, 0);
  }

  // Monotonicity of prefix_max_end_index
  for (size_t i = 1; i < pmax.size(); ++i) {
    int c = BytewiseComparator()->Compare(
        run.raw_entries[pmax[i - 1]].user_end_key(),
        run.raw_entries[pmax[i]].user_end_key());
    EXPECT_LE(c, 0);
  }

  // Index validity
  for (size_t i = 0; i < sorted.size(); ++i) {
    EXPECT_LT(sorted[i], entries.size());
    EXPECT_LT(pmax[i], entries.size());
  }
}

// --------------------------------------------------------------------------
// 25. M4-P1b-1.1: Large Run Lifecycle Test (B=8 -> L1(16), L2(32), L3(64))
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, AMTVRun_LargeRunLifecycle_L1_L2_L3) {
  auto PadNumber = [](int n, int width) -> std::string {
    std::ostringstream ss;
    ss << std::setw(width) << std::setfill('0') << n;
    return ss.str();
  };

  auto MergeTwoRuns = [&](const std::shared_ptr<const AMTVRun>& r1,
                          const std::shared_ptr<const AMTVRun>& r2,
                          uint64_t run_id) -> std::shared_ptr<const AMTVRun> {
    std::vector<OpenDeltaEntry> merged_entries;
    merged_entries.reserve(r1->raw_entries.size() + r2->raw_entries.size());
    merged_entries.insert(merged_entries.end(), r1->raw_entries.begin(), r1->raw_entries.end());
    merged_entries.insert(merged_entries.end(), r2->raw_entries.begin(), r2->raw_entries.end());
    return std::make_shared<const AMTVRun>(
        run_id, r1->level + 1, r1->source_chunk_count + r2->source_chunk_count,
        /*is_partial=*/false, std::move(merged_entries), bytewise_icmp_);
  };

  // Step 1: Construct 8 Level 0 runs (8 entries each)
  std::vector<std::shared_ptr<const AMTVRun>> l0_runs;
  for (int r = 0; r < 8; ++r) {
    std::vector<OpenDeltaEntry> l0_entries;
    for (int j = 0; j < 8; ++j) {
      int idx = r * 8 + j;
      std::string start = "k" + PadNumber(idx * 10, 4);
      std::string end = "k" + PadNumber((idx * 10) + 15, 4);
      l0_entries.emplace_back(start, end, (idx + 1) * 10);
    }
    l0_runs.push_back(std::make_shared<const AMTVRun>(
        r + 1, /*level=*/0, /*chunk_count=*/1, /*is_partial=*/false,
        std::move(l0_entries), bytewise_icmp_));
  }

  // Step 2: Merge pairs of Level 0 runs into 4 Level 1 runs (16 entries each)
  std::vector<std::shared_ptr<const AMTVRun>> l1_runs;
  for (int i = 0; i < 4; ++i) {
    l1_runs.push_back(MergeTwoRuns(l0_runs[2 * i], l0_runs[2 * i + 1], 10 + i));
    EXPECT_EQ(l1_runs.back()->level, 1U);
    EXPECT_EQ(l1_runs.back()->raw_entries.size(), 16U);
  }

  // Step 3: Merge pairs of Level 1 runs into 2 Level 2 runs (32 entries each)
  std::vector<std::shared_ptr<const AMTVRun>> l2_runs;
  for (int i = 0; i < 2; ++i) {
    l2_runs.push_back(MergeTwoRuns(l1_runs[2 * i], l1_runs[2 * i + 1], 20 + i));
    EXPECT_EQ(l2_runs.back()->level, 2U);
    EXPECT_EQ(l2_runs.back()->raw_entries.size(), 32U);
  }

  // --- Point 1 & Point 2: Snapshot Before L3 Merge (holds the 2 Level 2 runs, 32 entries each) ---
  auto snap_before_l3 = std::make_shared<AMTVSnapshot>();
  snap_before_l3->sealed_runs = l2_runs;
  snap_before_l3->open_delta = std::make_shared<OpenDelta>();

  ASSERT_EQ(snap_before_l3->sealed_run_count(), 2U);
  EXPECT_EQ(snap_before_l3->sealed_runs[0]->level, 2U);
  EXPECT_EQ(snap_before_l3->sealed_runs[0]->raw_entries.size(), 32U);
  EXPECT_EQ(snap_before_l3->sealed_runs[1]->level, 2U);
  EXPECT_EQ(snap_before_l3->sealed_runs[1]->raw_entries.size(), 32U);
  EXPECT_EQ(snap_before_l3->open_delta->size(), 0U);

  // Validate sidecar invariants on both 32-tombstone Level 2 runs
  std::string err;
  EXPECT_TRUE(snap_before_l3->sealed_runs[0]->sidecar_index().VerifyInvariants(
      snap_before_l3->sealed_runs[0]->raw_entries, bytewise_icmp_, &err)) << err;
  EXPECT_TRUE(snap_before_l3->sealed_runs[1]->sidecar_index().VerifyInvariants(
      snap_before_l3->sealed_runs[1]->raw_entries, bytewise_icmp_, &err)) << err;

  // Step 4: Merge the two Level 2 runs into 1 Level 3 run (64 entries)
  auto run_l3 = MergeTwoRuns(l2_runs[0], l2_runs[1], 30);
  EXPECT_EQ(run_l3->level, 3U);
  EXPECT_EQ(run_l3->raw_entries.size(), 64U);

  // --- Point 3: Snapshot After L3 Merge (holds the 1 Level 3 run + Open Delta with 2 tombstones) ---
  auto new_delta = std::make_shared<OpenDelta>();
  new_delta->AddEntry("k0100", "k0500", 700);
  new_delta->AddEntry("k0250", "k0650", 750);

  auto snap_after_l3 = std::make_shared<AMTVSnapshot>();
  snap_after_l3->sealed_runs = {run_l3};
  snap_after_l3->open_delta = new_delta;

  ASSERT_EQ(snap_after_l3->sealed_run_count(), 1U);
  EXPECT_EQ(snap_after_l3->sealed_runs[0]->level, 3U);
  EXPECT_EQ(snap_after_l3->sealed_runs[0]->raw_entries.size(), 64U);
  EXPECT_EQ(snap_after_l3->open_delta->size(), 2U);

  // Validate sidecar invariants on 64-tombstone Level 3 run
  EXPECT_TRUE(snap_after_l3->sealed_runs[0]->sidecar_index().VerifyInvariants(
      snap_after_l3->sealed_runs[0]->raw_entries, bytewise_icmp_, &err)) << err;

  // Query across diverse windows
  struct LargeWindowCase {
    const char* l_str;
    const char* u_str;
  };
  std::vector<LargeWindowCase> test_windows = {
      {"k0100", "k0400"}, // Dense overlap in lower half
      {"k0300", "k0600"}, // Straddling mid-range
      {"k0550", "k0650"}, // Near upper boundary
      {"k0900", "k0950"}, // Completely disjoint
      {nullptr, nullptr},  // Unbounded
  };

  std::vector<std::string> probe_keys = {"k0000", "k0100", "k0200", "k0300", "k0400", "k0500", "k0600", "k0700"};
  std::vector<SequenceNumber> probe_seqs = {50, 150, 350, 600, 720, 800};

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

    // 1. Verify Before Snapshot (two 32-entry Level 2 runs)
    for (size_t r = 0; r < snap_before_l3->sealed_runs.size(); ++r) {
      const auto& run = snap_before_l3->sealed_runs[r];
      std::vector<size_t> indices;
      AMTVRunIntervalIndexAuditInfo audit;
      run->CollectIntersectingRawEntryIndices(pL, pU, bytewise_icmp_, &indices, &audit);

      EXPECT_LE(audit.candidate_count, run->raw_entries.size());
      EXPECT_EQ(indices.size(), audit.candidate_count);
      EXPECT_EQ(audit.span, audit.right - audit.left);
      EXPECT_EQ(audit.index_bytes, run->raw_entries.size() * 16U);

      // Verify each candidate matches linear check on that run
      std::vector<OpenDeltaEntry> run_linear;
      for (const auto& e : run->raw_entries) {
        if (AMTVLocalScanReferenceView::IsIntersecting(e, pL, pU, BytewiseComparator())) {
          run_linear.push_back(e);
        }
      }
      EXPECT_EQ(indices.size(), run_linear.size());
    }

    // 2. Verify After Snapshot (one 64-entry Level 3 run + Open Delta)
    const auto& target_run_l3 = snap_after_l3->sealed_runs[0];
    std::vector<size_t> l3_indices;
    AMTVRunIntervalIndexAuditInfo l3_audit;
    target_run_l3->CollectIntersectingRawEntryIndices(pL, pU, bytewise_icmp_, &l3_indices, &l3_audit);

    EXPECT_LE(l3_audit.candidate_count, 64U);
    EXPECT_EQ(l3_indices.size(), l3_audit.candidate_count);
    EXPECT_EQ(l3_audit.span, l3_audit.right - l3_audit.left);
    EXPECT_EQ(l3_audit.index_bytes, 64U * 16U); // 1,024 bytes

    // Check linear equivalence for Level 3 run
    std::vector<OpenDeltaEntry> l3_linear;
    for (const auto& e : target_run_l3->raw_entries) {
      if (AMTVLocalScanReferenceView::IsIntersecting(e, pL, pU, BytewiseComparator())) {
        l3_linear.push_back(e);
      }
    }
    EXPECT_EQ(l3_indices.size(), l3_linear.size());

    // 3. Independent Pointwise Oracle verification on both snapshots
    AMTVCanonicalFullTruth truth_before(
        {snap_before_l3->sealed_runs[0]->raw_entries, snap_before_l3->sealed_runs[1]->raw_entries},
        snap_before_l3->open_delta->entries(), bytewise_icmp_, 1000);
    AMTVIndependentPointwiseOracle oracle_before(
        {snap_before_l3->sealed_runs[0]->raw_entries, snap_before_l3->sealed_runs[1]->raw_entries},
        snap_before_l3->open_delta->entries(), BytewiseComparator(), 1000);
    AMTVLocalScanReferenceView view_before(*snap_before_l3, pL, pU, bytewise_icmp_, 1000);
    Verify3WayPointwise(view_before, truth_before, oracle_before, pL, pU, probe_keys, probe_seqs, BytewiseComparator());

    AMTVCanonicalFullTruth truth_after(
        {run_l3->raw_entries}, snap_after_l3->open_delta->entries(), bytewise_icmp_, 1000);
    AMTVIndependentPointwiseOracle oracle_after(
        {run_l3->raw_entries}, snap_after_l3->open_delta->entries(), BytewiseComparator(), 1000);
    AMTVLocalScanReferenceView view_after(*snap_after_l3, pL, pU, bytewise_icmp_, 1000);
    Verify3WayPointwise(view_after, truth_after, oracle_after, pL, pU, probe_keys, probe_seqs, BytewiseComparator());
  }
}

// ==========================================================================
// Phase A: CanonicalRawWriteWitness and Associated Structures
// ==========================================================================
struct CanonicalTombstoneIdentity {
  std::string start_key;
  std::string end_key;
  SequenceNumber seq;
  std::string timestamp;

  bool operator==(const CanonicalTombstoneIdentity& other) const {
    return start_key == other.start_key && end_key == other.end_key &&
           seq == other.seq && timestamp == other.timestamp;
  }
  bool operator<(const CanonicalTombstoneIdentity& other) const {
    if (start_key != other.start_key) return start_key < other.start_key;
    if (end_key != other.end_key) return end_key < other.end_key;
    if (seq != other.seq) return seq < other.seq;
    return timestamp < other.timestamp;
  }
};

struct CanonicalPointWrite {
  std::string key;
  std::string value;
  SequenceNumber seq;
};

struct CanonicalOpLogEntry {
  SequenceNumber seq;
  std::string op_type;
  std::string desc;
};

class CanonicalRawWriteWitness {
 public:
  explicit CanonicalRawWriteWitness(uint64_t expected_generation = 1)
      : generation_(expected_generation) {}

  void RecordPointWrite(uint64_t gen, const std::string& key,
                        const std::string& value, SequenceNumber seq,
                        const std::string& note = "") {
    if (gen != generation_) {
      throw std::runtime_error("Witness fail-fast: generation mismatch! Expected " +
                               std::to_string(generation_) + ", got " + std::to_string(gen));
    }
    points_.push_back({key, value, seq});
    std::string desc = "Put(" + key + ", " + value + ")";
    if (!note.empty()) {
      desc += " [" + note + "]";
    }
    op_log_.push_back({seq, "PUT", desc});
  }

  void RecordWrite(uint64_t gen, const std::string& start, const std::string& end,
                   SequenceNumber seq, const std::string& ts = "") {
    if (gen != generation_) {
      throw std::runtime_error("Witness fail-fast: generation mismatch! Expected " +
                               std::to_string(generation_) + ", got " + std::to_string(gen));
    }
    entries_.push_back({start, end, seq, ts});
    std::string desc = "DeleteRange([" + start + ", " + end + "))";
    op_log_.push_back({seq, "DELETE_RANGE", desc});
  }

  uint64_t generation() const { return generation_; }
  const std::vector<CanonicalTombstoneIdentity>& entries() const {
    return entries_;
  }
  const std::vector<CanonicalPointWrite>& points() const {
    return points_;
  }
  const std::vector<CanonicalOpLogEntry>& op_log() const {
    return op_log_;
  }
  std::multiset<CanonicalTombstoneIdentity> ToMultiset() const {
    return std::multiset<CanonicalTombstoneIdentity>(entries_.begin(), entries_.end());
  }

  bool GetVisiblePoint(const std::string& key, SequenceNumber read_seq,
                       SequenceNumber* point_seq, std::string* point_val) const {
    SequenceNumber best_seq = 0;
    std::string best_val;
    bool found = false;
    for (const auto& p : points_) {
      if (p.key == key && p.seq <= read_seq) {
        if (!found || p.seq > best_seq) {
          best_seq = p.seq;
          best_val = p.value;
          found = true;
        }
      }
    }
    if (found) {
      if (point_seq) *point_seq = best_seq;
      if (point_val) *point_val = best_val;
    }
    return found;
  }

  SequenceNumber MaxCoveringTombstoneSeq(const std::string& key, SequenceNumber read_seq) const {
    SequenceNumber max_seq = 0;
    for (const auto& e : entries_) {
      if (e.seq <= read_seq) {
        if (key >= e.start_key && key < e.end_key) {
          if (e.seq > max_seq) {
            max_seq = e.seq;
          }
        }
      }
    }
    return max_seq;
  }

  void PrintOperationLog() const {
    std::vector<CanonicalOpLogEntry> sorted_log = op_log_;
    std::sort(sorted_log.begin(), sorted_log.end(),
              [](const CanonicalOpLogEntry& a, const CanonicalOpLogEntry& b) {
                return a.seq < b.seq;
              });
    fprintf(stderr, "\n=== [Sequence-Ordered Canonical Operations Log] ===\n");
    fprintf(stderr, "| seq | op_type      | description                                  |\n");
    fprintf(stderr, "|:---:|:------------:|:---------------------------------------------|\n");
    for (const auto& op : sorted_log) {
      fprintf(stderr, "| %-3" PRIu64 " | %-12s | %-44s |\n",
              static_cast<uint64_t>(op.seq), op.op_type.c_str(), op.desc.c_str());
    }
  }

 private:
  uint64_t generation_;
  std::vector<CanonicalTombstoneIdentity> entries_;
  std::vector<CanonicalPointWrite> points_;
  std::vector<CanonicalOpLogEntry> op_log_;
};

static std::multiset<CanonicalTombstoneIdentity> ExtractSnapshotRawMultiset(
    const AMTVSnapshot& snap, size_t ts_sz = 0) {
  std::multiset<CanonicalTombstoneIdentity> result;
  for (const auto& run : snap.sealed_runs) {
    if (run) {
      for (const auto& e : run->raw_entries) {
        result.insert(CanonicalTombstoneIdentity{
            e.user_start_key().ToString(),
            e.user_end_key().ToString(),
            e.seq,
            e.timestamp(ts_sz).ToString()});
      }
    }
  }
  if (snap.open_delta) {
    for (const auto& e : snap.open_delta->entries()) {
      result.insert(CanonicalTombstoneIdentity{
          e.user_start_key().ToString(),
          e.user_end_key().ToString(),
          e.seq,
          e.timestamp(ts_sz).ToString()});
    }
  }
  return result;
}

// --------------------------------------------------------------------------
// Test: P1b12_CanonicalRawWriteWitness_TwoTierVerification
// Tier 1: Structural multiset parity before and after background binary merge
// Tier 2: MVCC visibility parity across multiple read sequences
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b12_CanonicalRawWriteWitness_TwoTierVerification) {
  ScopedEnvBackgroundThreads scoped_bg(Env::Default(), 1, Env::Priority::LOW);
  const uint64_t kMemtableGen = 1;
  auto amtv_state = std::make_shared<AMTVState>(
      kMemtableGen /*memtable_generation*/, 8 /*delta_limit*/, 2 /*merge_soft_limit*/,
      8 /*hard_limit*/, &bytewise_icmp_, Env::Default());
  CanonicalRawWriteWitness witness(kMemtableGen);

  // 1. Initial point writes into witness
  witness.RecordPointWrite(kMemtableGen, "key0004", "val_v1", 2, "initial point");
  witness.RecordPointWrite(kMemtableGen, "key0124", "val_x1", 6, "initial point");

  // 2. Write 4 tombstones to Open Delta
  for (int i = 0; i < 4; ++i) {
    char buf_s[32], buf_e[32];
    snprintf(buf_s, sizeof(buf_s), "key%04d", i * 20);
    snprintf(buf_e, sizeof(buf_e), "key%04d", i * 20 + 8);
    std::string s(buf_s);
    std::string e(buf_e);
    SequenceNumber seq = 10 + i; // 10, 11, 12, 13
    witness.RecordWrite(kMemtableGen, s, e, seq);
    amtv_state->AddTombstone(s, e, seq, bytewise_icmp_);
  }

  // Put resurrection for key0004 at seq 14 (between tombstones 3 and 4)
  witness.RecordPointWrite(kMemtableGen, "key0004", "val_v2", 14, "Put resurrection");

  // Write remaining 4 tombstones to fill Open Delta and seal into Run 0 (size 8)
  for (int i = 4; i < 8; ++i) {
    char buf_s[32], buf_e[32];
    snprintf(buf_s, sizeof(buf_s), "key%04d", i * 20);
    snprintf(buf_e, sizeof(buf_e), "key%04d", i * 20 + 8);
    std::string s(buf_s);
    std::string e(buf_e);
    SequenceNumber seq = 11 + i; // 15, 16, 17, 18 (i=6 is key0120..key0128 @ seq 17)
    witness.RecordWrite(kMemtableGen, s, e, seq);
    amtv_state->AddTombstone(s, e, seq, bytewise_icmp_);
  }

  // 3. Write 8 more tombstones to seal into Run 1 (size 8, seq 19..26)
  for (int i = 8; i < 16; ++i) {
    char buf_s[32], buf_e[32];
    snprintf(buf_s, sizeof(buf_s), "key%04d", i * 20);
    snprintf(buf_e, sizeof(buf_e), "key%04d", i * 20 + 8);
    std::string s(buf_s);
    std::string e(buf_e);
    SequenceNumber seq = 11 + i; // 19..26
    witness.RecordWrite(kMemtableGen, s, e, seq);
    amtv_state->AddTombstone(s, e, seq, bytewise_icmp_);
  }

  auto witness_snap1_multiset = witness.ToMultiset();
  auto snap1 = amtv_state->GetSnapshot();
  ASSERT_NE(snap1, nullptr);
  ASSERT_FALSE(snap1->fallback_required) << "Witness fail-fast: AMTV Fallback occurred!";
  ASSERT_EQ(snap1->memtable_generation, witness.generation())
      << "Witness fail-fast: Generation changed!";

  // Tier 1 structural check on snap1 (pre-merge snapshot)
  auto snap1_multiset = ExtractSnapshotRawMultiset(*snap1);
  EXPECT_EQ(snap1_multiset, witness_snap1_multiset);
  EXPECT_EQ(snap1->sealed_runs.size(), 2U);

  // 4. Write 4 more into Open Delta (seq 27..30)
  for (int i = 16; i < 20; ++i) {
    char buf_s[32], buf_e[32];
    snprintf(buf_s, sizeof(buf_s), "key%04d", i * 20);
    snprintf(buf_e, sizeof(buf_e), "key%04d", i * 20 + 8);
    std::string s(buf_s);
    std::string e(buf_e);
    SequenceNumber seq = 11 + i; // 27..30
    witness.RecordWrite(kMemtableGen, s, e, seq);
    amtv_state->AddTombstone(s, e, seq, bytewise_icmp_);
  }

  auto witness_snap2_multiset = witness.ToMultiset();
  auto snap2 = amtv_state->GetSnapshot();
  ASSERT_NE(snap2, nullptr);
  ASSERT_FALSE(snap2->fallback_required) << "Witness fail-fast: AMTV Fallback occurred!";
  ASSERT_EQ(snap2->memtable_generation, witness.generation())
      << "Witness fail-fast: Generation changed!";

  // Tier 1 structural check on snap2
  EXPECT_EQ(ExtractSnapshotRawMultiset(*snap2), witness_snap2_multiset);
  EXPECT_EQ(snap2->open_delta->size(), 4U);

  // 5. Trigger background binary merge of Run 0 and Run 1
  bool merge_stable = amtv_state->WaitForMergeStable(10000000);
  ASSERT_TRUE(merge_stable) << "WaitForMergeStable timed out after 10 seconds! Merge thread hang.";

  auto snap3 = amtv_state->GetSnapshot();
  ASSERT_NE(snap3, nullptr);
  ASSERT_FALSE(snap3->fallback_required) << "Witness fail-fast: AMTV Fallback occurred!";
  ASSERT_EQ(snap3->memtable_generation, witness.generation())
      << "Witness fail-fast: Generation changed!";

  // Tier 1 structural check on snap3 (post-merge snapshot)
  auto snap3_multiset = ExtractSnapshotRawMultiset(*snap3);
  EXPECT_EQ(snap3_multiset, witness.ToMultiset());
  EXPECT_EQ(snap3->sealed_runs.size(), 1U); // Merged into 1 Run of size 16
  EXPECT_EQ(snap3->sealed_runs[0]->raw_entries.size(), 16U);

  // Concrete before/after merge witness parity verification
  fprintf(stderr, "\n=== [Phase A Witness Raw Audit Table (Tier-1 Multiset Parity)] ===\n");
  fprintf(stderr, "Note: Tier 1 does NOT filter by read_seq; verifies exact multiset parity of raw tombstones.\n");
  fprintf(stderr, "| generation_id | snapshot_id | phase               | raw_witness_count | snapshot_raw_count | multiset_match | fallback_count | merge_state    |\n");
  fprintf(stderr, "|:-------------:|:-----------:|:--------------------|:-----------------:|:------------------:|:--------------:|:--------------:|:---------------|\n");
  fprintf(stderr, "| %-13" PRIu64 " | snap1       | pre-merge           | %-17zu | %-18zu | %-14s | 0              | unmerged (2)   |\n",
          witness.generation(), witness_snap1_multiset.size(), snap1_multiset.size(), (snap1_multiset == witness_snap1_multiset ? "TRUE" : "FALSE"));
  fprintf(stderr, "| %-13" PRIu64 " | snap2       | open-delta-extended | %-17zu | %-18zu | %-14s | 0              | unmerged (2+1) |\n",
          witness.generation(), witness_snap2_multiset.size(), ExtractSnapshotRawMultiset(*snap2).size(), (ExtractSnapshotRawMultiset(*snap2) == witness_snap2_multiset ? "TRUE" : "FALSE"));
  fprintf(stderr, "| %-13" PRIu64 " | snap3       | post-merge          | %-17zu | %-18zu | %-14s | 0              | merged (1+1)   |\n",
          witness.generation(), witness.entries().size(), snap3_multiset.size(), (snap3_multiset == witness.ToMultiset() ? "TRUE" : "FALSE"));
  ASSERT_EQ(snap3_multiset, witness.ToMultiset());

  // Old snapshot snap1 must remain completely unaffected (immutable snapshot)
  EXPECT_EQ(snap1->sealed_runs.size(), 2U);
  EXPECT_EQ(ExtractSnapshotRawMultiset(*snap1).size(), 16U);

  // 6. Output sequence-ordered operations log
  witness.PrintOperationLog();

  // 7. Tier 2: MVCC Visibility Parity across multiple read sequences
  // Tier 2 validates MVCC visibility semantics filtered by read_seq.
  // Deconstructed truth table evaluates:
  // read_seq | point_value_seq | max_covering_tombstone_seq | tombstone_visible | point_deleted_by_tombstone | final_point_visible | expected_result | observed_result
  AMTVMultiSourceAdapter adapter3(snap3, &bytewise_icmp_);

  struct MVCCProbeCase {
    std::string probe_key;
    SequenceNumber read_seq;
    std::string test_phase;
  };

  std::vector<MVCCProbeCase> probe_cases = {
      {"key0004", 1, "pre-put (not-found)"},
      {"key0004", 5, "pre-delete (point-visible)"},
      {"key0004", 13, "delete-visible (deleted)"},
      {"key0004", 15, "put-resurrection (point-visible)"},
      {"key0124", 15, "future-delete-not-visible (point-visible)"},
      {"key0124", 20, "delete-visible (deleted)"},
      {"key0004", 100, "post-all (resurrection-visible)"},
      {"key0124", 100, "post-all (deleted)"},
  };

  fprintf(stderr, "\n=== [Phase A Witness MVCC Visibility Table (Tier-2 Semantic Parity)] ===\n");
  fprintf(stderr, "Note: Tier 2 validates MVCC visibility semantics filtered by read_seq across 8 canonical columns.\n");
  fprintf(stderr, "| probe_key | read_seq | point_value_seq | max_covering_tombstone_seq | tombstone_visible | point_deleted_by_tombstone | final_point_visible | expected_result | observed_result |\n");
  fprintf(stderr, "|:---------:|:--------:|:---------------:|:--------------------------:|:-----------------:|:--------------------------:|:-------------------:|:---------------:|:---------------:|\n");

  for (const auto& c : probe_cases) {
    SequenceNumber point_val_seq = 0;
    std::string point_val;
    bool has_point = witness.GetVisiblePoint(c.probe_key, c.read_seq, &point_val_seq, &point_val);

    SequenceNumber expected_covering_seq = witness.MaxCoveringTombstoneSeq(c.probe_key, c.read_seq);
    SequenceNumber observed_covering_seq = adapter3.MaxCoveringTombstoneSeqnum(c.probe_key, c.read_seq);
    EXPECT_EQ(observed_covering_seq, expected_covering_seq)
        << "Mismatch covering seq for " << c.probe_key << " at rseq=" << c.read_seq;

    bool exp_tombstone_vis = (expected_covering_seq > 0);
    bool obs_tombstone_vis = (observed_covering_seq > 0);
    EXPECT_EQ(obs_tombstone_vis, exp_tombstone_vis);

    bool exp_deleted = (exp_tombstone_vis && has_point && point_val_seq <= expected_covering_seq);
    bool obs_deleted = (obs_tombstone_vis && has_point && point_val_seq <= observed_covering_seq);
    EXPECT_EQ(obs_deleted, exp_deleted);

    bool exp_final_vis = (has_point && !exp_deleted);
    bool obs_final_vis = (has_point && !obs_deleted);
    EXPECT_EQ(obs_final_vis, exp_final_vis);

    std::string exp_result = exp_final_vis ? point_val : (exp_deleted ? "DELETED" : "NOT_FOUND");
    std::string obs_result = obs_final_vis ? point_val : (obs_deleted ? "DELETED" : "NOT_FOUND");
    EXPECT_EQ(obs_result, exp_result);

    fprintf(stderr, "| %-9s | %-8" PRIu64 " | %-15" PRIu64 " | %-26" PRIu64 " | %-17s | %-26s | %-19s | %-15s | %-15s |\n",
            c.probe_key.c_str(), static_cast<uint64_t>(c.read_seq),
            static_cast<uint64_t>(point_val_seq),
            static_cast<uint64_t>(observed_covering_seq),
            (obs_tombstone_vis ? "TRUE" : "FALSE"),
            (obs_deleted ? "TRUE" : "FALSE"),
            (obs_final_vis ? "TRUE" : "FALSE"),
            exp_result.c_str(), obs_result.c_str());
  }

  // Exhaustive 20-entry tombstone visibility checks across all probe points
  for (size_t i = 0; i < witness.entries().size(); ++i) {
    const auto& entry = witness.entries()[i];
    char buf_mid[32];
    snprintf(buf_mid, sizeof(buf_mid), "key%04d", static_cast<int>(i) * 20 + 4);
    std::string mid_probe(buf_mid);

    // At rseq = 5 (all tombstones seq >= 10, none visible)
    EXPECT_EQ(adapter3.MaxCoveringTombstoneSeqnum(mid_probe, 5), 0U);

    // At rseq = 13 (tombstones with seq <= 13 visible, > 13 not visible)
    if (entry.seq <= 13) {
      EXPECT_EQ(adapter3.MaxCoveringTombstoneSeqnum(mid_probe, 13), entry.seq);
    } else {
      EXPECT_EQ(adapter3.MaxCoveringTombstoneSeqnum(mid_probe, 13), 0U);
    }

    // At rseq = 15 (tombstones with seq <= 15 visible, > 15 not visible)
    if (entry.seq <= 15) {
      EXPECT_EQ(adapter3.MaxCoveringTombstoneSeqnum(mid_probe, 15), entry.seq);
    } else {
      EXPECT_EQ(adapter3.MaxCoveringTombstoneSeqnum(mid_probe, 15), 0U);
    }

    // At rseq = 100 (all tombstones visible)
    EXPECT_EQ(adapter3.MaxCoveringTombstoneSeqnum(mid_probe, 100), entry.seq);
  }

  // 8. Fail-fast demonstration on generation mismatch
  EXPECT_THROW(witness.RecordWrite(kMemtableGen + 1, "k", "k1", 200), std::runtime_error);

  amtv_state->CancelAndDrain();
}

// --------------------------------------------------------------------------
// Test: P1b12_SingleRunCandidateCountUpperBound
// Verifies that for every single Run query, candidate_count <= raw_entries.size()
// across varied window specifications, container reuse, and large run sizes.
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b12_SingleRunCandidateCountUpperBound) {
  // Test 1: Standard run with varied windows and container reuse
  {
    std::vector<OpenDeltaEntry> raw_entries;
    for (int i = 0; i < 32; ++i) {
      std::string s = "k" + std::to_string(i * 10);
      std::string e = "k" + std::to_string(i * 10 + 15);
      raw_entries.push_back(OpenDeltaEntry(s, e, 100 + i));
    }
    auto run = std::make_shared<AMTVRun>(1, raw_entries, bytewise_icmp_);

    struct TestWindow {
      std::optional<std::string> l;
      std::optional<std::string> u;
    };
    std::vector<TestWindow> test_windows = {
        {std::nullopt, std::nullopt},
        {std::nullopt, std::string("k100")},
        {std::string("k100"), std::nullopt},
        {std::string("k50"), std::string("k150")},
        {std::string("k0"), std::string("k400")},
        {std::string("k999"), std::string("k9999")},
        {std::string("k00"), std::string("k001")},
        {std::string("k200"), std::string("k100")},
    };

    // Verify with fresh container per query
    for (const auto& w : test_windows) {
      Slice sl, su;
      const Slice* pL = w.l.has_value() ? (sl = Slice(*w.l), &sl) : nullptr;
      const Slice* pU = w.u.has_value() ? (su = Slice(*w.u), &su) : nullptr;

      std::vector<size_t> indices;
      AMTVRunIntervalIndexAuditInfo audit;
      run->CollectIntersectingRawEntryIndices(pL, pU, bytewise_icmp_, &indices, &audit);

      EXPECT_LE(audit.candidate_count, run->raw_entries.size());
      EXPECT_EQ(indices.size(), audit.candidate_count);
    }

    // Verify container reuse: pass the same vector without clearing, verifying overwrite semantics
    std::vector<size_t> reused_indices;
    for (const auto& w : test_windows) {
      Slice sl, su;
      const Slice* pL = w.l.has_value() ? (sl = Slice(*w.l), &sl) : nullptr;
      const Slice* pU = w.u.has_value() ? (su = Slice(*w.u), &su) : nullptr;

      AMTVRunIntervalIndexAuditInfo audit;
      run->CollectIntersectingRawEntryIndices(pL, pU, bytewise_icmp_, &reused_indices, &audit);

      EXPECT_LE(audit.candidate_count, run->raw_entries.size());
      EXPECT_EQ(reused_indices.size(), audit.candidate_count);
    }
  }

  // Test 2: Large runs (128, 256, 512 entries)
  for (size_t run_sz : {128, 256, 512}) {
    std::vector<OpenDeltaEntry> raw_entries;
    raw_entries.reserve(run_sz);
    for (size_t i = 0; i < run_sz; ++i) {
      std::string s = "key_" + std::to_string(i * 10);
      std::string e = "key_" + std::to_string(i * 10 + 25);
      raw_entries.push_back(OpenDeltaEntry(s, e, 1000 + i));
    }
    auto run = std::make_shared<AMTVRun>(1, raw_entries, bytewise_icmp_);

    // Test 20 consecutive pseudo-random windows
    Random rnd(12345 + static_cast<int>(run_sz));
    for (int q = 0; q < 20; ++q) {
      int idx1 = rnd.Uniform(static_cast<int>(run_sz) * 10);
      int idx2 = rnd.Uniform(static_cast<int>(run_sz) * 10);
      std::string s1 = "key_" + std::to_string(std::min(idx1, idx2));
      std::string s2 = "key_" + std::to_string(std::max(idx1, idx2));
      Slice sl(s1), su(s2);

      std::vector<size_t> indices;
      AMTVRunIntervalIndexAuditInfo audit;
      run->CollectIntersectingRawEntryIndices(&sl, &su, bytewise_icmp_, &indices, &audit);

      EXPECT_LE(audit.candidate_count, run->raw_entries.size());
      EXPECT_EQ(indices.size(), audit.candidate_count);
    }
  }
}

// --------------------------------------------------------------------------
// Test: P1b12_EmptyUserKeyVsUnboundedBoundaryDistinction
// Verifies that empty user key Slice("") is distinct from unbounded (nullptr),
// covering all requested empty-key boundary topologies and empty-key tombstones.
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P1b12_EmptyUserKeyVsUnboundedBoundaryDistinction) {
  std::vector<OpenDeltaEntry> raw_entries;
  raw_entries.push_back(OpenDeltaEntry("", "b", 100));     // Entry 0: Starts at empty string ""
  raw_entries.push_back(OpenDeltaEntry("b", "d", 101));    // Entry 1: ["b", "d")
  raw_entries.push_back(OpenDeltaEntry("d", "f", 102));    // Entry 2: ["d", "f")

  auto run = std::make_shared<AMTVRun>(1, raw_entries, bytewise_icmp_);

  Slice empty_key("");
  Slice key_a("a");
  Slice key_b("b");
  Slice key_d("d");

  // 1. ["", "b"): lower bound is Slice("") -> non-empty valid interval.
  // Must match entry 0 ["", "b") because start="" < "b" and end="b" > "".
  {
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    run->CollectIntersectingRawEntryIndices(&empty_key, &key_b, bytewise_icmp_, &indices, &audit);
    EXPECT_EQ(indices.size(), 1U);
    EXPECT_EQ(indices[0], 0U);
  }

  // 2. ["a", ""): lower bound is "a", upper bound is Slice("").
  // Because "" < "a", interval is inverted/empty. Must strictly return 0 candidates.
  {
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    run->CollectIntersectingRawEntryIndices(&key_a, &empty_key, bytewise_icmp_, &indices, &audit);
    EXPECT_EQ(indices.size(), 0U);
    EXPECT_EQ(audit.candidate_count, 0U);
  }

  // 3. [nullopt, ""): unbounded lower bound, empty string as upper bound.
  // Because no valid user key satisfies key < "", interval is empty. Must return 0 candidates.
  {
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    run->CollectIntersectingRawEntryIndices(nullptr, &empty_key, bytewise_icmp_, &indices, &audit);
    EXPECT_EQ(indices.size(), 0U);
    EXPECT_EQ(audit.candidate_count, 0U);
  }

  // 4. [nullopt, nullopt): fully unbounded -> returns all 3 entries.
  {
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    run->CollectIntersectingRawEntryIndices(nullptr, nullptr, bytewise_icmp_, &indices, &audit);
    EXPECT_EQ(indices.size(), 3U);
  }

  // 5. Tombstone where actual start is "": entry 0 ["", "b").
  // Query [nullopt, "b"): matches entry 0.
  {
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    run->CollectIntersectingRawEntryIndices(nullptr, &key_b, bytewise_icmp_, &indices, &audit);
    EXPECT_EQ(indices.size(), 1U);
    EXPECT_EQ(indices[0], 0U);
  }

  // Query ["b", "d"): does NOT match entry 0 (end="b" <= lower="b").
  {
    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    run->CollectIntersectingRawEntryIndices(&key_b, &key_d, bytewise_icmp_, &indices, &audit);
    EXPECT_EQ(indices.size(), 1U);
    EXPECT_EQ(indices[0], 1U); // Only matches entry 1 ["b", "d")
  }

  // 6. Degenerate tombstone with empty range ["", "") or ["b", "")
  {
    std::vector<OpenDeltaEntry> degen_entries;
    degen_entries.push_back(OpenDeltaEntry("", "", 200));   // Empty range [0, 0)
    degen_entries.push_back(OpenDeltaEntry("b", "", 201));  // Inverted range [b, 0)
    auto degen_run = std::make_shared<AMTVRun>(1, degen_entries, bytewise_icmp_);

    std::vector<size_t> indices;
    AMTVRunIntervalIndexAuditInfo audit;
    // Querying with lower_bound != nullptr should never match degenerate entries
    degen_run->CollectIntersectingRawEntryIndices(&empty_key, &key_d, bytewise_icmp_, &indices, &audit);
    EXPECT_EQ(indices.size(), 0U);
  }
}

// ==========================================================================
// P2a-B0: Native Serialization Round-Trip and Byte-Level Layout Verification
// Validates exact byte layouts, InternalKey type (kTypeRangeDeletion = 0x0F),
// sequence number packing, value payload (end_key, NOT 8-byte seqnum),
// round-trip parsing via ParseInternalKey/RangeTombstone, and consumption
// through FragmentedRangeTombstoneList across 5 canonical groups.
// ==========================================================================
TEST_F(AMTVLocalScanReferenceTest, P2a_B0_NativeSerializationRoundTripVerification) {
  auto HexDump = [](const Slice& s) -> std::string {
    std::ostringstream oss;
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s.data());
    for (size_t i = 0; i < s.size(); ++i) {
      if (i > 0) oss << " ";
      oss << std::hex << std::setw(2) << std::setfill('0')
          << static_cast<unsigned int>(p[i]);
    }
    return oss.str();
  };

  std::cout << "\n====================================================================\n"
            << "P2a-B0 Native Range Tombstone Serialization Verification\n"
            << "====================================================================\n";

  // ------------------------------------------------------------------------
  // Group 1: Standard Tombstone [k10, k50) @ seq=100
  // ------------------------------------------------------------------------
  {
    std::cout << "\n--- Group 1: Standard Tombstone [k10, k50) @ seq=100 ---\n";
    RangeTombstone rt("k10", "k50", 100);
    auto kv = rt.Serialize();
    Slice key_slice = kv.first.Encode();
    Slice val_slice = kv.second;

    std::cout << "Key size: " << key_slice.size() << " bytes | Hex: " << HexDump(key_slice) << "\n";
    std::cout << "Val size: " << val_slice.size() << " bytes | Hex: " << HexDump(val_slice) << "\n";

    // 1. Layout verification
    ASSERT_EQ(key_slice.size(), 3U + 8U); // "k10" (3B) + 8B footer
    ASSERT_EQ(val_slice.size(), 3U);      // "k50" (3B) - USER KEY, NOT 8B sequence!
    ASSERT_EQ(ExtractUserKey(key_slice), "k10");
    ASSERT_EQ(val_slice, "k50");
    ASSERT_EQ(GetInternalKeySeqno(key_slice), 100U);
    ASSERT_EQ(ExtractValueType(key_slice), kTypeRangeDeletion);
    ASSERT_EQ(static_cast<uint8_t>(ExtractValueType(key_slice)), 0x0F);

    // 2. Round-trip deserialization
    ParsedInternalKey parsed;
    Status s = ParseInternalKey(key_slice, &parsed, false);
    ASSERT_TRUE(s.ok());
    EXPECT_EQ(parsed.user_key, "k10");
    EXPECT_EQ(parsed.sequence, 100U);
    EXPECT_EQ(parsed.type, kTypeRangeDeletion);

    RangeTombstone deserialized(parsed, val_slice);
    EXPECT_EQ(deserialized.start_key_, "k10");
    EXPECT_EQ(deserialized.end_key_, "k50");
    EXPECT_EQ(deserialized.seq_, 100U);

    // 3. Native fragmenter consumption
    std::vector<std::string> keys = {key_slice.ToString()};
    std::vector<std::string> values = {val_slice.ToString()};
    auto v_iter = std::make_unique<VectorIterator>(keys, values, &bytewise_icmp_);
    FragmentedRangeTombstoneList frag_list(std::move(v_iter), bytewise_icmp_);
    FragmentedRangeTombstoneIterator iter(&frag_list, bytewise_icmp_, kMaxSequenceNumber);

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.start_key(), "k10");
    EXPECT_EQ(iter.end_key(), "k50");
    EXPECT_EQ(iter.seq(), 100U);
    EXPECT_EQ(ExtractUserKey(iter.key()), "k10");
    EXPECT_EQ(iter.value(), "k50");
    EXPECT_EQ(GetInternalKeySeqno(iter.key()), 100U);
    iter.Next();
    EXPECT_FALSE(iter.Valid());
  }

  // ------------------------------------------------------------------------
  // Group 2: Same Start Key, Different Sequence Numbers [k10, k30)@150, [k10, k60)@200
  // ------------------------------------------------------------------------
  {
    std::cout << "\n--- Group 2: Same Start Key Different Sequence [k10, k30)@150, [k10, k60)@200 ---\n";
    RangeTombstone rt1("k10", "k30", 150);
    RangeTombstone rt2("k10", "k60", 200);

    auto kv1 = rt1.Serialize();
    auto kv2 = rt2.Serialize();

    std::cout << "Tombstone 1 (seq=150) Key Hex: " << HexDump(kv1.first.Encode())
              << " | Val Hex: " << HexDump(kv1.second) << "\n";
    std::cout << "Tombstone 2 (seq=200) Key Hex: " << HexDump(kv2.first.Encode())
              << " | Val Hex: " << HexDump(kv2.second) << "\n";

    // InternalKeyComparator ordering: higher seqno must sort BEFORE lower seqno
    EXPECT_LT(bytewise_icmp_.Compare(kv2.first.Encode(), kv1.first.Encode()), 0);

    // Round-trip parse both
    ParsedInternalKey p1, p2;
    ASSERT_TRUE(ParseInternalKey(kv1.first.Encode(), &p1, false).ok());
    ASSERT_TRUE(ParseInternalKey(kv2.first.Encode(), &p2, false).ok());
    RangeTombstone d1(p1, kv1.second);
    RangeTombstone d2(p2, kv2.second);
    EXPECT_EQ(d1.start_key_, "k10");
    EXPECT_EQ(d1.end_key_, "k30");
    EXPECT_EQ(d1.seq_, 150U);
    EXPECT_EQ(d2.start_key_, "k10");
    EXPECT_EQ(d2.end_key_, "k60");
    EXPECT_EQ(d2.seq_, 200U);

    // Pass in sorted order to FragmentedRangeTombstoneList
    std::vector<std::string> keys = {kv2.first.Encode().ToString(), kv1.first.Encode().ToString()};
    std::vector<std::string> values = {kv2.second.ToString(), kv1.second.ToString()};
    auto v_iter = std::make_unique<VectorIterator>(keys, values, &bytewise_icmp_);
    FragmentedRangeTombstoneList frag_list(std::move(v_iter), bytewise_icmp_);
    FragmentedRangeTombstoneIterator iter(&frag_list, bytewise_icmp_, kMaxSequenceNumber);

    // Fragment 1: [k10, k30) with top seq = 200
    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.start_key(), "k10");
    EXPECT_EQ(iter.end_key(), "k30");
    EXPECT_EQ(iter.seq(), 200U);

    // Fragment 2: [k30, k60) with top seq = 200
    iter.TopNext();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.start_key(), "k30");
    EXPECT_EQ(iter.end_key(), "k60");
    EXPECT_EQ(iter.seq(), 200U);

    iter.TopNext();
    EXPECT_FALSE(iter.Valid());
  }

  // ------------------------------------------------------------------------
  // Group 3: Overlapping Tombstones [k20, k70) @ 120, [k40, k90) @ 180
  // ------------------------------------------------------------------------
  {
    std::cout << "\n--- Group 3: Overlapping Tombstones [k20, k70)@120, [k40, k90)@180 ---\n";
    RangeTombstone rt1("k20", "k70", 120);
    RangeTombstone rt2("k40", "k90", 180);

    auto kv1 = rt1.Serialize();
    auto kv2 = rt2.Serialize();

    std::cout << "Overlap 1 Key: " << HexDump(kv1.first.Encode()) << " | Val: " << HexDump(kv1.second) << "\n";
    std::cout << "Overlap 2 Key: " << HexDump(kv2.first.Encode()) << " | Val: " << HexDump(kv2.second) << "\n";

    ParsedInternalKey p1, p2;
    ASSERT_TRUE(ParseInternalKey(kv1.first.Encode(), &p1, false).ok());
    ASSERT_TRUE(ParseInternalKey(kv2.first.Encode(), &p2, false).ok());
    RangeTombstone d1(p1, kv1.second);
    RangeTombstone d2(p2, kv2.second);
    EXPECT_EQ(d1.start_key_, "k20");
    EXPECT_EQ(d1.end_key_, "k70");
    EXPECT_EQ(d1.seq_, 120U);
    EXPECT_EQ(d2.start_key_, "k40");
    EXPECT_EQ(d2.end_key_, "k90");
    EXPECT_EQ(d2.seq_, 180U);

    std::vector<std::string> keys = {kv1.first.Encode().ToString(), kv2.first.Encode().ToString()};
    std::vector<std::string> values = {kv1.second.ToString(), kv2.second.ToString()};
    auto v_iter = std::make_unique<VectorIterator>(keys, values, &bytewise_icmp_);
    FragmentedRangeTombstoneList frag_list(std::move(v_iter), bytewise_icmp_);
    FragmentedRangeTombstoneIterator iter(&frag_list, bytewise_icmp_, kMaxSequenceNumber);

    // Fragment 1: [k20, k40) @ 120
    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.start_key(), "k20");
    EXPECT_EQ(iter.end_key(), "k40");
    EXPECT_EQ(iter.seq(), 120U);

    // Fragment 2: [k40, k70) @ 180 (top seq)
    iter.TopNext();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.start_key(), "k40");
    EXPECT_EQ(iter.end_key(), "k70");
    EXPECT_EQ(iter.seq(), 180U);

    // Fragment 3: [k70, k90) @ 180
    iter.TopNext();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.start_key(), "k70");
    EXPECT_EQ(iter.end_key(), "k90");
    EXPECT_EQ(iter.seq(), 180U);

    iter.TopNext();
    EXPECT_FALSE(iter.Valid());
  }

  // ------------------------------------------------------------------------
  // Group 4: Empty User Key Boundary ["", k25) @ seq=250
  // ------------------------------------------------------------------------
  {
    std::cout << "\n--- Group 4: Empty User Key Boundary [\"\", k25) @ seq=250 ---\n";
    RangeTombstone rt("", "k25", 250);
    auto kv = rt.Serialize();
    Slice key_slice = kv.first.Encode();
    Slice val_slice = kv.second;

    std::cout << "Empty-start Key size: " << key_slice.size() << " bytes | Hex: " << HexDump(key_slice) << "\n";
    std::cout << "Empty-start Val size: " << val_slice.size() << " bytes | Hex: " << HexDump(val_slice) << "\n";

    // Layout verification: start user key is empty, so key is EXACTLY 8 bytes!
    ASSERT_EQ(key_slice.size(), 8U);
    ASSERT_EQ(val_slice.size(), 3U);
    ASSERT_TRUE(ExtractUserKey(key_slice).empty());
    ASSERT_EQ(val_slice, "k25");
    ASSERT_EQ(GetInternalKeySeqno(key_slice), 250U);
    ASSERT_EQ(ExtractValueType(key_slice), kTypeRangeDeletion);

    // Round-trip deserialization
    ParsedInternalKey parsed;
    ASSERT_TRUE(ParseInternalKey(key_slice, &parsed, false).ok());
    EXPECT_TRUE(parsed.user_key.empty());
    EXPECT_EQ(parsed.sequence, 250U);
    EXPECT_EQ(parsed.type, kTypeRangeDeletion);

    RangeTombstone deserialized(parsed, val_slice);
    EXPECT_TRUE(deserialized.start_key_.empty());
    EXPECT_EQ(deserialized.end_key_, "k25");
    EXPECT_EQ(deserialized.seq_, 250U);

    // Native fragmenter consumption
    std::vector<std::string> keys = {key_slice.ToString()};
    std::vector<std::string> values = {val_slice.ToString()};
    auto v_iter = std::make_unique<VectorIterator>(keys, values, &bytewise_icmp_);
    FragmentedRangeTombstoneList frag_list(std::move(v_iter), bytewise_icmp_);
    FragmentedRangeTombstoneIterator iter(&frag_list, bytewise_icmp_, kMaxSequenceNumber);

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.start_key(), "");
    EXPECT_EQ(iter.end_key(), "k25");
    EXPECT_EQ(iter.seq(), 250U);
    EXPECT_TRUE(ExtractUserKey(iter.key()).empty());
    EXPECT_EQ(iter.value(), "k25");
    iter.Next();
    EXPECT_FALSE(iter.Valid());
  }

  // ------------------------------------------------------------------------
  // Group 5: User-Defined Timestamp (UDT) Tombstone [k30+ts50, k80+ts50) @ seq=300
  // ------------------------------------------------------------------------
  {
    std::cout << "\n--- Group 5: UDT Tombstone [k30+ts50, k80+ts50) @ seq=300 ---\n";
    const Comparator* ucmp = GetBytewiseComparatorWithU64Ts();
    ASSERT_NE(ucmp, nullptr);
    InternalKeyComparator ts_icmp(ucmp);
    const size_t ts_sz = ucmp->timestamp_size();
    ASSERT_EQ(ts_sz, sizeof(uint64_t));

    std::string ts50;
    PutFixed64(&ts50, 50);

    std::string sk_with_ts = "k30" + ts50;
    std::string ek_with_ts = "k80" + ts50;

    RangeTombstone rt(sk_with_ts, ek_with_ts, 300);
    auto kv = rt.Serialize();
    Slice key_slice = kv.first.Encode();
    Slice val_slice = kv.second;

    std::cout << "UDT Key size: " << key_slice.size() << " bytes | Hex: " << HexDump(key_slice) << "\n";
    std::cout << "UDT Val size: " << val_slice.size() << " bytes | Hex: " << HexDump(val_slice) << "\n";

    // Layout verification:
    // Key: "k30" (3B) + ts50 (8B) + 8B footer = 19 bytes
    // Val: "k80" (3B) + ts50 (8B) = 11 bytes (end key user string with timestamp)
    ASSERT_EQ(key_slice.size(), 3U + ts_sz + 8U);
    ASSERT_EQ(val_slice.size(), 3U + ts_sz);

    Slice extracted_uk = ExtractUserKey(key_slice);
    ASSERT_EQ(extracted_uk, sk_with_ts);
    ASSERT_EQ(StripTimestampFromUserKey(extracted_uk, ts_sz), "k30");
    ASSERT_EQ(ExtractTimestampFromUserKey(extracted_uk, ts_sz), ts50);

    ASSERT_EQ(StripTimestampFromUserKey(val_slice, ts_sz), "k80");
    ASSERT_EQ(ExtractTimestampFromUserKey(val_slice, ts_sz), ts50);

    ASSERT_EQ(GetInternalKeySeqno(key_slice), 300U);
    ASSERT_EQ(ExtractValueType(key_slice), kTypeRangeDeletion);

    // Round-trip deserialization
    ParsedInternalKey parsed;
    ASSERT_TRUE(ParseInternalKey(key_slice, &parsed, false).ok());
    EXPECT_EQ(parsed.user_key, sk_with_ts);
    EXPECT_EQ(parsed.sequence, 300U);
    EXPECT_EQ(parsed.type, kTypeRangeDeletion);

    RangeTombstone deserialized(parsed, val_slice);
    EXPECT_EQ(deserialized.start_key_, sk_with_ts);
    EXPECT_EQ(deserialized.end_key_, ek_with_ts);
    EXPECT_EQ(deserialized.seq_, 300U);

    // Native Fragmenter consumption with UDT comparator:
    // FragmentTombstones normalizes start and end keys in RangeTombstoneStack with max timestamp (0xFF...)
    std::vector<std::string> keys = {key_slice.ToString()};
    std::vector<std::string> values = {val_slice.ToString()};
    auto v_iter = std::make_unique<VectorIterator>(keys, values, &ts_icmp);
    FragmentedRangeTombstoneList frag_list(std::move(v_iter), ts_icmp);
    FragmentedRangeTombstoneIterator iter(&frag_list, ts_icmp, kMaxSequenceNumber);

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    std::string expected_kTsMax(ts_sz, static_cast<unsigned char>(0xff));
    EXPECT_EQ(iter.start_key(), "k30" + expected_kTsMax);
    EXPECT_EQ(iter.end_key(), "k80" + expected_kTsMax);
    EXPECT_EQ(iter.seq(), 300U);
    EXPECT_EQ(iter.timestamp(), ts50);
    std::cout << "FragmentedRangeTombstoneIterator start_key (normalized with max ts): "
              << HexDump(iter.start_key()) << "\n";
    std::cout << "FragmentedRangeTombstoneIterator end_key (normalized with max ts): "
              << HexDump(iter.end_key()) << "\n";
    std::cout << "FragmentedRangeTombstoneIterator extracted timestamp(): "
              << HexDump(iter.timestamp()) << "\n";

    iter.Next();
    EXPECT_FALSE(iter.Valid());
  }
  std::cout << "====================================================================\n\n";
}

// --------------------------------------------------------------------------
// P2a Test 1: WindowSpec and Boundary Distinction (nullopt vs "")
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_WindowSpecAndBoundaryDistinction) {
  const auto* ucmp = bytewise_icmp_.user_comparator();

  // 1. Boundary distinction
  WindowSpec unb(std::nullopt, std::nullopt);
  EXPECT_FALSE(unb.IsBounded());

  WindowSpec left_only("k10", std::nullopt);
  EXPECT_FALSE(left_only.IsBounded());

  WindowSpec right_only(std::nullopt, "k50");
  EXPECT_FALSE(right_only.IsBounded());

  // Empty string "" is a valid user key, NOT nullopt
  WindowSpec empty_start("", "k50");
  EXPECT_TRUE(empty_start.IsBounded());
  EXPECT_TRUE(empty_start.IsValidBounded(ucmp));

  WindowSpec inverted("k50", "k10");
  EXPECT_TRUE(inverted.IsBounded());
  EXPECT_FALSE(inverted.IsValidBounded(ucmp));

  WindowSpec degenerate("k30", "k30");
  EXPECT_TRUE(degenerate.IsBounded());
  EXPECT_FALSE(degenerate.IsValidBounded(ucmp));

  // 2. LocalRangeDelView behavior on unbounded vs empty window
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k40", 100), OpenDeltaEntry("k50", "k80", 120)},
  };
  std::vector<OpenDeltaEntry> delta = {OpenDeltaEntry("k30", "k60", 150)};

  // Unbounded: falls back to full candidates
  {
    LocalRangeDelView view(runs, delta, unb, bytewise_icmp_);
    EXPECT_TRUE(view.state()->fallback_to_full);
    EXPECT_FALSE(view.state()->is_bounded);
    EXPECT_EQ(view.state()->candidate_count, 3U);
  }

  // Inverted: produces empty window
  {
    LocalRangeDelView view(runs, delta, inverted, bytewise_icmp_);
    EXPECT_TRUE(view.state()->is_empty_window);
    EXPECT_EQ(view.state()->candidate_count, 0U);
    auto handle = view.CreateIteratorHandle(bytewise_icmp_, 200);
    handle->guard()->SeekToFirst();
    EXPECT_FALSE(handle->guard()->Valid());
    EXPECT_EQ(handle->ForwardStream().size(), 0U);
  }
}

// --------------------------------------------------------------------------
// P2a Test 2: WindowGuard Seek & SeekForPrev Boundary Contract & Rejection
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_WindowGuardSeekRejection) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k90", 100)},
  };
  std::vector<OpenDeltaEntry> delta;
  WindowSpec window("k20", "k70");
  LocalRangeDelView view(runs, delta, window, bytewise_icmp_);
  auto handle = view.CreateIteratorHandle(bytewise_icmp_, 200);
  auto* guard = handle->guard();

  // Forward Seek: allowed strictly L <= target < U
  // 1. target < L ("k10"): rejected
  guard->Seek("k10");
  EXPECT_FALSE(guard->Valid());
  EXPECT_TRUE(guard->status().IsInvalidArgument());

  // 2. target == L ("k20"): accepted
  guard->Seek("k20");
  EXPECT_TRUE(guard->status().ok());
  EXPECT_TRUE(guard->Valid());
  EXPECT_EQ(guard->start_key(), "k20");

  // 3. L < target < U ("k50"): accepted
  guard->Seek("k50");
  EXPECT_TRUE(guard->status().ok());
  EXPECT_TRUE(guard->Valid());

  // 4. target == U ("k70"): rejected for forward Seek
  guard->Seek("k70");
  EXPECT_FALSE(guard->Valid());
  EXPECT_TRUE(guard->status().IsInvalidArgument());

  // 5. target > U ("k80"): rejected
  guard->Seek("k80");
  EXPECT_FALSE(guard->Valid());
  EXPECT_TRUE(guard->status().IsInvalidArgument());

  // Reverse SeekForPrev: allowed strictly L <= target <= U
  // 1. target < L ("k10"): rejected
  guard->SeekForPrev("k10");
  EXPECT_FALSE(guard->Valid());
  EXPECT_TRUE(guard->status().IsInvalidArgument());

  // 2. target == L ("k20"): accepted
  guard->SeekForPrev("k20");
  EXPECT_TRUE(guard->status().ok());
  EXPECT_TRUE(guard->Valid());

  // 3. L < target < U ("k50"): accepted
  guard->SeekForPrev("k50");
  EXPECT_TRUE(guard->status().ok());
  EXPECT_TRUE(guard->Valid());

  // 4. target == U ("k70"): MUST BE ACCEPTED (RocksDB SeekToLast exclusive sentinel contract!)
  guard->SeekForPrev("k70");
  EXPECT_TRUE(guard->status().ok());
  EXPECT_TRUE(guard->Valid());
  EXPECT_EQ(guard->end_key(), "k70");

  // 5. target > U ("k80"): rejected
  guard->SeekForPrev("k80");
  EXPECT_FALSE(guard->Valid());
  EXPECT_TRUE(guard->status().IsInvalidArgument());
}

// --------------------------------------------------------------------------
// P2a Test 3: IteratorHandle Lifetime After View & Snapshot Destruction (Hard Test)
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_IteratorHandleLifetimeAfterViewAndSnapshotDestruction) {
  std::unique_ptr<LocalRangeDelIteratorHandle> handle;
  {
    std::vector<std::vector<OpenDeltaEntry>> runs = {
        {OpenDeltaEntry("k10", "k50", 100), OpenDeltaEntry("k60", "k90", 150)},
    };
    std::vector<OpenDeltaEntry> delta = {OpenDeltaEntry("k30", "k70", 200)};
    WindowSpec window("k20", "k80");
    LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
    handle = local_view.CreateIteratorHandle(bytewise_icmp_, 250);
    // local_view, runs, and delta go out of scope and are completely destroyed here!
  }

  ASSERT_NE(handle, nullptr);
  auto* guard = handle->guard();
  ASSERT_NE(guard, nullptr);

  // Traverse forward using the surviving handle
  guard->SeekToFirst();
  std::vector<ClippedTombstoneFragment> fwd_fragments;
  while (guard->Valid()) {
    fwd_fragments.push_back(guard->Fragment());
    guard->Next();
  }

  // Traverse backward using the surviving handle
  guard->SeekToLast();
  std::vector<ClippedTombstoneFragment> bwd_fragments;
  while (guard->Valid()) {
    bwd_fragments.push_back(guard->Fragment());
    guard->Prev();
  }

  // Verify non-empty and symmetric
  ASSERT_GT(fwd_fragments.size(), 0U);
  auto rev_bwd = bwd_fragments;
  std::reverse(rev_bwd.begin(), rev_bwd.end());
  EXPECT_EQ(fwd_fragments, rev_bwd);

  // Verify all fragments are within ["k20", "k80"]
  for (const auto& frag : fwd_fragments) {
    EXPECT_GE(frag.start_key, "k20");
    EXPECT_LE(frag.end_key, "k80");
  }
}

// --------------------------------------------------------------------------
// P2a Test 4: Equal Start Key, Different End Key & Sequence
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_EqualStartDifferentEndAndSequence) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k30", 150),
       OpenDeltaEntry("k10", "k60", 200),
       OpenDeltaEntry("k10", "k80", 100)},
  };
  std::vector<OpenDeltaEntry> delta;
  WindowSpec window("k10", "k70");

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto handle = local_view.CreateIteratorHandle(bytewise_icmp_, 300);

  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      OwnedRawRangeTombstone("k10", "k30", 150),
      OwnedRawRangeTombstone("k10", "k60", 200),
      OwnedRawRangeTombstone("k10", "k80", 100),
  };
  CanonicalRangeDelTruth canonical_truth(all_tombstones, window, bytewise_icmp_, 300);
  AMTVIndependentPointwiseOracle oracle(runs, delta);

  std::vector<std::string> probe_keys = {"k10", "k20", "k30", "k50", "k60", "k70"};
  std::vector<SequenceNumber> probe_seqs = {50, 120, 180, 250};

  VerifyFourWayDifferential(handle.get(), &canonical_truth, oracle, window,
                            probe_keys, probe_seqs, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// P2a Test 5: Long Tombstones Crossing Bounds, Nested, Adjacent & Spanning
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_LongTombstonesCrossingBounds_NestedAdjacentCrossing) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k05", "k95", 80),   // Fully spanning
       OpenDeltaEntry("k10", "k50", 100),  // Left-crossing
       OpenDeltaEntry("k20", "k30", 90),   // Adjacent outer left
       OpenDeltaEntry("k35", "k45", 140),  // Nested interior
       OpenDeltaEntry("k50", "k90", 120),  // Right-crossing
       OpenDeltaEntry("k70", "k80", 90)},  // Adjacent outer right
  };
  std::vector<OpenDeltaEntry> delta;
  WindowSpec window("k30", "k70");

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto handle = local_view.CreateIteratorHandle(bytewise_icmp_, 200);

  std::vector<OwnedRawRangeTombstone> all_tombstones;
  for (const auto& e : runs[0]) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  CanonicalRangeDelTruth canonical_truth(all_tombstones, window, bytewise_icmp_, 200);
  AMTVIndependentPointwiseOracle oracle(runs, delta);

  std::vector<std::string> probe_keys = {"k25", "k30", "k35", "k40", "k45", "k50", "k65", "k70", "k75"};
  std::vector<SequenceNumber> probe_seqs = {70, 95, 110, 130, 150};

  VerifyFourWayDifferential(handle.get(), &canonical_truth, oracle, window,
                            probe_keys, probe_seqs, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// P2a Test 6: Multi-Level Runs (L0, L1, L2, L3) and Open Delta Candidate Reconstruction
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_MultiLevelRuns_OpenDelta_L0_L1_L2_L3) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      // L0
      {OpenDeltaEntry("k100", "k250", 40), OpenDeltaEntry("k300", "k450", 45)},
      // L1
      {OpenDeltaEntry("k120", "k280", 50), OpenDeltaEntry("k320", "k480", 55)},
      // L2
      {OpenDeltaEntry("k050", "k200", 60), OpenDeltaEntry("k350", "k500", 65)},
      // L3
      {OpenDeltaEntry("k180", "k380", 70)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k220", "k340", 80),
  };
  WindowSpec window("k150", "k350");

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto handle = local_view.CreateIteratorHandle(bytewise_icmp_, 100);

  std::vector<OwnedRawRangeTombstone> all_tombstones;
  for (const auto& run : runs) {
    for (const auto& e : run) {
      all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
    }
  }
  for (const auto& e : delta) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  CanonicalRangeDelTruth canonical_truth(all_tombstones, window, bytewise_icmp_, 100);
  AMTVIndependentPointwiseOracle oracle(runs, delta);

  std::vector<std::string> probe_keys = {"k150", "k180", "k220", "k250", "k280", "k300", "k330", "k350"};
  std::vector<SequenceNumber> probe_seqs = {35, 45, 55, 65, 75, 85};

  VerifyFourWayDifferential(handle.get(), &canonical_truth, oracle, window,
                            probe_keys, probe_seqs, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// P2a Test 7: Put Resurrection & Future Tombstone MVCC Filtering at Iterator Layer
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_PutResurrectionAndFutureTombstoneMVCC) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k50", 10),
       OpenDeltaEntry("k20", "k60", 50),
       OpenDeltaEntry("k30", "k70", 100),
       OpenDeltaEntry("k40", "k80", 200)},
  };
  std::vector<OpenDeltaEntry> delta;
  WindowSpec window("k15", "k75");
  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);

  std::vector<OwnedRawRangeTombstone> all_tombstones;
  for (const auto& e : runs[0]) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  std::vector<std::string> probe_keys = {"k15", "k25", "k35", "k45", "k55", "k65", "k75"};
  std::vector<SequenceNumber> probe_seqs = {5, 15, 60, 120};

  // Test multiple read_seqs using the same immutable local_view State
  std::vector<SequenceNumber> read_seqs = {5, 20, 80, 150, 300};
  for (SequenceNumber rseq : read_seqs) {
    auto handle = local_view.CreateIteratorHandle(bytewise_icmp_, rseq);
    CanonicalRangeDelTruth canonical_truth(all_tombstones, window, bytewise_icmp_, rseq);
    AMTVIndependentPointwiseOracle oracle(runs, delta, BytewiseComparator(), rseq);
    VerifyFourWayDifferential(handle.get(), &canonical_truth, oracle, window,
                              probe_keys, probe_seqs, bytewise_icmp_);
  }
}

// --------------------------------------------------------------------------
// P2a Test 8: User-Defined Timestamp (UDT) with and without ts_upper_bound
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_UserDefinedTimestamp_WithAndWithoutTsUpperBound) {
  const Comparator* ucmp = GetBytewiseComparatorWithU64Ts();
  ASSERT_NE(ucmp, nullptr);
  InternalKeyComparator ts_icmp(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();
  std::string dummy_ts(ts_sz, '\0');

  std::string ts100, ts200, ts300, ts250;
  PutFixed64(&ts100, 100);
  PutFixed64(&ts200, 200);
  PutFixed64(&ts300, 300);
  PutFixed64(&ts250, 250);

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10" + ts100, "k60" + ts100, 50)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30" + ts200, "k80" + ts200, 80),
      OpenDeltaEntry("k50" + ts300, "k90" + ts300, 120),
  };
  WindowSpec window("k20", "k70");

  LocalRangeDelView local_view(runs, delta, window, ts_icmp);
  std::vector<OwnedRawRangeTombstone> all_tombstones;
  for (const auto& e : runs[0]) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  for (const auto& e : delta) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }

  std::vector<std::string> probe_keys = {
      "k20" + dummy_ts, "k35" + dummy_ts, "k55" + dummy_ts,
      "k65" + dummy_ts, "k70" + dummy_ts
  };
  std::vector<SequenceNumber> probe_seqs = {40, 70, 100};

  // Case 8a: Without ts_upper_bound (all visible)
  {
    AMTVIndependentPointwiseOracle oracle_8a(runs, delta, ucmp, 200, nullptr);
    auto handle = local_view.CreateIteratorHandle(ts_icmp, 200, nullptr);
    CanonicalRangeDelTruth canonical_truth(all_tombstones, window, ts_icmp, 200, nullptr);
    VerifyFourWayDifferential(handle.get(), &canonical_truth, oracle_8a, window,
                              probe_keys, probe_seqs, ts_icmp, nullptr);
  }

  // Case 8b: With ts_upper_bound = ts250 (ts300 tombstone filtered)
  {
    Slice ts_bound_slice(ts250);
    AMTVIndependentPointwiseOracle oracle_8b(runs, delta, ucmp, 200, &ts_bound_slice);
    auto handle = local_view.CreateIteratorHandle(ts_icmp, 200, &ts_bound_slice);
    CanonicalRangeDelTruth canonical_truth(all_tombstones, window, ts_icmp, 200, &ts_bound_slice);
    VerifyFourWayDifferential(handle.get(), &canonical_truth, oracle_8b, window,
                              probe_keys, probe_seqs, ts_icmp, &ts_bound_slice);
  }
}

// --------------------------------------------------------------------------
// P2a Test 9: Empty User Key as Valid Boundary ("")
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_EmptyUserKeyAsValidBoundary) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("", "k30", 200), OpenDeltaEntry("k20", "k60", 150)},
  };
  std::vector<OpenDeltaEntry> delta;
  WindowSpec window("", "k50");

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto handle = local_view.CreateIteratorHandle(bytewise_icmp_, 250);

  std::vector<OwnedRawRangeTombstone> all_tombstones;
  for (const auto& e : runs[0]) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  CanonicalRangeDelTruth canonical_truth(all_tombstones, window, bytewise_icmp_, 250);
  AMTVIndependentPointwiseOracle oracle(runs, delta);

  std::vector<std::string> probe_keys = {"", "k10", "k25", "k40", "k50"};
  std::vector<SequenceNumber> probe_seqs = {100, 180, 220};

  VerifyFourWayDifferential(handle.get(), &canonical_truth, oracle, window,
                            probe_keys, probe_seqs, bytewise_icmp_);
}

// --------------------------------------------------------------------------
// P2a Test 10: Old vs New Snapshot Equivalence Across Background Merge
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, P2a_OldVsNewSnapshotEquivalence) {
  // Old snapshot: 2 sealed runs + open delta
  std::vector<OpenDeltaEntry> run0_entries = {
      OpenDeltaEntry("k00", "k20", 10),
      OpenDeltaEntry("k30", "k50", 20),
  };
  std::vector<OpenDeltaEntry> run1_entries = {
      OpenDeltaEntry("k10", "k40", 30),
      OpenDeltaEntry("k60", "k80", 40),
  };
  std::vector<OpenDeltaEntry> delta_entries = {
      OpenDeltaEntry("k25", "k70", 50),
  };

  auto old_run0 = std::make_shared<AMTVRun>(1, run0_entries, bytewise_icmp_);
  auto old_run1 = std::make_shared<AMTVRun>(2, run1_entries, bytewise_icmp_);
  std::vector<std::shared_ptr<const AMTVRun>> old_sealed = {old_run0, old_run1};

  // Perform background merge of run0 + run1 into a single run
  std::vector<OpenDeltaEntry> merged_entries;
  for (const auto& e : run0_entries) merged_entries.push_back(e);
  for (const auto& e : run1_entries) merged_entries.push_back(e);
  std::sort(merged_entries.begin(), merged_entries.end(),
            [this](const OpenDeltaEntry& a, const OpenDeltaEntry& b) {
              return bytewise_icmp_.Compare(a.ikey.Encode(), b.ikey.Encode()) < 0;
            });
  auto new_run = std::make_shared<AMTVRun>(3, merged_entries, bytewise_icmp_);
  std::vector<std::shared_ptr<const AMTVRun>> new_sealed = {new_run};

  // All tombstones for canonical truth
  std::vector<OwnedRawRangeTombstone> all_tombstones;
  for (const auto& e : merged_entries) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  for (const auto& e : delta_entries) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }

  std::vector<std::vector<OpenDeltaEntry>> oracle_runs = {run0_entries, run1_entries};
  AMTVIndependentPointwiseOracle oracle(oracle_runs, delta_entries);

  // Test across multiple window topologies
  std::vector<WindowSpec> test_windows = {
      WindowSpec("k15", "k65"),  // Interior window
      WindowSpec("k00", "k90"),  // Full window
      WindowSpec("k35", "k55"),  // Narrow window
  };

  std::vector<std::string> probe_keys = {"k05", "k15", "k25", "k35", "k45", "k55", "k65", "k75"};
  std::vector<SequenceNumber> probe_seqs = {15, 25, 35, 45, 55};

  for (const auto& win : test_windows) {
    LocalRangeDelView old_view(old_sealed, delta_entries, win, bytewise_icmp_);
    LocalRangeDelView new_view(new_sealed, delta_entries, win, bytewise_icmp_);
    CanonicalRangeDelTruth canonical(all_tombstones, win, bytewise_icmp_, 100);

    auto old_handle = old_view.CreateIteratorHandle(bytewise_icmp_, 100);
    auto new_handle = new_view.CreateIteratorHandle(bytewise_icmp_, 100);

    auto old_stream = old_handle->ForwardStream();
    auto new_stream = new_handle->ForwardStream();
    auto can_stream = canonical.ForwardStream();

    ASSERT_EQ(old_stream.size(), new_stream.size());
    ASSERT_EQ(old_stream.size(), can_stream.size());
    for (size_t i = 0; i < old_stream.size(); ++i) {
      EXPECT_EQ(old_stream[i], new_stream[i]);
      EXPECT_EQ(old_stream[i], can_stream[i]);
    }

    VerifyFourWayDifferential(old_handle.get(), &canonical, oracle, win,
                              probe_keys, probe_seqs, bytewise_icmp_);
    VerifyFourWayDifferential(new_handle.get(), &canonical, oracle, win,
                              probe_keys, probe_seqs, bytewise_icmp_);
  }
}

// ==========================================================================
// P2b Baseline: Pure Native TruncatedRangeDelIterator Contract Verification
// ==========================================================================
TEST_F(AMTVLocalScanReferenceTest, P2b_NativeTruncatedRangeDelIteratorBaseline) {
  // Construct raw tombstones:
  // [k10, k50) @ seq=100
  // [k35, k65) @ seq=140
  // [k50, k90) @ seq=120
  std::vector<OwnedRawRangeTombstone> raw_tombstones = {
      {"k10", "k50", 100},
      {"k35", "k65", 140},
      {"k50", "k90", 120},
  };

  std::vector<std::string> keys, values;
  for (const auto& t : raw_tombstones) {
    auto kv = t.Serialize();
    keys.push_back(kv.first.Encode().ToString());
    values.push_back(kv.second.ToString());
  }
  auto v_iter = std::make_unique<VectorIterator>(keys, values, &bytewise_icmp_);
  auto frag_list = std::make_shared<FragmentedRangeTombstoneList>(
      std::move(v_iter), bytewise_icmp_);

  // Window [L, U) = [k30, k70)
  std::string L_key = "k30";
  std::string U_key = "k70";
  InternalKey smallest_ikey(L_key, kMaxSequenceNumber, kTypeRangeDeletion);
  InternalKey largest_ikey(U_key, kMaxSequenceNumber, kTypeRangeDeletion);

  auto make_native_trunc_iter = [&]() {
    auto frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
        frag_list, bytewise_icmp_, kMaxSequenceNumber);
    return std::make_unique<TruncatedRangeDelIterator>(
        std::move(frag_iter), &bytewise_icmp_, &smallest_ikey, &largest_ikey);
  };

  struct OperationRecord {
    std::string operation;
    std::string target;
    bool success;
    bool valid;
    std::string start_key;
    std::string end_key;
    SequenceNumber seq;
    std::string notes;
  };

  std::vector<OperationRecord> records;

  auto record_state = [&](const std::string& op, const std::string& target,
                          TruncatedRangeDelIterator* iter, const std::string& note = "") {
    OperationRecord r;
    r.operation = op;
    r.target = target;
    r.success = true;  // no throw or segfault
    r.valid = iter->Valid();
    if (r.valid) {
      r.start_key = iter->start_key().user_key.ToString();
      r.end_key = iter->end_key().user_key.ToString();
      r.seq = iter->seq();
    } else {
      r.start_key = "N/A";
      r.end_key = "N/A";
      r.seq = 0;
    }
    r.notes = note;
    records.push_back(r);
  };

  // 1. Forward Seek operations
  // 1a. Seek(L - eps) = "k20"
  {
    auto it = make_native_trunc_iter();
    it->Seek("k20");
    record_state("Seek", "k20 (L-eps)", it.get(),
                 "Target < L clamped by native iter to smallest_->user_key ('k30'); lands on covering [k30, k35)");
  }
  // 1b. Seek(L) = "k30"
  {
    auto it = make_native_trunc_iter();
    it->Seek("k30");
    record_state("Seek", "k30 (L)", it.get(),
                 "Exact L boundary; lands on [k30, k35)");
  }
  // 1c. Seek(mid) = "k40"
  {
    auto it = make_native_trunc_iter();
    it->Seek("k40");
    record_state("Seek", "k40 (mid)", it.get(),
                 "Interior key; lands on [k35, k45)");
  }
  // 1d. Seek(U) = "k70"
  {
    auto it = make_native_trunc_iter();
    it->Seek("k70");
    record_state("Seek", "k70 (U)", it.get(),
                 "Target == U; rejected by largest_ check; Valid() is FALSE");
  }
  // 1e. Seek(U + eps) = "k80"
  {
    auto it = make_native_trunc_iter();
    it->Seek("k80");
    record_state("Seek", "k80 (U+eps)", it.get(),
                 "Target > U; invalidated immediately; Valid() is FALSE");
  }

  // 2. Reverse SeekForPrev operations
  // 2a. SeekForPrev(U + eps) = "k80"
  {
    auto it = make_native_trunc_iter();
    it->SeekForPrev("k80");
    record_state("SeekForPrev", "k80 (U+eps)", it.get(),
                 "Target > U clamped to largest_->user_key ('k70'); lands on last in-window [k50, k70)");
  }
  // 2b. SeekForPrev(U) = "k70"
  {
    auto it = make_native_trunc_iter();
    it->SeekForPrev("k70");
    record_state("SeekForPrev", "k70 (U)", it.get(),
                 "Target == U; seeks tombstone with start <= U; lands on [k50, k70)");
  }
  // 2c. SeekForPrev(mid) = "k40"
  {
    auto it = make_native_trunc_iter();
    it->SeekForPrev("k40");
    record_state("SeekForPrev", "k40 (mid)", it.get(),
                 "Interior key; lands on [k35, k45)");
  }
  // 2d. SeekForPrev(L) = "k30"
  {
    auto it = make_native_trunc_iter();
    it->SeekForPrev("k30");
    record_state("SeekForPrev", "k30 (L)", it.get(),
                 "Target == L; lands on [k30, k35)");
  }
  // 2e. SeekForPrev(L - eps) = "k20"
  {
    auto it = make_native_trunc_iter();
    it->SeekForPrev("k20");
    record_state("SeekForPrev", "k20 (L-eps)", it.get(),
                 "Target < L; strictly invalidated by smallest_ check; Valid() is FALSE");
  }

  // 3. Traversal operations
  // 3a. SeekToFirst
  {
    auto it = make_native_trunc_iter();
    it->SeekToFirst();
    record_state("SeekToFirst", "-", it.get(), "First in-window fragment [k30, k35)");
    // 3b. Next sequential
    int step = 1;
    while (it->Valid()) {
      it->Next();
      record_state("Next (step " + std::to_string(step++) + ")", "-", it.get(),
                   it->Valid() ? "Next in-window fragment" : "Exhausted right boundary U");
    }
  }

  // 3c. SeekToLast
  {
    auto it = make_native_trunc_iter();
    it->SeekToLast();
    record_state("SeekToLast", "-", it.get(), "Last in-window fragment [k50, k70)");
    // 3d. Prev sequential
    int step = 1;
    while (it->Valid()) {
      it->Prev();
      record_state("Prev (step " + std::to_string(step++) + ")", "-", it.get(),
                   it->Valid() ? "Prev in-window fragment" : "Exhausted left boundary L");
    }
  }

  // Print formatted markdown table for audit document
  std::cout << "\n=== [Native TruncatedRangeDelIterator Baseline Audit Table] ===\n\n";
  std::cout << "| Operation | Target Key | Call Success | Valid() | status() | start_key | end_key | seq | Native Behavior / Notes |\n";
  std::cout << "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---|\n";
  for (const auto& r : records) {
    std::cout << "| `" << r.operation << "` | `" << r.target << "` | "
              << (r.success ? "YES" : "NO") << " | "
              << (r.valid ? "**TRUE**" : "FALSE") << " | `Status::OK()` | `"
              << r.start_key << "` | `" << r.end_key << "` | "
              << r.seq << " | " << r.notes << " |\n";
  }
  std::cout << "\n===============================================================\n\n";
}

// ==========================================================================
// M4-P2b: MergingIterator Consumer Compatibility Verification Harness & Tests
// ==========================================================================

// 1. Predicate & Safety Eligibility Types (Requirement 7)
enum class ScanTargetType {
  kActiveMemTable,
  kImmutableMemTable,
  kSSTLevel,
};

struct ScanEligibilityContext {
  ScanTargetType target_type = ScanTargetType::kActiveMemTable;
  bool amtv_enabled = true;
  std::optional<std::string> lower_bound;
  std::optional<std::string> upper_bound;
  const Comparator* ucmp = nullptr;
};

inline bool IsEligibleForLocalRangeDelView(const ScanEligibilityContext& ctx) {
  // 1. Target must strictly be active mutable memtable
  if (ctx.target_type != ScanTargetType::kActiveMemTable) {
    return false;
  }
  // 2. Column Family must have AMTV enabled
  if (!ctx.amtv_enabled) {
    return false;
  }
  // 3. Both bounds must be explicitly specified (non-nullopt)
  if (!ctx.lower_bound.has_value() || !ctx.upper_bound.has_value()) {
    return false;
  }
  // 4. lower < upper (non-empty, valid interval)
  if (ctx.ucmp == nullptr) {
    return false;
  }
  const bool has_ts = (ctx.ucmp->timestamp_size() > 0);
  if (ctx.ucmp->CompareWithoutTimestamp(
          *ctx.lower_bound, has_ts, *ctx.upper_bound, has_ts) >= 0) {
    return false;
  }
  return true;
}

// 2. CanonicalTombstoneContext
// Owns full range tombstones, sorted and fragmented via native Fragmenter,
// and manages smallest/largest internal keys for native TruncatedRangeDelIterator.
// Uses aliased shared_ptr so that the State is kept alive for the lifetime
// of any TruncatedRangeDelIterator created from it.
struct CanonicalTombstoneContext {
  struct State {
    std::shared_ptr<FragmentedRangeTombstoneList> fragmented_list;
    std::unique_ptr<InternalKey> smallest_ikey;
    std::unique_ptr<InternalKey> largest_ikey;
  };
  std::shared_ptr<State> state_;

  static CanonicalTombstoneContext Create(
      const std::vector<OwnedRawRangeTombstone>& all_tombstones,
      const WindowSpec& window,
      const InternalKeyComparator& icmp) {
    CanonicalTombstoneContext ctx;
    ctx.state_ = std::make_shared<State>();
    auto sorted = all_tombstones;
    const auto* ucmp = icmp.user_comparator();
    const bool has_ts = (ucmp->timestamp_size() > 0);
    std::sort(sorted.begin(), sorted.end(),
              [&](const OwnedRawRangeTombstone& a, const OwnedRawRangeTombstone& b) {
                int c = ucmp->CompareWithoutTimestamp(a.start_key, has_ts, b.start_key, has_ts);
                if (c != 0) return c < 0;
                return a.seq > b.seq;
              });

    std::vector<std::string> keys, values;
    for (const auto& t : sorted) {
      auto kv = t.Serialize();
      keys.push_back(kv.first.Encode().ToString());
      values.push_back(kv.second.ToString());
    }

    auto v_iter = std::make_unique<VectorIterator>(std::move(keys), std::move(values), &icmp);
    ctx.state_->fragmented_list = std::make_shared<FragmentedRangeTombstoneList>(std::move(v_iter), icmp);

    if (window.IsBounded() && window.IsValidBounded(ucmp)) {
      const size_t ts_sz = ucmp->timestamp_size();
      std::string smallest_buf = *window.lower_bound;
      std::string largest_buf = *window.upper_bound;
      if (ts_sz > 0) {
        if (smallest_buf.size() < ts_sz) {
          smallest_buf.append(std::string(ts_sz, '\xff'));
        }
        if (largest_buf.size() < ts_sz) {
          largest_buf.append(std::string(ts_sz, '\xff'));
        }
      }
      ctx.state_->smallest_ikey = std::make_unique<InternalKey>(
          smallest_buf, kMaxSequenceNumber, kTypeRangeDeletion);
      ctx.state_->largest_ikey = std::make_unique<InternalKey>(
          largest_buf, kMaxSequenceNumber, kTypeRangeDeletion);
    }
    return ctx;
  }

  std::unique_ptr<TruncatedRangeDelIterator> CreateNativeTruncatedRangeDelIterator(
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq = kMaxSequenceNumber,
      const Slice* ts_upper_bound = nullptr,
      bool force_unbounded = false) const {
    assert(state_);
    std::shared_ptr<FragmentedRangeTombstoneList> aliased_list(
        state_, state_->fragmented_list.get());
    auto frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
        aliased_list, icmp, read_seq, ts_upper_bound);
    if (!force_unbounded && state_->smallest_ikey && state_->largest_ikey) {
      return std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp, state_->smallest_ikey.get(), state_->largest_ikey.get());
    } else {
      return std::make_unique<TruncatedRangeDelIterator>(
          std::move(frag_iter), &icmp, nullptr, nullptr);
    }
  }
};

// 3. MergingIteratorCompatibilityHarness
// Compares Group A (Native-full) and Group B (AMTV-local) in real MergingIterator.
class MergingIteratorCompatibilityHarness {
 public:
  struct PointEntry {
    std::string user_key;
    SequenceNumber seq = 0;
    ValueType type = kTypeValue;
    std::string value;

    PointEntry(std::string ukey, SequenceNumber s, ValueType t = kTypeValue, std::string v = "")
        : user_key(std::move(ukey)), seq(s), type(t), value(std::move(v)) {
      if (value.empty()) {
        value = "val_" + user_key;
      }
    }
  };

  struct ComparisonStepRecord {
    std::string op;
    std::string target_desc;
    bool valid = false;
    Status status;
    std::string key;
    std::string value;
    SequenceNumber seq = 0;
    std::string timestamp;
  };

  static void RunDifferentialComparison(
      const std::vector<PointEntry>& points,
      const CanonicalTombstoneContext& canonical_ctx,
      const LocalRangeDelView& local_view,
      const WindowSpec& /*window*/,
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq,
      const std::vector<std::string>& probe_targets,
      const Slice* iterate_upper_bound = nullptr,
      const Slice* ts_upper_bound = nullptr,
      std::vector<ComparisonStepRecord>* out_trace = nullptr) {
    const auto* ucmp = icmp.user_comparator();

    // 1. Sort point entries according to icmp
    std::vector<std::pair<std::string, std::string>> sorted_points;
    for (const auto& p : points) {
      InternalKey ikey(p.user_key, p.seq, p.type);
      sorted_points.emplace_back(ikey.Encode().ToString(), p.value);
    }
    std::sort(sorted_points.begin(), sorted_points.end(),
              [&](const auto& a, const auto& b) {
                return icmp.Compare(a.first, b.first) < 0;
              });

    std::vector<std::string> pt_keys_a, pt_vals_a;
    std::vector<std::string> pt_keys_b, pt_vals_b;
    for (const auto& kv : sorted_points) {
      pt_keys_a.push_back(kv.first);
      pt_vals_a.push_back(kv.second);
      pt_keys_b.push_back(kv.first);
      pt_vals_b.push_back(kv.second);
    }

    // 2. Build Group A (Native-full) MergingIterator
    Arena arena_a;
    auto point_iter_a = new (arena_a.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_keys_a), std::move(pt_vals_a), &icmp);
    auto trunc_iter_a = canonical_ctx.CreateNativeTruncatedRangeDelIterator(
        icmp, read_seq, ts_upper_bound);

    MergeIteratorBuilder builder_a(&icmp, &arena_a, false /* prefix_seek */,
                                   iterate_upper_bound);
    builder_a.AddPointAndTombstoneIterator(point_iter_a, std::move(trunc_iter_a));
    InternalIterator* merge_iter_a = builder_a.Finish();
    if (read_seq < kMaxSequenceNumber) {
      merge_iter_a->SetRangeDelReadSeqno(read_seq);
    }

    // 3. Build Group B (AMTV-local) MergingIterator
    Arena arena_b;
    auto point_iter_b = new (arena_b.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_keys_b), std::move(pt_vals_b), &icmp);
    auto trunc_iter_b = local_view.CreateNativeTruncatedRangeDelIterator(
        icmp, read_seq, ts_upper_bound);

    MergeIteratorBuilder builder_b(&icmp, &arena_b, false /* prefix_seek */,
                                   iterate_upper_bound);
    builder_b.AddPointAndTombstoneIterator(point_iter_b, std::move(trunc_iter_b));
    InternalIterator* merge_iter_b = builder_b.Finish();
    if (read_seq < kMaxSequenceNumber) {
      merge_iter_b->SetRangeDelReadSeqno(read_seq);
    }

    // 4. Differential Verification Routine
    auto assert_match = [&](const std::string& op, const std::string& target_desc) {
      ASSERT_EQ(merge_iter_a->Valid(), merge_iter_b->Valid())
          << "Valid mismatch in op '" << op << "' with target '" << target_desc
          << "': Native=" << merge_iter_a->Valid() << ", AMTV=" << merge_iter_b->Valid();
      ASSERT_EQ(merge_iter_a->status().code(), merge_iter_b->status().code())
          << "Status mismatch in op '" << op << "' with target '" << target_desc << "'";

      if (merge_iter_a->Valid()) {
        ASSERT_EQ(merge_iter_a->key().ToString(true), merge_iter_b->key().ToString(true))
            << "Key mismatch in op '" << op << "' with target '" << target_desc << "'";
        ASSERT_EQ(merge_iter_a->value().ToString(true), merge_iter_b->value().ToString(true))
            << "Value mismatch in op '" << op << "' with target '" << target_desc << "'";

        ParsedInternalKey pik_a, pik_b;
        ParseInternalKey(merge_iter_a->key(), &pik_a, false).PermitUncheckedError();
        ParseInternalKey(merge_iter_b->key(), &pik_b, false).PermitUncheckedError();
        ASSERT_EQ(pik_a.sequence, pik_b.sequence)
            << "Seq mismatch in op '" << op << "' with target '" << target_desc << "'";
        ASSERT_EQ(pik_a.type, pik_b.type)
            << "Type mismatch in op '" << op << "' with target '" << target_desc << "'";

        const size_t ts_sz = ucmp->timestamp_size();
        if (ts_sz > 0 && pik_a.user_key.size() >= ts_sz) {
          Slice ts_a = ExtractTimestampFromUserKey(pik_a.user_key, ts_sz);
          Slice ts_b = ExtractTimestampFromUserKey(pik_b.user_key, ts_sz);
          ASSERT_EQ(ts_a.ToString(true), ts_b.ToString(true))
              << "Timestamp mismatch in op '" << op << "' with target '" << target_desc << "'";
        }

        if (out_trace) {
          ComparisonStepRecord rec;
          rec.op = op;
          rec.target_desc = target_desc;
          rec.valid = true;
          rec.status = merge_iter_a->status();
          rec.key = merge_iter_a->key().ToString(true);
          rec.value = merge_iter_a->value().ToString(true);
          rec.seq = pik_a.sequence;
          if (ts_sz > 0 && pik_a.user_key.size() >= ts_sz) {
            rec.timestamp = ExtractTimestampFromUserKey(pik_a.user_key, ts_sz).ToString(true);
          }
          out_trace->push_back(std::move(rec));
        }
      } else {
        if (out_trace) {
          ComparisonStepRecord rec;
          rec.op = op;
          rec.target_desc = target_desc;
          rec.valid = false;
          rec.status = merge_iter_a->status();
          out_trace->push_back(std::move(rec));
        }
      }
    };

    // 4a. Forward Scan: SeekToFirst() followed by sequential Next()
    merge_iter_a->SeekToFirst();
    merge_iter_b->SeekToFirst();
    assert_match("SeekToFirst", "-");
    while (merge_iter_a->Valid() && merge_iter_b->Valid()) {
      merge_iter_a->Next();
      merge_iter_b->Next();
      assert_match("Next", "-");
    }
    ASSERT_EQ(merge_iter_a->Valid(), merge_iter_b->Valid())
        << "Forward scan completion mismatch";

    // 4b. Reverse Scan: SeekToLast() followed by sequential Prev()
    merge_iter_a->SeekToLast();
    merge_iter_b->SeekToLast();
    assert_match("SeekToLast", "-");
    while (merge_iter_a->Valid() && merge_iter_b->Valid()) {
      merge_iter_a->Prev();
      merge_iter_b->Prev();
      assert_match("Prev", "-");
    }
    ASSERT_EQ(merge_iter_a->Valid(), merge_iter_b->Valid())
        << "Reverse scan completion mismatch";

    // 4c. Targeted Forward Seeks: Seek(target)
    for (const auto& target_ukey : probe_targets) {
      InternalKey target_ikey(target_ukey, read_seq, kValueTypeForSeek);
      merge_iter_a->Seek(target_ikey.Encode());
      merge_iter_b->Seek(target_ikey.Encode());
      assert_match("Seek", target_ukey);
    }

    // 4d. Targeted Reverse Seeks: SeekForPrev(target)
    for (const auto& target_ukey : probe_targets) {
      InternalKey target_ikey(target_ukey, 0, kValueTypeForSeekForPrev);
      merge_iter_a->SeekForPrev(target_ikey.Encode());
      merge_iter_b->SeekForPrev(target_ikey.Encode());
      assert_match("SeekForPrev", target_ukey);
    }

    // 5. Explicitly destruct MergingIterators before arena teardown
    merge_iter_a->~InternalIterator();
    merge_iter_b->~InternalIterator();
  }
};

// ==========================================================================
// Phase A (M4-P2c-0): Native Active MemTable Bounded-Equivalence Audit Harness
// ==========================================================================
class P2c0ActiveMemTableAuditHarness {
 public:
  using PointEntry = MergingIteratorCompatibilityHarness::PointEntry;

  static bool IsObservableValid(
      InternalIterator* iter, const Slice& L, const Slice& U, const Comparator* ucmp) {
    if (!iter || !iter->Valid()) return false;
    Slice k = iter->key();
    if (k.size() < kNumInternalBytes) return false;
    Slice uk = ExtractUserKey(k);
    const bool has_ts = (ucmp->timestamp_size() > 0);
    if (ucmp->CompareWithoutTimestamp(uk, has_ts, L, false) < 0) return false;
    if (ucmp->CompareWithoutTimestamp(uk, has_ts, U, false) >= 0) return false;
    return true;
  }

  // Track 1: Raw Out-of-Window Behavior Audit Table
  static void RunRawOutOfWindowAudit(
      const std::vector<PointEntry>& points,
      const CanonicalTombstoneContext& canonical_ctx,
      const LocalRangeDelView& local_view,
      const WindowSpec& window,
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq = kMaxSequenceNumber,
      const Slice* ts_upper_bound = nullptr) {
    ASSERT_TRUE(window.IsBounded() && window.lower_bound.has_value() && window.upper_bound.has_value());
    const auto* ucmp = icmp.user_comparator();
    const bool has_ts = (ucmp->timestamp_size() > 0);
    Slice U = *window.upper_bound;

    std::vector<std::pair<std::string, std::string>> sorted_points;
    for (const auto& p : points) {
      InternalKey ikey(p.user_key, p.seq, p.type);
      sorted_points.emplace_back(ikey.Encode().ToString(), p.value);
    }
    std::sort(sorted_points.begin(), sorted_points.end(),
              [&](const auto& a, const auto& b) {
                return icmp.Compare(a.first, b.first) < 0;
              });

    std::vector<std::string> pt_k_a, pt_v_a, pt_k_b, pt_v_b, pt_k_c, pt_v_c;
    for (const auto& kv : sorted_points) {
      pt_k_a.push_back(kv.first); pt_v_a.push_back(kv.second);
      pt_k_b.push_back(kv.first); pt_v_b.push_back(kv.second);
      pt_k_c.push_back(kv.first); pt_v_c.push_back(kv.second);
    }

    Arena arena_a, arena_b, arena_c;
    Slice ub_slice = U;
    if (has_ts && ub_slice.size() > ucmp->timestamp_size()) {
      ub_slice.remove_suffix(ucmp->timestamp_size());
    }

    // Group A: Unbounded TruncatedRangeDelIterator(nullptr, nullptr)
    MergeIteratorBuilder builder_a(&icmp, &arena_a, false, &ub_slice);
    auto pt_iter_a = new (arena_a.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_k_a), std::move(pt_v_a), &icmp);
    auto trunc_a = canonical_ctx.CreateNativeTruncatedRangeDelIterator(icmp, read_seq, ts_upper_bound, /*force_unbounded=*/true);
    builder_a.AddPointAndTombstoneIterator(pt_iter_a, std::move(trunc_a));
    InternalIterator* iter_a = builder_a.Finish();
    if (read_seq < kMaxSequenceNumber) {
      iter_a->SetRangeDelReadSeqno(read_seq);
    }

    // Group B: Bounded TruncatedRangeDelIterator(&smallest, &largest)
    MergeIteratorBuilder builder_b(&icmp, &arena_b, false, &ub_slice);
    auto pt_iter_b = new (arena_b.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_k_b), std::move(pt_v_b), &icmp);
    auto trunc_b = canonical_ctx.CreateNativeTruncatedRangeDelIterator(icmp, read_seq, ts_upper_bound, /*force_unbounded=*/false);
    builder_b.AddPointAndTombstoneIterator(pt_iter_b, std::move(trunc_b));
    InternalIterator* iter_b = builder_b.Finish();
    if (read_seq < kMaxSequenceNumber) {
      iter_b->SetRangeDelReadSeqno(read_seq);
    }

    // Group C: AMTV Local Bounded TruncatedRangeDelIterator(&smallest, &largest)
    MergeIteratorBuilder builder_c(&icmp, &arena_c, false, &ub_slice);
    auto pt_iter_c = new (arena_c.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_k_c), std::move(pt_v_c), &icmp);
    auto trunc_c = local_view.CreateNativeTruncatedRangeDelIterator(icmp, read_seq, ts_upper_bound);
    builder_c.AddPointAndTombstoneIterator(pt_iter_c, std::move(trunc_c));
    InternalIterator* iter_c = builder_c.Finish();
    if (read_seq < kMaxSequenceNumber) {
      iter_c->SetRangeDelReadSeqno(read_seq);
    }

    struct OpAudit {
      std::string op;
      std::string target;
      std::string a_res;
      std::string b_res;
      std::string c_res;
      std::string comment;
    };
    std::vector<OpAudit> audits;

    auto fmt_state = [&](InternalIterator* it) -> std::string {
      if (!it->Valid()) return "INVALID";
      Slice k = it->key();
      if (k.size() < kNumInternalBytes) return "INVALID_KEY";
      return ExtractUserKey(k).ToString();
    };

    // 1. SeekToFirst
    iter_a->SeekToFirst(); iter_b->SeekToFirst(); iter_c->SeekToFirst();
    audits.push_back({"SeekToFirst", "-", fmt_state(iter_a), fmt_state(iter_b), fmt_state(iter_c),
                      "A includes pre-window keys if not deleted; B/C start at first in-window or unmasked"});
    ASSERT_OK(iter_a->status()); ASSERT_OK(iter_b->status()); ASSERT_OK(iter_c->status());

    // 2. Seek(L - eps)
    std::string l_minus = "k00";
    if (has_ts) {
      std::string ts;
      PutFixed64(&ts, 100);
      l_minus += ts;
    }
    InternalKey ikey_l_minus(l_minus, kMaxSequenceNumber, kTypeValue);
    iter_a->Seek(ikey_l_minus.Encode()); iter_b->Seek(ikey_l_minus.Encode()); iter_c->Seek(ikey_l_minus.Encode());
    audits.push_back({"Seek", "L-eps (" + l_minus + ")", fmt_state(iter_a), fmt_state(iter_b), fmt_state(iter_c),
                      "Seek outside lower bound; A applies full tombstones, B/C clamp"});
    ASSERT_OK(iter_a->status()); ASSERT_OK(iter_b->status()); ASSERT_OK(iter_c->status());

    // 3. Seek(U + eps)
    std::string u_plus = "k99";
    if (has_ts) {
      std::string ts;
      PutFixed64(&ts, 100);
      u_plus += ts;
    }
    InternalKey ikey_u_plus(u_plus, kMaxSequenceNumber, kTypeValue);
    iter_a->Seek(ikey_u_plus.Encode()); iter_b->Seek(ikey_u_plus.Encode()); iter_c->Seek(ikey_u_plus.Encode());
    audits.push_back({"Seek", "U+eps (" + u_plus + ")", fmt_state(iter_a), fmt_state(iter_b), fmt_state(iter_c),
                      "Seek outside upper bound"});
    ASSERT_OK(iter_a->status()); ASSERT_OK(iter_b->status()); ASSERT_OK(iter_c->status());

    // 4. SeekToLast
    iter_a->SeekToLast(); iter_b->SeekToLast(); iter_c->SeekToLast();
    audits.push_back({"SeekToLast", "-", fmt_state(iter_a), fmt_state(iter_b), fmt_state(iter_c),
                      "Raw reverse end; A sees post-window tombstones; B/C bounded by U"});
    ASSERT_OK(iter_a->status()); ASSERT_OK(iter_b->status()); ASSERT_OK(iter_c->status());

    // 5. SeekForPrev(L - eps)
    iter_a->SeekForPrev(ikey_l_minus.Encode()); iter_b->SeekForPrev(ikey_l_minus.Encode()); iter_c->SeekForPrev(ikey_l_minus.Encode());
    audits.push_back({"SeekForPrev", "L-eps (" + l_minus + ")", fmt_state(iter_a), fmt_state(iter_b), fmt_state(iter_c),
                      "SeekForPrev before lower bound"});
    ASSERT_OK(iter_a->status()); ASSERT_OK(iter_b->status()); ASSERT_OK(iter_c->status());

    std::cout << "\n=== [M4-P2c-0 Out-of-Window Raw Behavior Audit Table] ===\n\n";
    std::cout << "| Operation | Target | Group A (Unbounded) | Group B (Full Bounded) | Group C (AMTV Bounded) | Nature of Divergence |\n";
    std::cout << "|:---|:---|:---|:---|:---|:---|\n";
    for (const auto& a : audits) {
      std::cout << "| `" << a.op << "` | `" << a.target << "` | `" << a.a_res << "` | `" << a.b_res << "` | `" << a.c_res << "` | " << a.comment << " |\n";
    }
    std::cout << "\n=========================================================\n\n";

    iter_a->~InternalIterator();
    iter_b->~InternalIterator();
    iter_c->~InternalIterator();
  }

  // Track 2: Bounded Consumer Equivalence (observable_valid projection in [L, U))
  static void RunBoundedConsumerEquivalence(
      const std::vector<PointEntry>& points,
      const CanonicalTombstoneContext& canonical_ctx,
      const LocalRangeDelView& local_view,
      const WindowSpec& window,
      const InternalKeyComparator& icmp,
      SequenceNumber read_seq = kMaxSequenceNumber,
      const std::vector<std::string>& probe_targets = {},
      const Slice* ts_upper_bound = nullptr) {
    ASSERT_TRUE(window.IsBounded() && window.lower_bound.has_value() && window.upper_bound.has_value());
    const auto* ucmp = icmp.user_comparator();
    const bool has_ts = (ucmp->timestamp_size() > 0);
    Slice L = *window.lower_bound;
    Slice U = *window.upper_bound;

    std::vector<std::pair<std::string, std::string>> sorted_points;
    for (const auto& p : points) {
      InternalKey ikey(p.user_key, p.seq, p.type);
      sorted_points.emplace_back(ikey.Encode().ToString(), p.value);
    }
    std::sort(sorted_points.begin(), sorted_points.end(),
              [&](const auto& a, const auto& b) {
                return icmp.Compare(a.first, b.first) < 0;
              });

    std::vector<std::string> pt_k_a, pt_v_a, pt_k_b, pt_v_b, pt_k_c, pt_v_c;
    for (const auto& kv : sorted_points) {
      pt_k_a.push_back(kv.first); pt_v_a.push_back(kv.second);
      pt_k_b.push_back(kv.first); pt_v_b.push_back(kv.second);
      pt_k_c.push_back(kv.first); pt_v_c.push_back(kv.second);
    }

    Arena arena_a, arena_b, arena_c;
    Slice ub_slice = U;
    if (has_ts && ub_slice.size() > ucmp->timestamp_size()) {
      ub_slice.remove_suffix(ucmp->timestamp_size());
    }

    // Group A: Unbounded TruncatedRangeDelIterator(nullptr, nullptr)
    MergeIteratorBuilder builder_a(&icmp, &arena_a, false, &ub_slice);
    auto pt_iter_a = new (arena_a.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_k_a), std::move(pt_v_a), &icmp);
    auto trunc_a = canonical_ctx.CreateNativeTruncatedRangeDelIterator(icmp, read_seq, ts_upper_bound, /*force_unbounded=*/true);
    builder_a.AddPointAndTombstoneIterator(pt_iter_a, std::move(trunc_a));
    InternalIterator* iter_a = builder_a.Finish();
    if (read_seq < kMaxSequenceNumber) {
      iter_a->SetRangeDelReadSeqno(read_seq);
    }

    // Group B: Bounded TruncatedRangeDelIterator(&smallest, &largest)
    MergeIteratorBuilder builder_b(&icmp, &arena_b, false, &ub_slice);
    auto pt_iter_b = new (arena_b.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_k_b), std::move(pt_v_b), &icmp);
    auto trunc_b = canonical_ctx.CreateNativeTruncatedRangeDelIterator(icmp, read_seq, ts_upper_bound, /*force_unbounded=*/false);
    builder_b.AddPointAndTombstoneIterator(pt_iter_b, std::move(trunc_b));
    InternalIterator* iter_b = builder_b.Finish();
    if (read_seq < kMaxSequenceNumber) {
      iter_b->SetRangeDelReadSeqno(read_seq);
    }

    // Group C: AMTV Local Bounded TruncatedRangeDelIterator(&smallest, &largest)
    MergeIteratorBuilder builder_c(&icmp, &arena_c, false, &ub_slice);
    auto pt_iter_c = new (arena_c.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_k_c), std::move(pt_v_c), &icmp);
    auto trunc_c = local_view.CreateNativeTruncatedRangeDelIterator(icmp, read_seq, ts_upper_bound);
    builder_c.AddPointAndTombstoneIterator(pt_iter_c, std::move(trunc_c));
    InternalIterator* iter_c = builder_c.Finish();
    if (read_seq < kMaxSequenceNumber) {
      iter_c->SetRangeDelReadSeqno(read_seq);
    }

    auto assert_observable_match = [&](const std::string& op_desc) {
      bool v_a = IsObservableValid(iter_a, L, U, ucmp);
      bool v_b = IsObservableValid(iter_b, L, U, ucmp);
      bool v_c = IsObservableValid(iter_c, L, U, ucmp);

      ASSERT_EQ(v_a, v_b) << op_desc << ": observable_valid mismatch between A and B";
      ASSERT_EQ(v_b, v_c) << op_desc << ": observable_valid mismatch between B and C";

      if (v_a) {
        ASSERT_EQ(iter_a->key().ToString(), iter_b->key().ToString())
            << op_desc << ": key mismatch between A and B";
        ASSERT_EQ(iter_b->key().ToString(), iter_c->key().ToString())
            << op_desc << ": key mismatch between B and C";
        ASSERT_EQ(iter_a->value().ToString(), iter_b->value().ToString())
            << op_desc << ": value mismatch between A and B";
        ASSERT_EQ(iter_b->value().ToString(), iter_c->value().ToString())
            << op_desc << ": value mismatch between B and C";

        ParsedInternalKey pik_a, pik_b, pik_c;
        ASSERT_OK(ParseInternalKey(iter_a->key(), &pik_a, false));
        ASSERT_OK(ParseInternalKey(iter_b->key(), &pik_b, false));
        ASSERT_OK(ParseInternalKey(iter_c->key(), &pik_c, false));

        ASSERT_EQ(pik_a.sequence, pik_b.sequence);
        ASSERT_EQ(pik_b.sequence, pik_c.sequence);
        ASSERT_EQ(pik_a.type, pik_b.type);
        ASSERT_EQ(pik_b.type, pik_c.type);

        if (has_ts) {
          Slice ts_a = ExtractTimestampFromUserKey(pik_a.user_key, ucmp->timestamp_size());
          Slice ts_b = ExtractTimestampFromUserKey(pik_b.user_key, ucmp->timestamp_size());
          Slice ts_c = ExtractTimestampFromUserKey(pik_c.user_key, ucmp->timestamp_size());
          ASSERT_EQ(ts_a.ToString(), ts_b.ToString());
          ASSERT_EQ(ts_b.ToString(), ts_c.ToString());
        }
      }
      ASSERT_OK(iter_a->status());
      ASSERT_OK(iter_b->status());
      ASSERT_OK(iter_c->status());
    };

    // 1. Forward Scan: DBIter begins with Seek(L)
    InternalKey ikey_l(L, kMaxSequenceNumber, kTypeValue);
    iter_a->Seek(ikey_l.Encode());
    iter_b->Seek(ikey_l.Encode());
    iter_c->Seek(ikey_l.Encode());

    while (IsObservableValid(iter_a, L, U, ucmp) ||
           IsObservableValid(iter_b, L, U, ucmp) ||
           IsObservableValid(iter_c, L, U, ucmp)) {
      assert_observable_match("Forward Scan Step");
      iter_a->Next();
      iter_b->Next();
      iter_c->Next();
    }
    assert_observable_match("Forward Scan Termination");

    // 2. Reverse Scan: DBIter begins with SeekForPrev(U)
    InternalKey ikey_u(U, 0, kValueTypeForSeekForPrev);
    iter_a->SeekForPrev(ikey_u.Encode());
    iter_b->SeekForPrev(ikey_u.Encode());
    iter_c->SeekForPrev(ikey_u.Encode());

    // In DBIter, if SeekForPrev(U) lands on key >= U, it steps Prev() until < U
    auto clamp_reverse_below_u = [&](InternalIterator* it) {
      while (it->Valid() && ucmp->CompareWithoutTimestamp(
                 ExtractUserKey(it->key()), has_ts, U, false) >= 0) {
        it->Prev();
      }
    };
    clamp_reverse_below_u(iter_a);
    clamp_reverse_below_u(iter_b);
    clamp_reverse_below_u(iter_c);

    while (IsObservableValid(iter_a, L, U, ucmp) ||
           IsObservableValid(iter_b, L, U, ucmp) ||
           IsObservableValid(iter_c, L, U, ucmp)) {
      assert_observable_match("Reverse Scan Step");
      iter_a->Prev();
      iter_b->Prev();
      iter_c->Prev();
    }
    assert_observable_match("Reverse Scan Termination");

    // 3. Intra-window targeted seeks
    for (const auto& target_ukey : probe_targets) {
      if (ucmp->CompareWithoutTimestamp(target_ukey, has_ts, L, false) < 0 ||
          ucmp->CompareWithoutTimestamp(target_ukey, has_ts, U, false) >= 0) {
        continue;
      }

      InternalKey target_seek_ikey(target_ukey, kMaxSequenceNumber, kTypeValue);
      iter_a->Seek(target_seek_ikey.Encode());
      iter_b->Seek(target_seek_ikey.Encode());
      iter_c->Seek(target_seek_ikey.Encode());
      assert_observable_match("Target Seek(" + target_ukey + ")");

      InternalKey target_prev_ikey(target_ukey, 0, kValueTypeForSeekForPrev);
      iter_a->SeekForPrev(target_prev_ikey.Encode());
      iter_b->SeekForPrev(target_prev_ikey.Encode());
      iter_c->SeekForPrev(target_prev_ikey.Encode());
      auto clamp_reverse_below_t = [&](InternalIterator* it) {
        while (it->Valid() && ucmp->CompareWithoutTimestamp(
                   ExtractUserKey(it->key()), has_ts, target_ukey, false) > 0) {
          it->Prev();
        }
      };
      clamp_reverse_below_t(iter_a);
      clamp_reverse_below_t(iter_b);
      clamp_reverse_below_t(iter_c);
      assert_observable_match("Target SeekForPrev(" + target_ukey + ")");
    }

    iter_a->~InternalIterator();
    iter_b->~InternalIterator();
    iter_c->~InternalIterator();
  }
};

// ==========================================================================
// P2b Test Cases: MergingIterator Consumer Compatibility Verification
// ==========================================================================

TEST_F(AMTVLocalScanReferenceTest, P2b_SafetyEligibilityPredicate) {
  const Comparator* ucmp = BytewiseComparator();

  // 1. Active MemTable + AMTV enabled + both bounds + L < U -> Local
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k10";
    ctx.upper_bound = "k50";
    ctx.ucmp = ucmp;
    EXPECT_TRUE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 2. Unbounded (both nullopt) -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = std::nullopt;
    ctx.upper_bound = std::nullopt;
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 3. Single lower bound -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k10";
    ctx.upper_bound = std::nullopt;
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 4. Single upper bound -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = std::nullopt;
    ctx.upper_bound = "k50";
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 5. Inverted window (L > U) -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k50";
    ctx.upper_bound = "k10";
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 6. Empty window (L == U) -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k20";
    ctx.upper_bound = "k20";
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 7. AMTV disabled -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = false;
    ctx.lower_bound = "k10";
    ctx.upper_bound = "k50";
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 8. Immutable MemTable -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kImmutableMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k10";
    ctx.upper_bound = "k50";
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 9. SST Level -> Native
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kSSTLevel;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k10";
    ctx.upper_bound = "k50";
    ctx.ucmp = ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 10. UDT enabled: L < U -> Local
  {
    const Comparator* ts_ucmp = GetBytewiseComparatorWithU64Ts();
    std::string ts1, ts2;
    PutFixed64(&ts1, 100);
    PutFixed64(&ts2, 200);
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k10" + ts1;
    ctx.upper_bound = "k50" + ts2;
    ctx.ucmp = ts_ucmp;
    EXPECT_TRUE(IsEligibleForLocalRangeDelView(ctx));
  }
  // 11. UDT enabled: L >= U -> Native
  {
    const Comparator* ts_ucmp = GetBytewiseComparatorWithU64Ts();
    std::string ts1, ts2;
    PutFixed64(&ts1, 100);
    PutFixed64(&ts2, 200);
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k50" + ts1;
    ctx.upper_bound = "k10" + ts2;
    ctx.ucmp = ts_ucmp;
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));
  }
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_LongTombstonesCrossingBounds) {
  WindowSpec window("k30", "k70");

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k45", 100)},  // left crossing
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k55", "k90", 120),  // right crossing
      OpenDeltaEntry("k05", "k95", 80),   // full crossing
  };

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k10", "k45", 100},
      {"k55", "k90", 120},
      {"k05", "k95", 80},
  };
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, bytewise_icmp_);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k08", 90},
      {"k20", 70},
      {"k30", 90},
      {"k40", 90},
      {"k50", 90},
      {"k60", 110},
      {"k70", 110},
      {"k85", 110},
  };

  std::vector<std::string> probe_targets = {
      "k05", "k20", "k30", "k40", "k50", "k60", "k70", "k85", "k99"};

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets, nullptr, nullptr, &trace);

  EXPECT_GT(trace.size(), 0U);
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_OverlappingAndNestedTombstones) {
  WindowSpec window("k20", "k80");

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k15", "k50", 100), OpenDeltaEntry("k35", "k65", 150)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k50", "k85", 120),
      OpenDeltaEntry("k25", "k75", 90),
      OpenDeltaEntry("k40", "k60", 180),
  };

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k15", "k50", 100},
      {"k35", "k65", 150},
      {"k50", "k85", 120},
      {"k25", "k75", 90},
      {"k40", "k60", 180},
  };
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, bytewise_icmp_);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k18", 95},
      {"k22", 80},
      {"k28", 95},
      {"k42", 160},
      {"k42", 190},  // resurrected!
      {"k55", 140},
      {"k70", 100},
      {"k78", 110},
      {"k82", 110},
  };

  std::vector<std::string> probe_targets = {
      "k15", "k20", "k22", "k28", "k42", "k55", "k70", "k78", "k80", "k90"};

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets, nullptr, nullptr, &trace);

  EXPECT_GT(trace.size(), 0U);
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_AdjacentTombstones) {
  WindowSpec window("k30", "k70");

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k30", "k50", 120), OpenDeltaEntry("k50", "k70", 130)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30", "k45", 100),
      OpenDeltaEntry("k45", "k60", 110),
      OpenDeltaEntry("k60", "k70", 120),
  };

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k30", "k50", 120},
      {"k50", "k70", 130},
      {"k30", "k45", 100},
      {"k45", "k60", 110},
      {"k60", "k70", 120},
  };
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, bytewise_icmp_);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k30", 110},
      {"k35", 105},
      {"k35", 140},  // resurrected!
      {"k45", 105},
      {"k50", 125},
      {"k55", 140},  // resurrected!
      {"k60", 115},
      {"k65", 115},
      {"k70", 120},
  };

  std::vector<std::string> probe_targets = {
      "k10", "k25", "k30", "k35", "k45", "k50", "k55", "k60", "k65", "k70", "k75", "k90"};

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets, nullptr, nullptr, &trace);

  EXPECT_GT(trace.size(), 0U);
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_EqualStartDifferentSequence) {
  WindowSpec window("k20", "k80");

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k30", "k40", 80), OpenDeltaEntry("k30", "k50", 140)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30", "k60", 110),
  };

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k30", "k40", 80},
      {"k30", "k50", 140},
      {"k30", "k60", 110},
  };
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, bytewise_icmp_);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k25", 100},
      {"k35", 100},
      {"k45", 120},
      {"k55", 100},
      {"k55", 130},  // resurrected above seq 110 (after k50 where 140 ends)
      {"k65", 100},
      {"k75", 100},
  };

  std::vector<std::string> probe_targets = {
      "k20", "k25", "k30", "k35", "k45", "k55", "k65", "k75", "k80"};

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets, nullptr, nullptr, &trace);

  EXPECT_GT(trace.size(), 0U);
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_FutureTombstonesAndMultiSnapshot) {
  WindowSpec window("k20", "k80");

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k25", "k55", 60), OpenDeltaEntry("k35", "k65", 110)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k45", "k75", 160),
      OpenDeltaEntry("k50", "k70", 210),
  };

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k25", "k55", 60},
      {"k35", "k65", 110},
      {"k45", "k75", 160},
      {"k50", "k70", 210},
  };
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, bytewise_icmp_);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k30", 50},
      {"k30", 70},
      {"k40", 90},
      {"k40", 130},
      {"k50", 140},
      {"k50", 180},
      {"k60", 150},
      {"k60", 220},
  };

  std::vector<std::string> probe_targets = {
      "k20", "k25", "k30", "k40", "k50", "k60", "k70", "k75", "k80"};

  // Test across multiple snapshot read_seqs: 80, 130, 180, 250
  std::vector<SequenceNumber> test_snapshots = {80, 130, 180, 250};
  for (SequenceNumber read_seq : test_snapshots) {
    std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace;
    MergingIteratorCompatibilityHarness::RunDifferentialComparison(
        points, canonical_ctx, local_view, window, bytewise_icmp_,
        read_seq, probe_targets, nullptr, nullptr, &trace);
    EXPECT_GT(trace.size(), 0U);
  }
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_PutResurrection) {
  WindowSpec window("k30", "k70");

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k30", "k70", 100)},
  };
  std::vector<OpenDeltaEntry> delta = {};

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k30", "k70", 100},
  };
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, bytewise_icmp_);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k40", 50},   // covered, deleted (seq 50 < 100)
      {"k45", 150},  // resurrected, visible (seq 150 > 100)
      {"k50", 80},   // covered, deleted (seq 80 < 100)
      {"k60", 130},  // resurrected, visible (seq 130 > 100)
      {"k60", 70},   // older than resurrection and tombstone (seq 70 < 100)
      {"k65", 99},   // covered, deleted (seq 99 < 100)
  };

  std::vector<std::string> probe_targets = {
      "k20", "k30", "k40", "k45", "k50", "k60", "k65", "k70", "k80"};

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets, nullptr, nullptr, &trace);

  EXPECT_GT(trace.size(), 0U);
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_UserDefinedTimestamp) {
  const Comparator* ucmp = GetBytewiseComparatorWithU64Ts();
  ASSERT_NE(ucmp, nullptr);
  InternalKeyComparator ts_icmp(ucmp);
  const size_t ts_sz = ucmp->timestamp_size();
  std::string dummy_ts(ts_sz, '\0');

  std::string ts100, ts200, ts300, ts250;
  PutFixed64(&ts100, 100);
  PutFixed64(&ts200, 200);
  PutFixed64(&ts300, 300);
  PutFixed64(&ts250, 250);

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10" + ts100, "k60" + ts100, 50)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k30" + ts200, "k80" + ts200, 80),
      OpenDeltaEntry("k50" + ts300, "k90" + ts300, 120),
  };
  WindowSpec window("k20", "k70");

  LocalRangeDelView local_view(runs, delta, window, ts_icmp);
  std::vector<OwnedRawRangeTombstone> all_tombstones;
  for (const auto& e : runs[0]) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  for (const auto& e : delta) {
    all_tombstones.emplace_back(e.user_start_key().ToString(), e.end_key, e.seq);
  }
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, ts_icmp);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k15" + ts100, 60},
      {"k25" + ts100, 40},
      {"k35" + ts200, 70},
      {"k45" + ts200, 90},
      {"k55" + ts300, 110},
      {"k65" + ts300, 130},
      {"k75" + ts300, 110},
  };

  std::vector<std::string> probe_targets = {
      "k15" + dummy_ts, "k20" + dummy_ts, "k35" + dummy_ts,
      "k45" + dummy_ts, "k55" + dummy_ts, "k70" + dummy_ts,
      "k80" + dummy_ts};

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, local_view, window, ts_icmp,
      200, probe_targets, nullptr, nullptr, &trace);

  EXPECT_GT(trace.size(), 0U);
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_OldVsNewAmtvSnapshot) {
  WindowSpec window("k25", "k75");

  std::vector<std::vector<OpenDeltaEntry>> old_runs = {
      {OpenDeltaEntry("k10", "k50", 100)},
      {OpenDeltaEntry("k40", "k80", 120)},
  };
  std::vector<OpenDeltaEntry> old_delta = {
      OpenDeltaEntry("k60", "k90", 140),
  };

  // Simulating background merge of Run 1 and Run 2 into a single Run
  std::vector<std::vector<OpenDeltaEntry>> new_runs = {
      {OpenDeltaEntry("k10", "k50", 100), OpenDeltaEntry("k40", "k80", 120)},
  };
  std::vector<OpenDeltaEntry> new_delta = {
      OpenDeltaEntry("k60", "k90", 140),
  };

  LocalRangeDelView old_view(old_runs, old_delta, window, bytewise_icmp_);
  LocalRangeDelView new_view(new_runs, new_delta, window, bytewise_icmp_);

  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k10", "k50", 100},
      {"k40", "k80", 120},
      {"k60", "k90", 140},
  };
  auto canonical_ctx = CanonicalTombstoneContext::Create(
      all_tombstones, window, bytewise_icmp_);

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k15", 90},
      {"k30", 90},
      {"k45", 110},
      {"k55", 110},
      {"k65", 130},
      {"k70", 150},
      {"k85", 130},
  };

  std::vector<std::string> probe_targets = {
      "k15", "k25", "k35", "k45", "k55", "k65", "k75", "k85"};

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace_old;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, old_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets, nullptr, nullptr, &trace_old);

  std::vector<MergingIteratorCompatibilityHarness::ComparisonStepRecord> trace_new;
  MergingIteratorCompatibilityHarness::RunDifferentialComparison(
      points, canonical_ctx, new_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets, nullptr, nullptr, &trace_new);

  ASSERT_EQ(trace_old.size(), trace_new.size());
  for (size_t i = 0; i < trace_old.size(); ++i) {
    EXPECT_EQ(trace_old[i].op, trace_new[i].op);
    EXPECT_EQ(trace_old[i].valid, trace_new[i].valid);
    EXPECT_EQ(trace_old[i].key, trace_new[i].key);
    EXPECT_EQ(trace_old[i].value, trace_new[i].value);
    EXPECT_EQ(trace_old[i].seq, trace_new[i].seq);
  }
}

TEST_F(AMTVLocalScanReferenceTest, P2b_MergingIter_EmptyAndInvertedWindowFallback) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k20", "k60", 100)},
  };
  std::vector<OpenDeltaEntry> delta = {
      OpenDeltaEntry("k40", "k80", 120),
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k20", "k60", 100},
      {"k40", "k80", 120},
  };

  std::vector<MergingIteratorCompatibilityHarness::PointEntry> points = {
      {"k25", 90},
      {"k45", 110},
      {"k65", 110},
  };
  std::vector<std::string> probe_targets = {"k20", "k40", "k60", "k80"};

  // 1. Empty window [k40, k40): Evaluated by predicate as not eligible -> Fallback to Native Full
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k40";
    ctx.upper_bound = "k40";
    ctx.ucmp = bytewise_icmp_.user_comparator();
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));

    // Under Safety Rule, empty window falls back to full native view (unbounded)
    WindowSpec fallback_win(std::nullopt, std::nullopt);
    LocalRangeDelView local_view(runs, delta, fallback_win, bytewise_icmp_);
    EXPECT_TRUE(local_view.state()->fallback_to_full);

    auto canonical_ctx = CanonicalTombstoneContext::Create(
        all_tombstones, fallback_win, bytewise_icmp_);
    MergingIteratorCompatibilityHarness::RunDifferentialComparison(
        points, canonical_ctx, local_view, fallback_win, bytewise_icmp_,
        kMaxSequenceNumber, probe_targets);
  }

  // 2. Inverted window [k60, k30): Evaluated by predicate as not eligible -> Fallback to Native Full
  {
    ScanEligibilityContext ctx;
    ctx.target_type = ScanTargetType::kActiveMemTable;
    ctx.amtv_enabled = true;
    ctx.lower_bound = "k60";
    ctx.upper_bound = "k30";
    ctx.ucmp = bytewise_icmp_.user_comparator();
    EXPECT_FALSE(IsEligibleForLocalRangeDelView(ctx));

    WindowSpec fallback_win(std::nullopt, std::nullopt);
    LocalRangeDelView local_view(runs, delta, fallback_win, bytewise_icmp_);
    EXPECT_TRUE(local_view.state()->fallback_to_full);

    auto canonical_ctx = CanonicalTombstoneContext::Create(
        all_tombstones, fallback_win, bytewise_icmp_);
    MergingIteratorCompatibilityHarness::RunDifferentialComparison(
        points, canonical_ctx, local_view, fallback_win, bytewise_icmp_,
        kMaxSequenceNumber, probe_targets);
  }
}

// ==========================================================================
// Phase A (M4-P2c-0): Native Active MemTable Bounded-Equivalence Test Suite
// ==========================================================================

// 1. Raw Out-of-Window Behavior Audit (Non-failing, logs divergence table)
TEST_F(AMTVLocalScanReferenceTest, P2c_0_RawOutOfWindowBehaviorAudit) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k05", "k25", 100}, {"k20", "k45", 120}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k55", "k85", 140},
      {"k90", "k98", 160},
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k05", "k25", 100},
      {"k20", "k45", 120},
      {"k55", "k85", 140},
      {"k90", "k98", 160},
  };

  WindowSpec window("k30", "k70");
  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k10", 50},  // outside left, covered by [k05, k25)
      {"k22", 50},  // outside left, covered by [k20, k45)
      {"k35", 50},  // in-window, covered by [k20, k45)
      {"k48", 50},  // in-window, visible!
      {"k60", 50},  // in-window, covered by [k55, k85)
      {"k68", 50},  // in-window, covered by [k55, k85)
      {"k75", 50},  // outside right, covered by [k55, k85)
      {"k95", 50},  // outside right, covered by [k90, k98)
  };

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto canonical_ctx = CanonicalTombstoneContext::Create(all_tombstones, window, bytewise_icmp_);

  P2c0ActiveMemTableAuditHarness::RunRawOutOfWindowAudit(
      points, canonical_ctx, local_view, window, bytewise_icmp_);
}

// 2. Lifecycle: State, Builder, and Snapshot Destruction (Zero UAF / Dangling Bounds)
TEST_F(AMTVLocalScanReferenceTest, P2c_0_Lifecycle_StateAndBoundsDestruction) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k10", "k35", 100}, {"k40", "k60", 120}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k55", "k80", 140},
  };

  WindowSpec window("k30", "k70");
  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k20", 50},  // outside left
      {"k32", 50},  // covered by [k10, k35)
      {"k38", 50},  // visible!
      {"k45", 50},  // covered by [k40, k60)
      {"k50", 150}, // resurrected Put! (seq 150 > 120)
      {"k65", 50},  // covered by [k55, k80)
      {"k75", 50},  // outside right
  };

  Arena arena;
  InternalIterator* merging_iter = nullptr;
  Slice ub_slice = "k70";

  // Build inside scope
  {
    LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
    auto trunc_iter = local_view.CreateNativeTruncatedRangeDelIterator(bytewise_icmp_);

    std::vector<std::pair<std::string, std::string>> sorted_points;
    for (const auto& p : points) {
      InternalKey ikey(p.user_key, p.seq, p.type);
      sorted_points.emplace_back(ikey.Encode().ToString(), p.value);
    }
    std::sort(sorted_points.begin(), sorted_points.end(),
              [&](const auto& a, const auto& b) {
                return bytewise_icmp_.Compare(a.first, b.first) < 0;
              });
    std::vector<std::string> pt_k, pt_v;
    for (const auto& kv : sorted_points) {
      pt_k.push_back(kv.first); pt_v.push_back(kv.second);
    }

    MergeIteratorBuilder builder(&bytewise_icmp_, &arena, false, &ub_slice);
    auto pt_iter = new (arena.AllocateAligned(sizeof(VectorIterator)))
        VectorIterator(std::move(pt_k), std::move(pt_v), &bytewise_icmp_);
    builder.AddPointAndTombstoneIterator(pt_iter, std::move(trunc_iter));
    merging_iter = builder.Finish();

    // local_view, builder, pt_k, pt_v fall out of scope and are destroyed here!
  }

  // OUTSIDE SCOPE: merging_iter survives independently.
  // Execute full operations in observable window [k30, k70)
  InternalKey seek_l("k30", kMaxSequenceNumber, kTypeValue);
  merging_iter->Seek(seek_l.Encode());

  std::vector<std::string> visible_keys_forward;
  while (P2c0ActiveMemTableAuditHarness::IsObservableValid(
             merging_iter, "k30", "k70", bytewise_icmp_.user_comparator())) {
    visible_keys_forward.push_back(ExtractUserKey(merging_iter->key()).ToString());
    merging_iter->Next();
  }
  ASSERT_OK(merging_iter->status());

  // Expected visible keys: k38 and k50 (resurrected)
  ASSERT_EQ(visible_keys_forward.size(), 2u);
  EXPECT_EQ(visible_keys_forward[0], "k38");
  EXPECT_EQ(visible_keys_forward[1], "k50");

  // Reverse scan
  InternalKey seek_u("k70", 0, kValueTypeForSeekForPrev);
  merging_iter->SeekForPrev(seek_u.Encode());
  while (merging_iter->Valid() && bytewise_icmp_.user_comparator()->CompareWithoutTimestamp(
             ExtractUserKey(merging_iter->key()), false, "k70", false) >= 0) {
    merging_iter->Prev();
  }
  std::vector<std::string> visible_keys_backward;
  while (P2c0ActiveMemTableAuditHarness::IsObservableValid(
             merging_iter, "k30", "k70", bytewise_icmp_.user_comparator())) {
    visible_keys_backward.push_back(ExtractUserKey(merging_iter->key()).ToString());
    merging_iter->Prev();
  }
  ASSERT_OK(merging_iter->status());
  ASSERT_EQ(visible_keys_backward.size(), 2u);
  EXPECT_EQ(visible_keys_backward[0], "k50");
  EXPECT_EQ(visible_keys_backward[1], "k38");

  merging_iter->~InternalIterator();
}

// 3. Long Tombstones Crossing Bounds
TEST_F(AMTVLocalScanReferenceTest, P2c_0_ActiveMemTable_LongTombstonesCrossingBounds) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k10", "k45", 100}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k55", "k90", 120},
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k10", "k45", 100},
      {"k55", "k90", 120},
  };

  WindowSpec window("k30", "k70");
  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k20", 50},  // outside left
      {"k30", 50},  // boundary L, covered by [k10, k45)
      {"k35", 50},  // covered by [k10, k45)
      {"k48", 50},  // visible!
      {"k50", 50},  // visible!
      {"k58", 50},  // covered by [k55, k90)
      {"k68", 50},  // covered by [k55, k90)
      {"k70", 50},  // boundary U
      {"k80", 50},  // outside right
  };
  std::vector<std::string> probe_targets = {"k30", "k48", "k50", "k60", "k70"};

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto canonical_ctx = CanonicalTombstoneContext::Create(all_tombstones, window, bytewise_icmp_);

  P2c0ActiveMemTableAuditHarness::RunBoundedConsumerEquivalence(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets);
}

// 4. Overlapping and Nested Tombstones
TEST_F(AMTVLocalScanReferenceTest, P2c_0_ActiveMemTable_OverlappingAndNestedTombstones) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k25", "k65", 100}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k35", "k50", 150},
      {"k45", "k60", 180},
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k25", "k65", 100},
      {"k35", "k50", 150},
      {"k45", "k60", 180},
  };

  WindowSpec window("k30", "k70");
  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k32", 80},  // covered by [k25, k65) @ 100
      {"k38", 120}, // covered by [k35, k50) @ 150
      {"k48", 160}, // covered by [k45, k60) @ 180
      {"k62", 80},  // covered by [k25, k65) @ 100
      {"k68", 80},  // visible!
  };
  std::vector<std::string> probe_targets = {"k30", "k38", "k48", "k62", "k68"};

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto canonical_ctx = CanonicalTombstoneContext::Create(all_tombstones, window, bytewise_icmp_);

  P2c0ActiveMemTableAuditHarness::RunBoundedConsumerEquivalence(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets);
}

// 5. Adjacent Boundaries
TEST_F(AMTVLocalScanReferenceTest, P2c_0_ActiveMemTable_AdjacentBoundaries) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k20", "k40", 100}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k40", "k60", 120},
      {"k60", "k80", 140},
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k20", "k40", 100},
      {"k40", "k60", 120},
      {"k60", "k80", 140},
  };

  WindowSpec window("k30", "k70");
  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k35", 50},  // covered by [k20, k40)
      {"k40", 110}, // right at boundary! [k20, k40) ends at k40, but [k40, k60) @ 120 covers k40!
      {"k45", 50},  // covered by [k40, k60)
      {"k55", 130}, // resurrected Put! (seq 130 > 120)
      {"k60", 50},  // covered by [k60, k80) @ 140
      {"k65", 50},  // covered by [k60, k80)
  };
  std::vector<std::string> probe_targets = {"k30", "k40", "k45", "k55", "k60", "k65"};

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto canonical_ctx = CanonicalTombstoneContext::Create(all_tombstones, window, bytewise_icmp_);

  P2c0ActiveMemTableAuditHarness::RunBoundedConsumerEquivalence(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      kMaxSequenceNumber, probe_targets);
}

// 6. Put Resurrection and Future Tombstones
TEST_F(AMTVLocalScanReferenceTest, P2c_0_ActiveMemTable_PutResurrectionAndFutureTombstones) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k20", "k50", 100}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k40", "k80", 250}, // future tombstone at read_seq = 200
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k20", "k50", 100},
      {"k40", "k80", 250},
  };

  WindowSpec window("k30", "k70");
  const SequenceNumber read_seq = 200;

  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k35", 50},  // covered by [k20, k50) @ 100
      {"k38", 150}, // resurrected Put! (seq 150 > 100, <= 200)
      {"k45", 150}, // in [k40, k80) @ 250, but read_seq=200 so tombstone @ 250 is inactive! visible!
      {"k55", 50},  // in [k40, k80) @ 250, but tombstone is future, visible!
      {"k65", 50},  // visible!
  };
  std::vector<std::string> probe_targets = {"k30", "k35", "k38", "k45", "k55", "k65"};

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto canonical_ctx = CanonicalTombstoneContext::Create(all_tombstones, window, bytewise_icmp_);

  P2c0ActiveMemTableAuditHarness::RunBoundedConsumerEquivalence(
      points, canonical_ctx, local_view, window, bytewise_icmp_,
      read_seq, probe_targets);
}

// 7. User-Defined Timestamp (UDT)
TEST_F(AMTVLocalScanReferenceTest, P2c_0_ActiveMemTable_UserDefinedTimestamp) {
  const Comparator* ts_ucmp = GetBytewiseComparatorWithU64Ts();
  InternalKeyComparator ts_icmp(ts_ucmp);

  std::string ts10, ts20, ts30, ts40, ts50;
  PutFixed64(&ts10, 10);
  PutFixed64(&ts20, 20);
  PutFixed64(&ts30, 30);
  PutFixed64(&ts40, 40);
  PutFixed64(&ts50, 50);

  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k20" + ts20, "k50" + ts20, 100}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k40" + ts30, "k80" + ts30, 150},
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k20" + ts20, "k50" + ts20, 100},
      {"k40" + ts30, "k80" + ts30, 150},
  };

  WindowSpec window("k30" + ts50, "k70" + ts50);
  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k35" + ts10, 50},  // covered by [k20, k50)
      {"k38" + ts10, 180}, // resurrected Put! (seq 180 > 100)
      {"k45" + ts10, 120}, // covered by [k40, k80) @ 150
      {"k55" + ts10, 120}, // covered by [k40, k80) @ 150
      {"k65" + ts10, 180}, // resurrected Put! (seq 180 > 150)
      {"k68" + ts10, 50},  // not covered, visible!
  };
  std::vector<std::string> probe_targets = {
      "k30" + ts50, "k35" + ts10, "k38" + ts10, "k45" + ts10, "k65" + ts10, "k68" + ts10
  };

  LocalRangeDelView local_view(runs, delta, window, ts_icmp);
  auto canonical_ctx = CanonicalTombstoneContext::Create(all_tombstones, window, ts_icmp);

  P2c0ActiveMemTableAuditHarness::RunBoundedConsumerEquivalence(
      points, canonical_ctx, local_view, window, ts_icmp,
      kMaxSequenceNumber, probe_targets);
}

// 8. Multi-Snapshot Equivalence
TEST_F(AMTVLocalScanReferenceTest, P2c_0_ActiveMemTable_MultiSnapshot) {
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {{"k20", "k50", 100}},
  };
  std::vector<OpenDeltaEntry> delta = {
      {"k30", "k60", 200},
      {"k40", "k70", 300},
  };
  std::vector<OwnedRawRangeTombstone> all_tombstones = {
      {"k20", "k50", 100},
      {"k30", "k60", 200},
      {"k40", "k70", 300},
  };

  WindowSpec window("k25", "k65");
  std::vector<P2c0ActiveMemTableAuditHarness::PointEntry> points = {
      {"k28", 50},
      {"k35", 150},
      {"k45", 250},
      {"k55", 350},
  };
  std::vector<std::string> probe_targets = {"k25", "k28", "k35", "k45", "k55", "k65"};

  LocalRangeDelView local_view(runs, delta, window, bytewise_icmp_);
  auto canonical_ctx = CanonicalTombstoneContext::Create(all_tombstones, window, bytewise_icmp_);

  for (SequenceNumber snapshot_seq : {50, 150, 250, 350, 400}) {
    P2c0ActiveMemTableAuditHarness::RunBoundedConsumerEquivalence(
        points, canonical_ctx, local_view, window, bytewise_icmp_,
        snapshot_seq, probe_targets);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
