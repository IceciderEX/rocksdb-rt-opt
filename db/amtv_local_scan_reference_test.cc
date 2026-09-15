//  Copyright (c) 2026-present. All rights reserved.
//  Test-only implementation and test suite for AMTV M4-P1b-0: Run-Internal
//  Prefix-Max-End Interval Index Prototype with Boundary Clipping and 3-Way Oracle.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#include <algorithm>
#include <iomanip>
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
