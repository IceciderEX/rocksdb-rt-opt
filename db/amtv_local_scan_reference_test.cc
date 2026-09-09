//  Copyright (c) 2026-present. All rights reserved.
//  Test-only implementation and test suite for AMTV M4-P1a.1: Local Scan Semantic Reference Prototype
//  with Output Boundary Clipping and 3-Way Independent Oracle.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#include <algorithm>
#include <iostream>
#include <memory>
#include <random>
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
        if (entry.sequence() > max_seq) {
          max_seq = entry.sequence();
          if (has_ts) {
            best_ts = entry.timestamp(ts_sz);
          }
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

  // Verification 4: Stream equivalence on new view
  std::vector<std::vector<OpenDeltaEntry>> new_runs_raw;
  for (const auto& r : new_snapshot->sealed_runs) {
    if (r) new_runs_raw.push_back(r->raw_entries);
  }
  AMTVCanonicalFullTruth new_truth(
      new_runs_raw, new_snapshot->open_delta->entries(), bytewise_icmp_, 200);
  VerifyClippedStreamEquivalence(new_view, new_truth, &L, &U, bytewise_icmp_);

  amtv_state->CancelAndDrain();
  Env::Default()->SetBackgroundThreads(0, Env::Priority::LOW);
}

// --------------------------------------------------------------------------
// 14. Randomized Differential Testing (1,000 Trials)
// Features randomized read_seq (early, mid, late) and controlled long
// tombstone boundary mix (left spanning, right spanning, fully enclosing).
// Retains failure diagnostics with minimal reproduction info.
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
          // Left-boundary spanning: S < L < E < U
          k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(l_idx + 1, u_idx - 1)(rng);
        } else if (topo_type == 1) {
          // Right-boundary spanning: L < S < U < E
          k1 = std::uniform_int_distribution<int>(l_idx + 1, u_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(u_idx + 1, u_idx + 30)(rng);
        } else if (topo_type == 2) {
          // Fully enclosing: S < L < U < E
          k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
          k2 = std::uniform_int_distribution<int>(u_idx + 1, u_idx + 30)(rng);
        } else {
          // Generic random interval
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
        // Spanning left boundary
        k1 = std::uniform_int_distribution<int>(0, l_idx - 1)(rng);
        k2 = std::uniform_int_distribution<int>(l_idx + 1, u_idx)(rng);
      } else if (topo_type == 1) {
        // Spanning right boundary
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

    // Requirement II.4: 3 categories of randomized read_seq
    int seq_category = std::uniform_int_distribution<int>(0, 2)(rng);
    SequenceNumber read_seq = 0;
    if (seq_category == 0) {
      // Early: earlier than all tombstones
      read_seq = (min_seq > 1) ? (min_seq / 2) : 1;
    } else if (seq_category == 1) {
      // Mid: in the middle of tombstones
      read_seq = std::uniform_int_distribution<SequenceNumber>(min_seq, max_seq)(rng);
    } else {
      // Late: later than all tombstones
      read_seq = max_seq + 10;
    }

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

    // Diagnostic assertion wrapper
    try {
      Verify3WayPointwise(local_view, full_truth, oracle, &L, &U, probe_keys,
                          probe_seqs, BytewiseComparator());
      VerifyClippedStreamEquivalence(local_view, full_truth, &L, &U,
                                     bytewise_icmp_);
    } catch (...) {
      std::cerr << "FAILED at trial=" << trial << " seed=" << kMasterSeed
                << " category=" << seq_category << " read_seq=" << read_seq
                << " L=" << l_buf << " U=" << u_buf << "\n";
      throw;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
