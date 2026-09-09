//  Copyright (c) 2026-present. All rights reserved.
//  Test-only implementation and test suite for AMTV M4-P1a: Local Scan Semantic Reference Prototype.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "db/amtv.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

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
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key, has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(), has_ts) < 0) {
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
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key, has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(), has_ts) < 0) {
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
      if (ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound, false) >= 0) {
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
    // 1. Full safe traversal of sealed runs raw entries
    for (const auto& run : runs_raw) {
      for (const auto& entry : run) {
        num_total_++;
        if (IsIntersecting(entry, lower_bound, upper_bound, ucmp)) {
          selected.push_back(entry);
        }
      }
    }
    // 2. Full safe traversal of open delta raw entries
    for (const auto& entry : delta_raw) {
      num_total_++;
      if (IsIntersecting(entry, lower_bound, upper_bound, ucmp)) {
        selected.push_back(entry);
      }
    }
    num_selected_ = selected.size();

    // 3. Sort selected raw entries by icmp
    std::sort(selected.begin(), selected.end(),
              [this](const OpenDeltaEntry& a, const OpenDeltaEntry& b) {
                return icmp_.Compare(a.ikey.Encode(), b.ikey.Encode()) < 0;
              });

    // 4. Feed selected entries to native FragmentedRangeTombstoneList
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
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key, has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(), has_ts) < 0) {
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
    if (ucmp->CompareWithoutTimestamp(iter_->start_key(), has_ts, user_key, has_ts) <= 0 &&
        ucmp->CompareWithoutTimestamp(user_key, has_ts, iter_->end_key(), has_ts) < 0) {
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
// Test Fixture
// ==========================================================================
class AMTVLocalScanReferenceTest : public testing::Test {
 public:
  AMTVLocalScanReferenceTest()
      : bytewise_icmp_(BytewiseComparator()) {}

 protected:
  InternalKeyComparator bytewise_icmp_;

  void VerifyLocalMatchesFullTruth(
      const AMTVLocalScanReferenceView& local_view,
      AMTVCanonicalFullTruth& full_truth,
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
      ASSERT_EQ(local_seq, full_seq) << "Mismatch for probe key: " << key;

      if (has_ts) {
        Slice full_ts = full_truth.CoveringTimestamp(key);
        Slice local_ts = local_view.CoveringTimestamp(key);
        ASSERT_EQ(local_ts.ToString(), full_ts.ToString())
            << "Timestamp mismatch for key: " << key;
      }

      for (SequenceNumber pseq : probe_seqs) {
        bool full_del = full_truth.ShouldDelete(key, pseq);
        bool local_del = local_view.ShouldDelete(key, pseq);
        ASSERT_EQ(local_del, full_del)
            << "ShouldDelete mismatch for key: " << key << " at seq: " << pseq;

        bool full_vis = full_truth.IsPointVisible(key, pseq);
        bool local_vis = local_view.IsPointVisible(key, pseq);
        ASSERT_EQ(local_vis, full_vis)
            << "Visibility mismatch for key: " << key << " at seq: " << pseq;
      }
    }
  }
};

// --------------------------------------------------------------------------
// 1. Left Boundary Spanning: start < L < end < U
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, LeftBoundarySpanning) {
  // Tombstone starts before L and ends inside [L, U)
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k10", "k60", 100)},
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k30";
  std::string U_str = "k80";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_,
                                       200);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 200);

  EXPECT_EQ(local_view.num_selected_raw_entries(), 1U);

  std::vector<std::string> probe_keys = {"k20", "k30", "k45", "k59",
                                         "k60", "k70", "k80", "k90"};
  std::vector<SequenceNumber> probe_seqs = {50, 100, 150};

  VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                              probe_seqs, BytewiseComparator());
}

// --------------------------------------------------------------------------
// 2. Right Boundary Spanning: L < start < U < end
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, RightBoundarySpanning) {
  // Tombstone starts inside [L, U) and ends after U
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k40", "k90", 100)},
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k20";
  std::string U_str = "k70";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_,
                                       200);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 200);

  EXPECT_EQ(local_view.num_selected_raw_entries(), 1U);

  std::vector<std::string> probe_keys = {"k15", "k20", "k30", "k40",
                                         "k55", "k69", "k70", "k85"};
  std::vector<SequenceNumber> probe_seqs = {50, 100, 150};

  VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                              probe_seqs, BytewiseComparator());
}

// --------------------------------------------------------------------------
// 3. Fully Spanning: start < L < U < end
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, FullySpanning) {
  // Long tombstone engulfing the entire query window
  std::vector<std::vector<OpenDeltaEntry>> runs = {
      {OpenDeltaEntry("k05", "k95", 100)},
  };
  std::vector<OpenDeltaEntry> delta;

  std::string L_str = "k20";
  std::string U_str = "k70";
  Slice L(L_str);
  Slice U(U_str);

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_,
                                       200);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 200);

  EXPECT_EQ(local_view.num_selected_raw_entries(), 1U);

  std::vector<std::string> probe_keys = {"k10", "k20", "k35", "k50",
                                         "k69", "k70", "k90"};
  std::vector<SequenceNumber> probe_seqs = {50, 100, 150};

  VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                              probe_seqs, BytewiseComparator());
}

// --------------------------------------------------------------------------
// 4. Overlap, Nesting, and Adjacent Boundaries
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, OverlapNestingAdjacent) {
  // Run with nesting: [k10, k90) containing [k30, k60) containing [k40, k50)
  // Overlap: [k20, k55)
  // Adjacent: [k70, k80) and [k80, k95)
  // Outside: [k00, k05) and [k96, k99)
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

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_,
                                       250);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 250);

  // k00-k05 and k96-k99 must be pruned; remaining 6 entries must be selected
  EXPECT_EQ(local_view.num_total_raw_entries(), 8U);
  EXPECT_EQ(local_view.num_selected_raw_entries(), 6U);

  std::vector<std::string> probe_keys = {
      "k02", "k15", "k25", "k28", "k30", "k35", "k40", "k45",
      "k50", "k52", "k55", "k58", "k65", "k70", "k75", "k80",
      "k82", "k84", "k85", "k90", "k97",
  };
  std::vector<SequenceNumber> probe_seqs = {40, 60, 75, 85, 100, 150, 220};

  VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                              probe_seqs, BytewiseComparator());
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

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_,
                                       150);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 150);

  std::vector<std::string> probe_keys = {
      "k20", "k25", "k30", "k35", "k40", "k45", "k50",
      "k55", "k60", "k65", "k70", "k74", "k75", "k80",
  };
  std::vector<SequenceNumber> probe_seqs = {20, 50, 70, 90, 100, 120};

  VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                              probe_seqs, BytewiseComparator());
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

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_,
                                       100);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 100);

  // Candidate point versions:
  // (k30, seq 30) -> covered by tombstone seq 50 (50 > 30) -> deleted
  // (k30, seq 70) -> point seq 70 > tombstone seq 50 -> resurrected!
  // (k50, seq 40) -> covered by tombstone seq 50 (50 > 40) -> deleted
  // (k50, seq 50) -> 50 > 50 is false -> visible (RocksDB MVCC: tombstone does not delete point with equal seq)
  // (k50, seq 80) -> point seq 80 > tombstone seq 50 -> resurrected!
  // (k80, seq 40) -> outside tombstone -> visible

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

  // Compare with Full Truth
  std::vector<std::string> probe_keys = {"k25", "k30", "k50", "k70", "k74"};
  std::vector<SequenceNumber> probe_seqs = {30, 50, 70, 80};
  VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                              probe_seqs, BytewiseComparator());
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
    EXPECT_EQ(view_s1.MaxCoveringTombstoneSeqnum("k50"), 40U);
    EXPECT_EQ(truth_s1.MaxCoveringTombstoneSeqnum("k50"), 40U);
  }

  // Snapshot 2 at seq 80 (sees run 0 @ 40 and run 1 @ 70)
  {
    AMTVLocalScanReferenceView view_s2(runs, delta, &L, &U, bytewise_icmp_, 80);
    AMTVCanonicalFullTruth truth_s2(runs, delta, bytewise_icmp_, 80);
    EXPECT_EQ(view_s2.MaxCoveringTombstoneSeqnum("k50"), 70U);
    EXPECT_EQ(truth_s2.MaxCoveringTombstoneSeqnum("k50"), 70U);
  }

  // Snapshot 3 at seq 120 (sees all three: delta @ 100)
  {
    AMTVLocalScanReferenceView view_s3(runs, delta, &L, &U, bytewise_icmp_, 120);
    AMTVCanonicalFullTruth truth_s3(runs, delta, bytewise_icmp_, 120);
    EXPECT_EQ(view_s3.MaxCoveringTombstoneSeqnum("k50"), 100U);
    EXPECT_EQ(truth_s3.MaxCoveringTombstoneSeqnum("k50"), 100U);
  }
}

// --------------------------------------------------------------------------
// 8. User Defined Timestamp Enabled
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, UserDefinedTimestamp) {
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  ASSERT_NE(ucmp, nullptr);
  InternalKeyComparator ts_icmp(ucmp);

  const size_t ts_sz = sizeof(uint64_t);
  std::string dummy_ts(ts_sz, '\0');

  std::string ts100, ts200;
  PutFixed64(&ts100, 100);
  PutFixed64(&ts200, 200);

  // Run 0: [k10, k60) @ seq 50, ts 100
  // Delta: [k30, k80) @ seq 80, ts 200
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
    AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, ts_icmp, 100,
                                         nullptr);
    AMTVCanonicalFullTruth full_truth(runs, delta, ts_icmp, 100, nullptr);

    std::vector<std::string> probe_keys = {
        "k15" + dummy_ts, "k25" + dummy_ts, "k35" + dummy_ts,
        "k50" + dummy_ts, "k65" + dummy_ts, "k75" + dummy_ts,
    };
    std::vector<SequenceNumber> probe_seqs = {40, 60, 90};

    VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                                probe_seqs, ucmp);
  }

  // Case 8b: ts_upper_bound = 150 (Delta with ts 200 must be filtered out!)
  {
    std::string ts150;
    PutFixed64(&ts150, 150);
    Slice ts_ub(ts150);

    AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, ts_icmp, 100,
                                         &ts_ub);
    AMTVCanonicalFullTruth full_truth(runs, delta, ts_icmp, 100, &ts_ub);

    std::vector<std::string> probe_keys = {
        "k15" + dummy_ts, "k25" + dummy_ts, "k35" + dummy_ts,
        "k50" + dummy_ts, "k65" + dummy_ts, "k75" + dummy_ts,
    };
    std::vector<SequenceNumber> probe_seqs = {40, 60, 90};

    VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                                probe_seqs, ucmp);

    // Specifically verify k50: ts 200 tombstone invisible, fallback to seq 50 with ts 100
    EXPECT_EQ(local_view.MaxCoveringTombstoneSeqnum("k50" + dummy_ts), 50U);
    EXPECT_EQ(full_truth.MaxCoveringTombstoneSeqnum("k50" + dummy_ts), 50U);
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
  }

  // Case 9b: Short window completely in gap between tombstones
  {
    std::string L_str = "k00";
    std::string U_str = "k10";
    Slice L(L_str);
    Slice U(U_str);
    AMTVLocalScanReferenceView view(runs, delta, &L, &U, bytewise_icmp_, 100);
    EXPECT_EQ(view.num_selected_raw_entries(), 0U);
    EXPECT_TRUE(view.empty());
  }

  // Case 9c: Full window (L = nullptr, U = nullptr)
  {
    AMTVLocalScanReferenceView view(runs, delta, nullptr, nullptr,
                                    bytewise_icmp_, 100);
    AMTVCanonicalFullTruth truth(runs, delta, bytewise_icmp_, 100);
    EXPECT_EQ(view.num_selected_raw_entries(), 3U);
    EXPECT_EQ(view.num_total_raw_entries(), 3U);

    std::vector<std::string> probe_keys = {"k10", "k25", "k45", "k65", "k85",
                                           "k95"};
    std::vector<SequenceNumber> probe_seqs = {40, 65, 80};
    VerifyLocalMatchesFullTruth(view, truth, nullptr, nullptr, probe_keys,
                                probe_seqs, BytewiseComparator());
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

  AMTVLocalScanReferenceView local_view(runs, delta, &L, &U, bytewise_icmp_,
                                       100);
  AMTVCanonicalFullTruth full_truth(runs, delta, bytewise_icmp_, 100);

  // Forward Seek and Next
  local_view.Seek(L);
  full_truth.iter()->Seek(L);
  while (local_view.Valid() && full_truth.iter()->Valid()) {
    if (BytewiseComparator()->Compare(full_truth.iter()->start_key(), U) >= 0) {
      break;
    }
    ASSERT_EQ(local_view.seq(), full_truth.iter()->seq());
    ASSERT_EQ(local_view.start_key().user_key.ToString(),
              full_truth.iter()->parsed_start_key().user_key.ToString());
    ASSERT_EQ(local_view.end_key().user_key.ToString(),
              full_truth.iter()->parsed_end_key().user_key.ToString());
    local_view.Next();
    full_truth.iter()->TopNext();
  }

  // Reverse SeekForPrev and Prev
  local_view.SeekForPrev(U);
  full_truth.iter()->SeekForPrev(U);
  while (local_view.Valid() && full_truth.iter()->Valid()) {
    if (BytewiseComparator()->Compare(full_truth.iter()->end_key(), L) <= 0) {
      break;
    }
    ASSERT_EQ(local_view.seq(), full_truth.iter()->seq());
    ASSERT_EQ(local_view.start_key().user_key.ToString(),
              full_truth.iter()->parsed_start_key().user_key.ToString());
    ASSERT_EQ(local_view.end_key().user_key.ToString(),
              full_truth.iter()->parsed_end_key().user_key.ToString());
    local_view.Prev();
    full_truth.iter()->TopPrev();
  }
}

// --------------------------------------------------------------------------
// 11. Randomized Differential Testing (1,000 Trials)
// --------------------------------------------------------------------------
TEST_F(AMTVLocalScanReferenceTest, RandomizedDifferential1000Trials) {
  std::mt19937_64 rng(1337);

  for (int trial = 0; trial < 1000; ++trial) {
    int num_runs = std::uniform_int_distribution<int>(0, 4)(rng);
    std::vector<std::vector<OpenDeltaEntry>> runs;
    SequenceNumber next_seq = 10;

    for (int r = 0; r < num_runs; ++r) {
      int count = std::uniform_int_distribution<int>(1, 8)(rng);
      std::vector<OpenDeltaEntry> run_entries;
      for (int i = 0; i < count; ++i) {
        int k1 = std::uniform_int_distribution<int>(0, 80)(rng);
        int len = std::uniform_int_distribution<int>(5, 40)(rng);
        int k2 = k1 + len;
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
      int k1 = std::uniform_int_distribution<int>(0, 80)(rng);
      int len = std::uniform_int_distribution<int>(5, 40)(rng);
      int k2 = k1 + len;
      char buf1[16], buf2[16];
      snprintf(buf1, sizeof(buf1), "key%04d", k1);
      snprintf(buf2, sizeof(buf2), "key%04d", k2);
      next_seq += std::uniform_int_distribution<SequenceNumber>(1, 10)(rng);
      delta_entries.emplace_back(buf1, buf2, next_seq);
    }

    // Random window [L, U)
    int l_idx = std::uniform_int_distribution<int>(0, 60)(rng);
    int u_idx = l_idx + std::uniform_int_distribution<int>(5, 60)(rng);
    char l_buf[16], u_buf[16];
    snprintf(l_buf, sizeof(l_buf), "key%04d", l_idx);
    snprintf(u_buf, sizeof(u_buf), "key%04d", u_idx);
    Slice L(l_buf);
    Slice U(u_buf);

    SequenceNumber read_seq = next_seq + 10;

    AMTVLocalScanReferenceView local_view(runs, delta_entries, &L, &U,
                                         bytewise_icmp_, read_seq);
    AMTVCanonicalFullTruth full_truth(runs, delta_entries, bytewise_icmp_,
                                      read_seq);

    // Probe keys inside [L, U)
    std::vector<std::string> probe_keys;
    for (int p = l_idx; p < u_idx; p += 3) {
      char p_buf[16];
      snprintf(p_buf, sizeof(p_buf), "key%04d", p);
      probe_keys.emplace_back(p_buf);
    }
    std::vector<SequenceNumber> probe_seqs = {1, next_seq / 2, next_seq};

    VerifyLocalMatchesFullTruth(local_view, full_truth, &L, &U, probe_keys,
                                probe_seqs, BytewiseComparator());
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
