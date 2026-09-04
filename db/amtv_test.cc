//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"

#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

class AMTVTest : public testing::Test {
 public:
  AMTVTest() : dbname_(test::PerThreadDBPath("amtv_test")) {}
  ~AMTVTest() override { DestroyDB(dbname_, Options()).PermitUncheckedError(); }

 protected:
  std::string dbname_;
  InternalKeyComparator bytewise_icmp_{BytewiseComparator()};
};

namespace {

std::unique_ptr<InternalIterator> MakeRangeDelIter(
    const std::vector<RangeTombstone>& range_dels,
    const InternalKeyComparator* icmp) {
  std::vector<std::string> keys, values;
  keys.reserve(range_dels.size());
  values.reserve(range_dels.size());
  for (const auto& range_del : range_dels) {
    auto key_and_value = range_del.Serialize();
    keys.push_back(key_and_value.first.Encode().ToString());
    values.push_back(key_and_value.second.ToString());
  }
  return std::make_unique<VectorIterator>(std::move(keys), std::move(values),
                                          icmp);
}

// Differential Oracle Fail-Fast Validator:
// Compares AMTV multi-source adapter (Base + Sealed + Open) against a Ground Truth
// single FragmentedRangeTombstoneList built from the union of all tombstones.
void VerifyDifferentialOracle(
    const std::vector<RangeTombstone>& base_tombstones,
    const std::vector<RangeTombstone>& sealed_tombstones,
    const std::vector<RangeTombstone>& open_tombstones,
    const std::vector<std::string>& probe_keys,
    const std::vector<SequenceNumber>& probe_seqs,
    const InternalKeyComparator& icmp) {
  // 1. Build Ground Truth (union of all tombstones in one native list)
  std::vector<RangeTombstone> all_tombstones;
  all_tombstones.insert(all_tombstones.end(), base_tombstones.begin(),
                        base_tombstones.end());
  all_tombstones.insert(all_tombstones.end(), sealed_tombstones.begin(),
                        sealed_tombstones.end());
  all_tombstones.insert(all_tombstones.end(), open_tombstones.begin(),
                        open_tombstones.end());

  std::unique_ptr<FragmentedRangeTombstoneList> gt_list;
  if (!all_tombstones.empty()) {
    auto unfrag_iter = MakeRangeDelIter(all_tombstones, &icmp);
    gt_list = std::make_unique<FragmentedRangeTombstoneList>(
        std::move(unfrag_iter), icmp);
  }

  // 2. Build AMTV Multi-Source Adapter
  auto snapshot = std::make_shared<AMTVSnapshot>();

  if (!base_tombstones.empty()) {
    auto unfrag = MakeRangeDelIter(base_tombstones, &icmp);
    snapshot->base =
        std::make_unique<FragmentedRangeTombstoneList>(std::move(unfrag), icmp);
  }

  if (!sealed_tombstones.empty()) {
    auto unfrag = MakeRangeDelIter(sealed_tombstones, &icmp);
    snapshot->sealed_delta =
        std::make_unique<FragmentedRangeTombstoneList>(std::move(unfrag), icmp);
  }

  auto open_delta = std::make_shared<OpenDelta>();
  for (const auto& t : open_tombstones) {
    open_delta->AddEntry(t.start_key_, t.end_key_, t.seq_);
  }
  snapshot->open_delta = open_delta;

  AMTVMultiSourceAdapter adapter(snapshot, &icmp);

  // 3. Fail-Fast Point Query Differential Assertions
  for (SequenceNumber seq : probe_seqs) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> gt_iter;
    if (gt_list) {
      gt_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
          gt_list.get(), icmp, seq);
    }

    for (const auto& key : probe_keys) {
      SequenceNumber gt_seq = 0;
      if (gt_iter) {
        gt_seq = gt_iter->MaxCoveringTombstoneSeqnum(key);
      }
      SequenceNumber amtv_seq = adapter.MaxCoveringTombstoneSeqnum(key, seq);

      if (gt_seq != amtv_seq) {
        FAIL() << "[Differential Oracle FAIL-FAST Mismatch] "
               << "Key: \"" << key << "\", read_seq: " << seq
               << ", Expected Ground Truth seq: " << gt_seq
               << ", Actual AMTV Adapter seq: " << amtv_seq;
      }
    }
  }

  // 4. Fail-Fast Aggregator Query Differential Assertions
  for (SequenceNumber seq : probe_seqs) {
    ReadRangeDelAggregator gt_agg(&icmp, seq);
    if (gt_list) {
      gt_agg.AddTombstones(std::make_unique<FragmentedRangeTombstoneIterator>(
          gt_list.get(), icmp, seq));
    }

    ReadRangeDelAggregator amtv_agg(&icmp, seq);
    std::vector<std::unique_ptr<FragmentedRangeTombstoneList>> pinned;
    adapter.AddToRangeDelAggregator(&amtv_agg, seq, &pinned);

    for (const auto& key : probe_keys) {
      ParsedInternalKey pik(key, seq, kTypeValue);
      bool gt_del = gt_agg.ShouldDelete(
          pik, RangeDelPositioningMode::kForwardTraversal);
      bool amtv_del = amtv_agg.ShouldDelete(
          pik, RangeDelPositioningMode::kForwardTraversal);

      if (gt_del != amtv_del) {
        FAIL() << "[Differential Oracle Aggregator FAIL-FAST Mismatch] "
               << "Key: \"" << key << "\", seq: " << seq
               << ", Expected Ground Truth ShouldDelete: " << gt_del
               << ", Actual AMTV Adapter ShouldDelete: " << amtv_del;
      }
    }
  }
}

}  // namespace

// ==========================================================================
// Category 1: Pure Data Structure & Adapter Reconciliation Tests
// ==========================================================================

TEST_F(AMTVTest, OverlapAndNesting) {
  // Base layer: [10, 50) @ seq 10, [70, 90) @ seq 15
  std::vector<RangeTombstone> base = {
      RangeTombstone("key10", "key50", 10),
      RangeTombstone("key70", "key90", 15),
  };
  // Sealed delta: [25, 75) @ seq 30 (cross-overlaps with both base ranges)
  std::vector<RangeTombstone> sealed = {
      RangeTombstone("key25", "key75", 30),
  };
  // Open delta: [40, 60) @ seq 50 (nested), [85, 100) @ seq 25 (partial overlap)
  std::vector<RangeTombstone> open = {
      RangeTombstone("key40", "key60", 50),
      RangeTombstone("key85", "key99", 25),
  };

  std::vector<std::string> probe_keys = {
      "key05", "key10", "key20", "key25", "key35", "key40",
      "key45", "key50", "key55", "key60", "key65", "key70",
      "key75", "key80", "key85", "key90", "key95", "key99",
  };
  std::vector<SequenceNumber> probe_seqs = {5, 12, 20, 35, 60, 100};

  VerifyDifferentialOracle(base, sealed, open, probe_keys, probe_seqs,
                           bytewise_icmp_);
}

TEST_F(AMTVTest, AdjacentBoundaries) {
  // Exactly adjacent boundaries: [10, 30), [30, 60), [60, 90)
  std::vector<RangeTombstone> base = {
      RangeTombstone("key10", "key30", 20),
  };
  std::vector<RangeTombstone> sealed = {
      RangeTombstone("key30", "key60", 40),
  };
  std::vector<RangeTombstone> open = {
      RangeTombstone("key60", "key90", 10),
  };

  std::vector<std::string> probe_keys = {
      "key09", "key10", "key29", "key30", "key31",
      "key59", "key60", "key61", "key89", "key90", "key91",
  };
  std::vector<SequenceNumber> probe_seqs = {5, 15, 25, 45, 100};

  VerifyDifferentialOracle(base, sealed, open, probe_keys, probe_seqs,
                           bytewise_icmp_);
}

TEST_F(AMTVTest, SequenceShadowingAndInversion) {
  // Identical interval [20, 80) present in all three layers with different seqs
  std::vector<RangeTombstone> base = {
      RangeTombstone("key20", "key80", 10),
  };
  std::vector<RangeTombstone> sealed = {
      RangeTombstone("key20", "key80", 50),
  };
  std::vector<RangeTombstone> open = {
      RangeTombstone("key20", "key80", 30),
  };

  std::vector<std::string> probe_keys = {"key10", "key20", "key50", "key79",
                                         "key80", "key90"};
  std::vector<SequenceNumber> probe_seqs = {5, 10, 20, 30, 40, 50, 60};

  VerifyDifferentialOracle(base, sealed, open, probe_keys, probe_seqs,
                           bytewise_icmp_);
}

TEST_F(AMTVTest, EmptyLayersCombinations) {
  // Test permutations where some layers are empty
  std::vector<RangeTombstone> single = {
      RangeTombstone("a", "m", 100),
  };
  std::vector<std::string> probe_keys = {"a", "g", "m", "z"};
  std::vector<SequenceNumber> probe_seqs = {50, 150};

  // Base only
  VerifyDifferentialOracle(single, {}, {}, probe_keys, probe_seqs,
                           bytewise_icmp_);
  // Sealed only
  VerifyDifferentialOracle({}, single, {}, probe_keys, probe_seqs,
                           bytewise_icmp_);
  // Open only
  VerifyDifferentialOracle({}, {}, single, probe_keys, probe_seqs,
                           bytewise_icmp_);
  // All empty
  VerifyDifferentialOracle({}, {}, {}, probe_keys, probe_seqs, bytewise_icmp_);
}

TEST_F(AMTVTest, FreezeBoundaryCheck65) {
  // Test threshold freeze behavior at exactly 64 tombstones
  AMTVState state(0 /* generation */, 64 /* delta limit */);

  // Add 63 tombstones: open delta has 63, sealed delta is nullptr
  for (int i = 0; i < 63; ++i) {
    char start_buf[32], end_buf[32];
    snprintf(start_buf, sizeof(start_buf), "k%05d", i * 10);
    snprintf(end_buf, sizeof(end_buf), "k%05d", i * 10 + 5);
    state.AddTombstone(start_buf, end_buf, i + 1, bytewise_icmp_);
  }
  {
    auto snap = state.GetSnapshot();
    ASSERT_EQ(snap->open_delta->size(), 63U);
    ASSERT_EQ(snap->sealed_delta, nullptr);
  }

  // Add 64th tombstone: must freeze open delta into sealed delta
  state.AddTombstone("k00630", "k00635", 64, bytewise_icmp_);
  {
    auto snap = state.GetSnapshot();
    ASSERT_NE(snap->sealed_delta, nullptr);
    ASSERT_EQ(snap->sealed_delta->num_unfragmented_tombstones(), 64U);
    ASSERT_EQ(snap->open_delta->size(), 0U);
  }

  // Add 65th tombstone: open delta has 1 tombstone, sealed delta remains 64
  state.AddTombstone("k00640", "k00645", 65, bytewise_icmp_);
  {
    auto snap = state.GetSnapshot();
    ASSERT_NE(snap->sealed_delta, nullptr);
    ASSERT_EQ(snap->sealed_delta->num_unfragmented_tombstones(), 64U);
    ASSERT_EQ(snap->open_delta->size(), 1U);
    ASSERT_EQ(snap->total_tombstones(), 65U);
  }
}

// ==========================================================================
// Category 2: DB Semantics Reconciliation Tests
// ==========================================================================

// ==========================================================================
// Category 2: DB Semantics Reconciliation Tests (3-Way Truth Verification)
// AMTV Shadow Adapter == Native Full Ground Truth == Native DB Get / Iterator
// ==========================================================================

TEST_F(AMTVTest, DBDeleteRangePutResurrection) {
  Options options;
  options.create_if_missing = true;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  // Shadow AMTV state and ground truth tombstone list
  AMTVState shadow_amtv(0 /* generation */, 64 /* delta limit */);
  std::vector<RangeTombstone> gt_tombstones;
  std::unordered_map<std::string, SequenceNumber> put_seqs;

  // 1. Put keys k10 .. k90
  for (int i = 1; i <= 9; ++i) {
    std::string k = "k" + std::to_string(i * 10);
    ASSERT_OK(db->Put(WriteOptions(), k, "val_" + k));
    put_seqs[k] = db->GetLatestSequenceNumber();
  }

  // 2. DeleteRange [k20, k80)
  ASSERT_OK(db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), "k20",
                            "k80"));
  SequenceNumber del_seq = db->GetLatestSequenceNumber();
  shadow_amtv.AddTombstone("k20", "k80", del_seq, bytewise_icmp_);
  gt_tombstones.emplace_back("k20", "k80", del_seq);

  // 3. Put resurrection: Put k50 = "resurrected_50"
  ASSERT_OK(db->Put(WriteOptions(), "k50", "resurrected_50"));
  put_seqs["k50"] = db->GetLatestSequenceNumber();

  SequenceNumber current_seq = db->GetLatestSequenceNumber();

  // Construct Ground Truth single list
  auto unfrag_iter = MakeRangeDelIter(gt_tombstones, &bytewise_icmp_);
  FragmentedRangeTombstoneList gt_list(std::move(unfrag_iter), bytewise_icmp_);
  FragmentedRangeTombstoneIterator gt_iter(&gt_list, bytewise_icmp_, current_seq);

  // Construct Shadow AMTV MultiSourceAdapter
  AMTVMultiSourceAdapter shadow_adapter(shadow_amtv.GetSnapshot(),
                                        &bytewise_icmp_);

  // 4. Verify 3-Way Truth across all keys k10 .. k90:
  // AMTV Shadow Adapter == Native Ground Truth == DB Get
  for (int i = 1; i <= 9; ++i) {
    std::string k = "k" + std::to_string(i * 10);
    SequenceNumber gt_seq = gt_iter.MaxCoveringTombstoneSeqnum(k);
    SequenceNumber amtv_seq =
        shadow_adapter.MaxCoveringTombstoneSeqnum(k, current_seq);

    // Assert 1: AMTV shadow adapter output == Native full Ground Truth output
    ASSERT_EQ(gt_seq, amtv_seq)
        << "[3-Way Mismatch AMTV vs GT] Key: " << k;

    // Assert 2: Ground Truth / AMTV covering seqnum determines DB Get visibility
    std::string val;
    Status s = db->Get(ReadOptions(), k, &val);
    SequenceNumber put_seq = put_seqs[k];

    if (gt_seq > put_seq) {
      ASSERT_TRUE(s.IsNotFound())
          << "Key " << k << " should be deleted by range tombstone @ seq "
          << gt_seq << ", put seq was " << put_seq;
    } else {
      ASSERT_OK(s) << "Key " << k << " should be visible with put seq "
                   << put_seq;
      if (k == "k50") {
        ASSERT_EQ(val, "resurrected_50");
      } else {
        ASSERT_EQ(val, "val_" + k);
      }
    }
  }

  // 5. Verify Iterator matches Point Get exactly
  std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
  std::vector<std::pair<std::string, std::string>> seen;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    seen.emplace_back(it->key().ToString(), it->value().ToString());
  }
  ASSERT_OK(it->status());

  std::vector<std::pair<std::string, std::string>> expected = {
      {"k10", "val_k10"},
      {"k50", "resurrected_50"},
      {"k80", "val_k80"},
      {"k90", "val_k90"},
  };
  ASSERT_EQ(seen, expected);
}

TEST_F(AMTVTest, DBSnapshotBoundary) {
  Options options;
  options.create_if_missing = true;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  AMTVState shadow_amtv(0 /* generation */, 64 /* delta limit */);
  std::vector<RangeTombstone> gt_tombstones;

  ASSERT_OK(db->Put(WriteOptions(), "k1", "v1"));
  ASSERT_OK(db->Put(WriteOptions(), "k2", "v2"));
  ASSERT_OK(db->Put(WriteOptions(), "k3", "v3"));

  // Snapshot 1: before range deletion
  const Snapshot* snap1 = db->GetSnapshot();
  SequenceNumber snap1_seq = snap1->GetSequenceNumber();

  // Range delete [k2, k4)
  ASSERT_OK(
      db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), "k2", "k4"));
  SequenceNumber del_seq = db->GetLatestSequenceNumber();
  shadow_amtv.AddTombstone("k2", "k4", del_seq, bytewise_icmp_);
  gt_tombstones.emplace_back("k2", "k4", del_seq);

  // Snapshot 2: after range deletion
  const Snapshot* snap2 = db->GetSnapshot();
  SequenceNumber snap2_seq = snap2->GetSequenceNumber();

  auto unfrag_iter = MakeRangeDelIter(gt_tombstones, &bytewise_icmp_);
  FragmentedRangeTombstoneList gt_list(std::move(unfrag_iter), bytewise_icmp_);
  AMTVMultiSourceAdapter shadow_adapter(shadow_amtv.GetSnapshot(),
                                        &bytewise_icmp_);

  // 3-Way Reconciliation under Snapshot 1
  {
    FragmentedRangeTombstoneIterator gt_iter(&gt_list, bytewise_icmp_,
                                            snap1_seq);
    ReadOptions ro_snap1;
    ro_snap1.snapshot = snap1;
    for (const char* k : {"k1", "k2", "k3"}) {
      SequenceNumber gt_seq = gt_iter.MaxCoveringTombstoneSeqnum(k);
      SequenceNumber amtv_seq =
          shadow_adapter.MaxCoveringTombstoneSeqnum(k, snap1_seq);
      ASSERT_EQ(gt_seq, amtv_seq)
          << "Under Snap1 AMTV vs GT mismatch for " << k;
      ASSERT_EQ(amtv_seq, 0U)
          << "Under Snap1 tombstone must not be visible for " << k;

      std::string val;
      ASSERT_OK(db->Get(ro_snap1, k, &val));
    }
  }

  // 3-Way Reconciliation under Snapshot 2
  {
    FragmentedRangeTombstoneIterator gt_iter(&gt_list, bytewise_icmp_,
                                            snap2_seq);
    ReadOptions ro_snap2;
    ro_snap2.snapshot = snap2;
    for (const char* k : {"k1", "k2", "k3"}) {
      SequenceNumber gt_seq = gt_iter.MaxCoveringTombstoneSeqnum(k);
      SequenceNumber amtv_seq =
          shadow_adapter.MaxCoveringTombstoneSeqnum(k, snap2_seq);
      ASSERT_EQ(gt_seq, amtv_seq)
          << "Under Snap2 AMTV vs GT mismatch for " << k;

      std::string val;
      Status s = db->Get(ro_snap2, k, &val);
      if (std::string(k) == "k1") {
        ASSERT_EQ(amtv_seq, 0U);
        ASSERT_OK(s);
      } else {
        ASSERT_EQ(amtv_seq, del_seq);
        ASSERT_TRUE(s.IsNotFound());
      }
    }
  }

  db->ReleaseSnapshot(snap1);
  db->ReleaseSnapshot(snap2);
}

TEST_F(AMTVTest, DBKeyByKeyValueReconciliation) {
  Options options;
  options.create_if_missing = true;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  // Use a small delta limit (8) to force multiple open -> sealed freezes
  AMTVState shadow_amtv(0 /* generation */, 8 /* delta limit */);
  std::vector<std::string> owned_starts;
  std::vector<std::string> owned_ends;
  owned_starts.reserve(15);
  owned_ends.reserve(15);
  std::vector<RangeTombstone> gt_tombstones;
  std::unordered_map<std::string, SequenceNumber> put_seqs;
  std::unordered_map<std::string, std::string> put_vals;

  // Perform 40 operations alternating Puts and DeleteRanges with zero-padded keys
  for (int i = 0; i < 40; ++i) {
    char k_buf[32];
    snprintf(k_buf, sizeof(k_buf), "k%04d", i);
    if (i % 4 == 0) {
      char end_buf[32];
      snprintf(end_buf, sizeof(end_buf), "k%04d", i + 15);
      ASSERT_OK(db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(),
                                k_buf, end_buf));
      SequenceNumber del_seq = db->GetLatestSequenceNumber();
      shadow_amtv.AddTombstone(k_buf, end_buf, del_seq, bytewise_icmp_);
      owned_starts.emplace_back(k_buf);
      owned_ends.emplace_back(end_buf);
      gt_tombstones.emplace_back(owned_starts.back(), owned_ends.back(),
                                 del_seq);
    } else {
      std::string val = "v_" + std::string(k_buf);
      ASSERT_OK(db->Put(WriteOptions(), k_buf, val));
      SequenceNumber pseq = db->GetLatestSequenceNumber();
      put_seqs[k_buf] = pseq;
      put_vals[k_buf] = val;
    }
  }

  SequenceNumber current_seq = db->GetLatestSequenceNumber();
  auto unfrag_iter = MakeRangeDelIter(gt_tombstones, &bytewise_icmp_);
  FragmentedRangeTombstoneList gt_list(std::move(unfrag_iter), bytewise_icmp_);
  FragmentedRangeTombstoneIterator gt_iter(&gt_list, bytewise_icmp_,
                                          current_seq);
  AMTVMultiSourceAdapter shadow_adapter(shadow_amtv.GetSnapshot(),
                                        &bytewise_icmp_);

  // 3-Way Reconciliation across all 40 keys:
  // AMTV shadow adapter output == Native full Ground Truth output == Native DB Get / Iterator visible result
  for (int i = 0; i < 40; ++i) {
    char k_buf[32];
    snprintf(k_buf, sizeof(k_buf), "k%04d", i);
    std::string key(k_buf);

    SequenceNumber gt_seq = gt_iter.MaxCoveringTombstoneSeqnum(key);
    SequenceNumber amtv_seq =
        shadow_adapter.MaxCoveringTombstoneSeqnum(key, current_seq);

    // Assert 1: AMTV shadow adapter output == Native Ground Truth output
    ASSERT_EQ(gt_seq, amtv_seq)
        << "[3-Way Reconciliation Mismatch] Key " << key;

    // Assert 2: Ground Truth / AMTV output == DB Get result
    std::string val_get;
    Status s = db->Get(ReadOptions(), key, &val_get);

    auto pseq_it = put_seqs.find(key);
    if (pseq_it == put_seqs.end() || gt_seq > pseq_it->second) {
      // Key was either never put, or deleted by a range tombstone with gt_seq > pseq
      ASSERT_TRUE(s.IsNotFound())
          << "Key " << key << " should be NotFound, gt_seq=" << gt_seq;
    } else {
      // Key must be visible with latest put value
      ASSERT_OK(s) << "Key " << key << " should be found!";
      ASSERT_EQ(val_get, put_vals[key]);
    }
  }

  // Scan all keys and verify Get vs Iterator consistency bit-for-bit
  std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    std::string val_iter = it->value().ToString();
    std::string val_get;
    Status s = db->Get(ReadOptions(), key, &val_get);
    ASSERT_OK(s) << "Key " << key << " visible in Iterator but not Get!";
    ASSERT_EQ(val_iter, val_get)
        << "Value mismatch for key " << key << " between Get and Iterator!";
  }
  ASSERT_OK(it->status());
}


}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
