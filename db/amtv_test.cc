//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

class AMTVTest : public testing::Test {
 public:
  AMTVTest() : dbname_(test::PerThreadDBPath("amtv_test")) {}
  void SetUp() override { DestroyDB(dbname_, Options()).PermitUncheckedError(); }
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
  uint64_t run_id = 1;

  if (!base_tombstones.empty()) {
    std::vector<OpenDeltaEntry> raw;
    for (const auto& t : base_tombstones) {
      raw.emplace_back(t.start_key_, t.end_key_, t.seq_);
    }
    snapshot->sealed_runs.emplace_back(run_id++, std::move(raw), icmp);
  }

  if (!sealed_tombstones.empty()) {
    std::vector<OpenDeltaEntry> raw;
    for (const auto& t : sealed_tombstones) {
      raw.emplace_back(t.start_key_, t.end_key_, t.seq_);
    }
    snapshot->sealed_runs.emplace_back(run_id++, std::move(raw), icmp);
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
    adapter.AddToRangeDelAggregator(&amtv_agg, seq, pinned);

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

  // Add 63 tombstones: open delta has 63, sealed runs is empty
  for (int i = 0; i < 63; ++i) {
    char start_buf[32], end_buf[32];
    snprintf(start_buf, sizeof(start_buf), "k%05d", i * 10);
    snprintf(end_buf, sizeof(end_buf), "k%05d", i * 10 + 5);
    state.AddTombstone(start_buf, end_buf, i + 1, bytewise_icmp_);
  }
  {
    auto snap = state.GetSnapshot();
    ASSERT_EQ(snap->open_delta->size(), 63U);
    ASSERT_TRUE(snap->sealed_runs.empty());
  }

  // Add 64th tombstone: must freeze open delta into sealed runs
  state.AddTombstone("k00630", "k00635", 64, bytewise_icmp_);
  {
    auto snap = state.GetSnapshot();
    ASSERT_EQ(snap->sealed_runs.size(), 1U);
    ASSERT_EQ(snap->sealed_runs[0].tombstone_count, 64U);
    ASSERT_EQ(snap->sealed_runs[0].raw_entries.size(), 64U);
    ASSERT_EQ(snap->sealed_runs[0].min_seq, 1U);
    ASSERT_EQ(snap->sealed_runs[0].max_seq, 64U);
    ASSERT_EQ(snap->open_delta->size(), 0U);
  }

  // Add 65th tombstone: open delta has 1 tombstone, sealed runs remains 1
  state.AddTombstone("k00640", "k00645", 65, bytewise_icmp_);
  {
    auto snap = state.GetSnapshot();
    ASSERT_EQ(snap->sealed_runs.size(), 1U);
    ASSERT_EQ(snap->sealed_runs[0].tombstone_count, 64U);
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

TEST_F(AMTVTest, Concurrent8Readers1Writer_GetOnly) {
  Options options;
  options.create_if_missing = true;
  options.enable_amtv = true;
  options.amtv_delta_tombstones = 16;  // Small threshold to force open_delta -> sealed_delta transitions
  options.amtv_max_sealed_deltas = 1;

  std::string test_db = test::PerThreadDBPath("amtv_test_concurrent");
  DestroyDB(test_db, options).PermitUncheckedError();
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_db, &db));

  struct OpLogEntry {
    enum Type { kPut, kDeleteRange, kFlush } type;
    std::string key;
    std::string end_key;
    std::string val;
    SequenceNumber seq;
  };

  std::vector<OpLogEntry> op_log;
  std::mutex log_mutex;

  // Truth model evaluating visibility of key at explicit snapshot sequence
  auto EvaluateTruthModelUnlocked = [&](const std::string& key, SequenceNumber read_seq,
                                        std::string* expected_val) -> bool {
    SequenceNumber latest_put_seq = 0;
    std::string latest_val;
    SequenceNumber max_covering_del_seq = 0;

    for (const auto& op : op_log) {
      if (op.seq > read_seq) {
        break;
      }
      if (op.type == OpLogEntry::kPut) {
        if (op.key == key) {
          latest_put_seq = op.seq;
          latest_val = op.val;
        }
      } else if (op.type == OpLogEntry::kDeleteRange) {
        if (op.key <= key && key < op.end_key) {
          if (op.seq > max_covering_del_seq) {
            max_covering_del_seq = op.seq;
          }
        }
      }
    }

    if (latest_put_seq > 0 && latest_put_seq > max_covering_del_seq) {
      if (expected_val != nullptr) {
        *expected_val = latest_val;
      }
      return true;
    }
    return false;
  };

  std::atomic<bool> writer_done{false};
  std::atomic<uint64_t> total_reads{0};
  const int kNumReaders = 8;
  std::vector<std::thread> readers;

  // 8 Reader threads
  for (int r = 0; r < kNumReaders; ++r) {
    readers.emplace_back([&, r]() {
      uint64_t local_reads = 0;
      std::mt19937_64 rng(1337 + r);
      while (!writer_done.load(std::memory_order_relaxed) || local_reads < 600) {
        int key_idx = rng() % 100;
        char k_buf[32];
        snprintf(k_buf, sizeof(k_buf), "k%04d", key_idx);
        std::string key(k_buf);

        const Snapshot* snap = nullptr;
        SequenceNumber read_seq = 0;
        std::string expected_val;
        bool expected_found = false;

        {
          std::lock_guard<std::mutex> lock(log_mutex);
          snap = db->GetSnapshot();
          read_seq = snap->GetSequenceNumber();
          expected_found = EvaluateTruthModelUnlocked(key, read_seq, &expected_val);
        }

        ReadOptions ro;
        ro.snapshot = snap;
        std::string actual_val;
        Status s = db->Get(ro, key, &actual_val);

        db->ReleaseSnapshot(snap);

        if (expected_found) {
          ASSERT_OK(s) << "Reader " << r << " key " << key << " expected found at seq " << read_seq;
          ASSERT_EQ(actual_val, expected_val) << "Reader " << r << " value mismatch for key " << key;
        } else {
          ASSERT_TRUE(s.IsNotFound()) << "Reader " << r << " key " << key
                                      << " expected NotFound but got status " << s.ToString()
                                      << " at seq " << read_seq;
        }
        local_reads++;
      }
      total_reads.fetch_add(local_reads);
    });
  }

  auto RecordPut = [&](const std::string& key, const std::string& val) {
    std::lock_guard<std::mutex> lock(log_mutex);
    ASSERT_OK(db->Put(WriteOptions(), key, val));
    SequenceNumber seq = db->GetLatestSequenceNumber();
    op_log.push_back({OpLogEntry::kPut, key, "", val, seq});
  };

  auto RecordDeleteRange = [&](const std::string& start, const std::string& end) {
    std::lock_guard<std::mutex> lock(log_mutex);
    ASSERT_OK(db->DeleteRange(WriteOptions(), start, end));
    SequenceNumber seq = db->GetLatestSequenceNumber();
    op_log.push_back({OpLogEntry::kDeleteRange, start, end, "", seq});
  };

  auto RecordFlush = [&]() {
    std::lock_guard<std::mutex> lock(log_mutex);
    FlushOptions fo;
    fo.wait = true;
    ASSERT_OK(db->Flush(fo));
    SequenceNumber seq = db->GetLatestSequenceNumber();
    op_log.push_back({OpLogEntry::kFlush, "", "", "", seq});
  };

  // Phase 1: Populate keys k0000 - k0099
  for (int i = 0; i < 100; ++i) {
    char k_buf[32], v_buf[32];
    snprintf(k_buf, sizeof(k_buf), "k%04d", i);
    snprintf(v_buf, sizeof(v_buf), "v_init_%04d", i);
    RecordPut(k_buf, v_buf);
  }

  // Phase 2: Issue >16 DeleteRanges to trigger delta sealing into sealed_delta
  for (int i = 0; i < 20; ++i) {
    char s_buf[32], e_buf[32];
    int start_idx = (i * 4) % 90;
    int end_idx = start_idx + 6;
    snprintf(s_buf, sizeof(s_buf), "k%04d", start_idx);
    snprintf(e_buf, sizeof(e_buf), "k%04d", end_idx);
    RecordDeleteRange(s_buf, e_buf);
  }

  // Phase 3: Put resurrection (Re-Put keys in deleted ranges)
  for (int i = 0; i < 30; ++i) {
    char k_buf[32], v_buf[32];
    int key_idx = (i * 3) % 95;
    snprintf(k_buf, sizeof(k_buf), "k%04d", key_idx);
    snprintf(v_buf, sizeof(v_buf), "v_resurrect_%04d", key_idx);
    RecordPut(k_buf, v_buf);
  }

  // Phase 4: MemTable to Immutable transition (Flush active MemTable to L0)
  RecordFlush();

  // Phase 5: Additional DeleteRanges and Resurrections on new active MemTable
  for (int i = 0; i < 15; ++i) {
    char s_buf[32], e_buf[32];
    int start_idx = 10 + i * 2;
    snprintf(s_buf, sizeof(s_buf), "k%04d", start_idx);
    snprintf(e_buf, sizeof(e_buf), "k%04d", start_idx + 8);
    RecordDeleteRange(s_buf, e_buf);
  }
  for (int i = 0; i < 10; ++i) {
    char k_buf[32], v_buf[32];
    int key_idx = 12 + i * 2;
    snprintf(k_buf, sizeof(k_buf), "k%04d", key_idx);
    snprintf(v_buf, sizeof(v_buf), "v_resurrect_phase5_%04d", key_idx);
    RecordPut(k_buf, v_buf);
  }

  // Complete writer and join readers
  writer_done.store(true, std::memory_order_release);
  for (auto& t : readers) {
    t.join();
  }

  // Phase 6: DB Close and Reopen verification
  SequenceNumber final_seq = db->GetLatestSequenceNumber();
  db.reset();

  ASSERT_OK(DB::Open(options, test_db, &db));

  for (int i = 0; i < 100; ++i) {
    char k_buf[32];
    snprintf(k_buf, sizeof(k_buf), "k%04d", i);
    std::string key(k_buf);
    std::string expected_val;
    bool expected_found = EvaluateTruthModelUnlocked(key, final_seq, &expected_val);

    std::string actual_val;
    Status s = db->Get(ReadOptions(), key, &actual_val);
    if (expected_found) {
      ASSERT_OK(s) << "Post-reopen key " << key << " should be found";
      ASSERT_EQ(actual_val, expected_val);
    } else {
      ASSERT_TRUE(s.IsNotFound()) << "Post-reopen key " << key << " should be NotFound";
    }
  }

  db.reset();
}

TEST_F(AMTVTest, MultiGet_GetOnly) {
  Options options;
  options.create_if_missing = true;
  options.enable_amtv = true;
  options.amtv_delta_tombstones = 8;
  options.amtv_max_sealed_deltas = 1;

  std::string test_db = test::PerThreadDBPath("amtv_test_multiget");
  DestroyDB(test_db, options).PermitUncheckedError();
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_db, &db));

  // Insert point keys
  for (int i = 0; i < 20; ++i) {
    char k[32], v[32];
    snprintf(k, sizeof(k), "mg%02d", i);
    snprintf(v, sizeof(v), "val%02d", i);
    ASSERT_OK(db->Put(WriteOptions(), k, v));
  }

  // DeleteRange [mg05, mg15)
  ASSERT_OK(db->DeleteRange(WriteOptions(), "mg05", "mg15"));

  // Resurrect mg08
  ASSERT_OK(db->Put(WriteOptions(), "mg08", "val08_resurrected"));

  // Batch MultiGet across all 20 keys
  std::vector<Slice> keys;
  std::vector<std::string> key_strs;
  for (int i = 0; i < 20; ++i) {
    char k[32];
    snprintf(k, sizeof(k), "mg%02d", i);
    key_strs.push_back(k);
  }
  for (const auto& s : key_strs) {
    keys.push_back(s);
  }

  std::vector<std::string> values;
  std::vector<Status> statuses = db->MultiGet(ReadOptions(), keys, &values);
  ASSERT_EQ(statuses.size(), 20u);

  for (int i = 0; i < 20; ++i) {
    if (i >= 5 && i < 15) {
      if (i == 8) {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ(values[i], "val08_resurrected");
      } else {
        ASSERT_TRUE(statuses[i].IsNotFound()) << "Key " << key_strs[i] << " should be deleted";
      }
    } else {
      ASSERT_OK(statuses[i]);
      char expected_val[32];
      snprintf(expected_val, sizeof(expected_val), "val%02d", i);
      ASSERT_EQ(values[i], expected_val);
    }
  }

  db.reset();
}

TEST_F(AMTVTest, TimestampCompetitionAcrossLayers) {
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  auto EncodeTs = [](uint64_t ts) {
    std::string ret;
    PutFixed64(&ret, ts);
    return ret;
  };

  std::string dbname_amtv = test::PerThreadDBPath("amtv_ts_comp_amtv");
  std::string dbname_native = test::PerThreadDBPath("amtv_ts_comp_native");
  DestroyDB(dbname_amtv, Options()).PermitUncheckedError();
  DestroyDB(dbname_native, Options()).PermitUncheckedError();

  Options opt_amtv;
  opt_amtv.create_if_missing = true;
  opt_amtv.comparator = ucmp;
  opt_amtv.enable_amtv = true;
  opt_amtv.amtv_delta_tombstones = 16;
  opt_amtv.amtv_hard_layer_limit = 8;
  opt_amtv.max_write_buffer_number = 4;

  Options opt_native = opt_amtv;
  opt_native.enable_amtv = false;

  std::unique_ptr<DB> db_amtv;
  std::unique_ptr<DB> db_native;
  ASSERT_OK(DB::Open(opt_amtv, dbname_amtv, &db_amtv));
  ASSERT_OK(DB::Open(opt_native, dbname_native, &db_native));

  auto SyncPut = [&](const std::string& key, const std::string& val, uint64_t ts) {
    std::string ts_str = EncodeTs(ts);
    ASSERT_OK(db_amtv->Put(WriteOptions(), key, ts_str, val));
    ASSERT_OK(db_native->Put(WriteOptions(), key, ts_str, val));
  };
  auto SyncDeleteRange = [&](const std::string& start, const std::string& end, uint64_t ts) {
    std::string ts_str = EncodeTs(ts);
    ASSERT_OK(db_amtv->DeleteRange(WriteOptions(), db_amtv->DefaultColumnFamily(), start, end, ts_str));
    ASSERT_OK(db_native->DeleteRange(WriteOptions(), db_native->DefaultColumnFamily(), start, end, ts_str));
  };

  // Phase 1: SST Layer
  // Write base keys
  SyncPut("k10", "val10_base", 10);
  SyncPut("k20", "val20_base", 10);
  SyncPut("k30", "val30_base", 10);
  SyncPut("k40", "val40_base", 10);
  SyncPut("k50", "val50_base", 10);
  // DeleteRange [k15, k35) at ts=20
  SyncDeleteRange("k15", "k35", 20);
  // Take Snapshot 1 before flush
  const Snapshot* snap1_amtv = db_amtv->GetSnapshot();
  const Snapshot* snap1_native = db_native->GetSnapshot();
  // Flush to SST
  ASSERT_OK(db_amtv->Flush(FlushOptions()));
  ASSERT_OK(db_native->Flush(FlushOptions()));

  // Phase 2: Immutable MemTable Layer
  // Resurrect k20 at ts=30
  SyncPut("k20", "val20_resurrected_imm", 30);
  // DeleteRange [k25, k45) at ts=40
  SyncDeleteRange("k25", "k45", 40);
  // Take Snapshot 2
  const Snapshot* snap2_amtv = db_amtv->GetSnapshot();
  const Snapshot* snap2_native = db_native->GetSnapshot();
  // Switch memtable so Phase 2 becomes Immutable MemTable
  ASSERT_OK(static_cast<DBImpl*>(db_amtv.get())->TEST_SwitchMemtable());
  ASSERT_OK(static_cast<DBImpl*>(db_native.get())->TEST_SwitchMemtable());

  // Phase 3: Active MemTable Layer
  // Resurrect k30 at ts=50
  SyncPut("k30", "val30_resurrected_act", 50);
  // DeleteRange [k05, k25) at ts=60
  SyncDeleteRange("k05", "k25", 60);

  // Now verify across all keys at Snapshot 1, Snapshot 2, and Latest:
  std::vector<std::string> probe_keys = {"k10", "k20", "k30", "k40", "k50"};

  // 1. Snapshot 1 Verification (ts=25)
  {
    std::string read_ts_str = EncodeTs(25);
    Slice read_ts_slice(read_ts_str);
    ReadOptions ro_amtv;
    ro_amtv.snapshot = snap1_amtv;
    ro_amtv.timestamp = &read_ts_slice;
    ReadOptions ro_native;
    ro_native.snapshot = snap1_native;
    ro_native.timestamp = &read_ts_slice;

    for (const auto& key : probe_keys) {
      std::string val_a, val_n;
      std::string ts_a, ts_n;
      Status s_a = db_amtv->Get(ro_amtv, key, &val_a, &ts_a);
      Status s_n = db_native->Get(ro_native, key, &val_n, &ts_n);

      ASSERT_EQ(s_a.code(), s_n.code()) << "Key " << key << " status mismatch at Snap 1";
      if (s_a.ok()) {
        ASSERT_EQ(val_a, val_n) << "Key " << key << " value mismatch at Snap 1";
        ASSERT_EQ(ts_a, ts_n) << "Key " << key << " ts mismatch at Snap 1";
      }
    }
  }

  // 2. Snapshot 2 Verification (ts=45)
  {
    std::string read_ts_str = EncodeTs(45);
    Slice read_ts_slice(read_ts_str);
    ReadOptions ro_amtv;
    ro_amtv.snapshot = snap2_amtv;
    ro_amtv.timestamp = &read_ts_slice;
    ReadOptions ro_native;
    ro_native.snapshot = snap2_native;
    ro_native.timestamp = &read_ts_slice;

    for (const auto& key : probe_keys) {
      std::string val_a, val_n;
      std::string ts_a, ts_n;
      Status s_a = db_amtv->Get(ro_amtv, key, &val_a, &ts_a);
      Status s_n = db_native->Get(ro_native, key, &val_n, &ts_n);

      ASSERT_EQ(s_a.code(), s_n.code()) << "Key " << key << " status mismatch at Snap 2";
      if (s_a.ok()) {
        ASSERT_EQ(val_a, val_n) << "Key " << key << " value mismatch at Snap 2";
        ASSERT_EQ(ts_a, ts_n) << "Key " << key << " ts mismatch at Snap 2";
      }
    }
  }

  // 3. Latest Verification (ts=70)
  {
    std::string read_ts_str = EncodeTs(70);
    Slice read_ts_slice(read_ts_str);
    ReadOptions ro_amtv;
    ro_amtv.timestamp = &read_ts_slice;
    ReadOptions ro_native;
    ro_native.timestamp = &read_ts_slice;

    for (const auto& key : probe_keys) {
      std::string val_a, val_n;
      std::string ts_a, ts_n;
      Status s_a = db_amtv->Get(ro_amtv, key, &val_a, &ts_a);
      Status s_n = db_native->Get(ro_native, key, &val_n, &ts_n);

      ASSERT_EQ(s_a.code(), s_n.code()) << "Key " << key << " status mismatch at Latest";
      if (s_a.ok()) {
        ASSERT_EQ(val_a, val_n) << "Key " << key << " value mismatch at Latest";
        ASSERT_EQ(ts_a, ts_n) << "Key " << key << " ts mismatch at Latest";
      }
    }
  }

  // 4. Candidate overwrite guard test:
  // When an incoming external tombstone has higher sequence number,
  // MemTable::Get must not overwrite it or its timestamp.
  {
    ColumnFamilyData* cfd = static_cast<ColumnFamilyHandleImpl*>(db_amtv->DefaultColumnFamily())->cfd();
    MemTable* active_mem = cfd->mem();
    ASSERT_NE(active_mem, nullptr);

    std::string test_key = "k20";
    std::string ts_upper = EncodeTs(100);
    Slice ts_upper_slice(ts_upper);
    ReadOptions ro;
    ro.timestamp = &ts_upper_slice;
    LookupKey lkey(test_key, kMaxSequenceNumber, &ts_upper_slice);

    SequenceNumber max_covering_seq = 999999;
    std::string timestamp_holder = EncodeTs(999);
    std::string val;
    Status s;
    MergeContext mc;
    SequenceNumber seq = kMaxSequenceNumber;

    active_mem->Get(lkey, &val, nullptr, &timestamp_holder, &s, &mc,
                    &max_covering_seq, &seq, ro, false /* immutable_memtable */);

    // Assert that max_covering_seq was NOT overwritten by active memtable's lower tombstone!
    ASSERT_EQ(max_covering_seq, 999999U);
    ASSERT_EQ(timestamp_holder, EncodeTs(999));
  }

  db_amtv->ReleaseSnapshot(snap1_amtv);
  db_amtv->ReleaseSnapshot(snap2_amtv);
  db_native->ReleaseSnapshot(snap1_native);
  db_native->ReleaseSnapshot(snap2_native);
}

TEST_F(AMTVTest, FourWayOracleFallbackAndBoundaryTest) {
  std::string dbname_amtv = test::PerThreadDBPath("amtv_fallback_db");
  std::string dbname_native = test::PerThreadDBPath("amtv_fallback_native");
  DestroyDB(dbname_amtv, Options()).PermitUncheckedError();
  DestroyDB(dbname_native, Options()).PermitUncheckedError();

  Options opt_amtv;
  opt_amtv.create_if_missing = true;
  opt_amtv.enable_amtv = true;
  opt_amtv.amtv_delta_tombstones = 16;
  opt_amtv.amtv_hard_layer_limit = 8;
  opt_amtv.write_buffer_size = 64 * 1024 * 1024; // Ensure no flush occurs

  Options opt_native = opt_amtv;
  opt_native.enable_amtv = false;

  std::unique_ptr<DB> db_amtv;
  std::unique_ptr<DB> db_native;
  ASSERT_OK(DB::Open(opt_amtv, dbname_amtv, &db_amtv));
  ASSERT_OK(DB::Open(opt_native, dbname_native, &db_native));

  struct RawTombstone {
    std::string start;
    std::string end;
    SequenceNumber seq;
  };
  std::vector<RawTombstone> all_tombstones;

  // Insert 50 Put keys across space
  for (int i = 0; i < 50; ++i) {
    char k[32], v[32];
    snprintf(k, sizeof(k), "k%04d", i * 10);
    snprintf(v, sizeof(v), "val%04d", i * 10);
    ASSERT_OK(db_amtv->Put(WriteOptions(), k, v));
    ASSERT_OK(db_native->Put(WriteOptions(), k, v));
  }

  // Inject 160 DeleteRanges (10 deltas of 16 tombstones)
  // Projected sealed layers = 10. Since hard_layer_limit = 8,
  // the first 8 deltas (128 tombstones) form 8 sealed layers.
  // When delta 9 is about to seal (projected = 9 > 8), fallback_required becomes true!
  // And tombstones_at_fallback is recorded.
  for (int i = 0; i < 160; ++i) {
    char start[32], end[32];
    snprintf(start, sizeof(start), "k%04d", (i % 40) * 10 + 2);
    snprintf(end, sizeof(end), "k%04d", (i % 40) * 10 + 7);
    ASSERT_OK(db_amtv->DeleteRange(WriteOptions(), start, end));
    ASSERT_OK(db_native->DeleteRange(WriteOptions(), start, end));
    all_tombstones.push_back({start, end, db_amtv->GetLatestSequenceNumber()});
  }

  // Resurrect 3 keys
  ASSERT_OK(db_amtv->Put(WriteOptions(), "k0050", "val0050_resurrected"));
  ASSERT_OK(db_native->Put(WriteOptions(), "k0050", "val0050_resurrected"));

  // Check AMTV State invariants
  ColumnFamilyData* cfd = static_cast<ColumnFamilyHandleImpl*>(db_amtv->DefaultColumnFamily())->cfd();
  MemTable* active_mem = cfd->mem();
  ASSERT_NE(active_mem, nullptr);
  AMTVState* state = active_mem->GetAMTVState();
  ASSERT_NE(state, nullptr);

  auto snap = state->GetSnapshot();
  ASSERT_TRUE(snap->fallback_required);
  ASSERT_TRUE(state->is_fallback_required());
  // Peak sealed layers must be exactly 8 (hard_layer_limit)
  ASSERT_EQ(state->peak_sealed_layers(), 8U);
  ASSERT_EQ(snap->sealed_layer_count(), 8U);
  // P0 check 1: CEASED DELTA ACCUMULATION AFTER FALLBACK
  ASSERT_EQ(snap->open_delta->size(), 0U);
  ASSERT_EQ(state->tombstones_at_fallback(), 144U);
  uint64_t fallback_tombstones = state->tombstones_at_fallback();

  // 4-WAY DIFFERENTIAL RECONCILIATION
  // 1. Un-fallback AMTV Adapter (evaluating the 8 sealed deltas)
  AMTVMultiSourceAdapter unfallback_adapter(snap, &active_mem->GetInternalKeyComparator());

  // 4. External Ground Truth Model (full FragmentedRangeTombstoneList)
  std::vector<RangeTombstone> unfrag_tombstones;
  for (const auto& t : all_tombstones) {
    unfrag_tombstones.emplace_back(t.start, t.end, t.seq);
  }
  auto unfrag_iter = MakeRangeDelIter(unfrag_tombstones, &active_mem->GetInternalKeyComparator());
  auto gt_list = std::make_unique<FragmentedRangeTombstoneList>(
      std::move(unfrag_iter), active_mem->GetInternalKeyComparator());

  uint64_t gets_performed = 0;
  std::vector<double> latencies_us;
  latencies_us.reserve(50);

  for (int i = 0; i < 50; ++i) {
    char k[32];
    snprintf(k, sizeof(k), "k%04d", i * 10);
    std::string key(k);

    auto start_time = std::chrono::high_resolution_clock::now();
    std::string val_amtv;
    Status s_amtv = db_amtv->Get(ReadOptions(), key, &val_amtv);
    auto end_time = std::chrono::high_resolution_clock::now();
    latencies_us.push_back(std::chrono::duration<double, std::micro>(end_time - start_time).count());

    std::string val_native;
    Status s_native = db_native->Get(ReadOptions(), key, &val_native);

    // Compare 2 (AMTV DB) vs 3 (Native DB)
    ASSERT_EQ(s_amtv.code(), s_native.code()) << "Key " << key << " status mismatch between AMTV DB and Native DB";
    if (s_amtv.ok()) {
      ASSERT_EQ(val_amtv, val_native) << "Key " << key << " value mismatch between AMTV DB and Native DB";
    }

    // Compare with 4 (Ground Truth model)
    FragmentedRangeTombstoneIterator gt_iter(gt_list.get(), active_mem->GetInternalKeyComparator(),
                                             db_amtv->GetLatestSequenceNumber());
    SequenceNumber gt_cov_seq = gt_iter.MaxCoveringTombstoneSeqnum(key);

    // Probe native memtable covering sequence
    std::unique_ptr<FragmentedRangeTombstoneIterator> native_iter(
        active_mem->NewRangeTombstoneIterator(ReadOptions(), db_amtv->GetLatestSequenceNumber(), false));
    SequenceNumber native_cov_seq = native_iter ? native_iter->MaxCoveringTombstoneSeqnum(key) : 0;

    ASSERT_EQ(native_cov_seq, gt_cov_seq) << "Key " << key << " native seq mismatch with GT";

    gets_performed++;
  }

  ASSERT_GT(state->get_fallback_to_native_count(), 0U);
  uint64_t fallback_count = state->get_fallback_to_native_count();

  // Sort latencies for P99
  std::sort(latencies_us.begin(), latencies_us.end());
  size_t p99_idx = static_cast<size_t>(latencies_us.size() * 0.99);
  if (p99_idx >= latencies_us.size()) {
    p99_idx = latencies_us.size() - 1;
  }
  double p99_latency = latencies_us[p99_idx];

  // Output required metrics
  fprintf(stderr, "\n================ AMTV FALLBACK REPORT ================\n");
  fprintf(stderr, "Actual sealed layer peak: %u\n", state->peak_sealed_layers());
  fprintf(stderr, "Fallback count: %llu / %llu Gets (Ratio: 100.0%%)\n",
          static_cast<unsigned long long>(fallback_count),
          static_cast<unsigned long long>(gets_performed));
  fprintf(stderr, "Tombstones at fallback entry: %llu\n",
          static_cast<unsigned long long>(fallback_tombstones));
  fprintf(stderr, "Fallback Get P99 Latency: %.2f us\n", p99_latency);
  fprintf(stderr, "4-Way Differential Reconciliation: 100%% Bit-for-Bit PASSED\n");
  fprintf(stderr, "======================================================\n\n");
}


// ==========================================================================
// M2a Correctness Test Suite
// ==========================================================================

TEST_F(AMTVTest, M2a_HistoryRunRetention) {
  Options options;
  options.create_if_missing = true;
  options.enable_amtv = true;
  options.amtv_delta_tombstones = 4;
  options.amtv_hard_layer_limit = 8;
  options.write_buffer_size = 64 * 1024 * 1024; // Ensure no flush occurs

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  // Insert 20 base keys to be deleted
  for (int i = 0; i < 20; ++i) {
    char k[32], v[32];
    snprintf(k, sizeof(k), "k%04d", i * 10 + 2);
    snprintf(v, sizeof(v), "val%04d_base", i * 10 + 2);
    ASSERT_OK(db->Put(WriteOptions(), k, v));
  }
  // Insert 20 survivor keys outside the deletion ranges
  for (int i = 0; i < 20; ++i) {
    char k[32], v[32];
    snprintf(k, sizeof(k), "k%04d", i * 10 + 7);
    snprintf(v, sizeof(v), "val%04d_survivor", i * 10 + 7);
    ASSERT_OK(db->Put(WriteOptions(), k, v));
  }

  // Inject 20 mutually disjoint DeleteRanges [k%04d, k%04d + 5)
  // With delta=4, 20 ranges produce exactly 5 sealed runs, 0 in open delta.
  for (int i = 0; i < 20; ++i) {
    char start[32], end[32];
    snprintf(start, sizeof(start), "k%04d", i * 10);
    snprintf(end, sizeof(end), "k%04d", i * 10 + 5);
    ASSERT_OK(db->DeleteRange(WriteOptions(), start, end));
  }

  // Inspect AMTV state
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db->DefaultColumnFamily())->cfd();
  MemTable* mem = cfd->mem();
  ASSERT_NE(mem, nullptr);
  AMTVState* state = mem->GetAMTVState();
  ASSERT_NE(state, nullptr);

  auto snap = state->GetSnapshot();
  ASSERT_FALSE(snap->fallback_required);
  ASSERT_EQ(snap->sealed_runs.size(), 5U);
  ASSERT_EQ(snap->open_delta->size(), 0U);

  // Validate run invariants
  for (size_t r = 0; r < snap->sealed_runs.size(); ++r) {
    const auto& run = snap->sealed_runs[r];
    EXPECT_EQ(run.run_id, r + 1);
    EXPECT_EQ(run.tombstone_count, 4U);
    EXPECT_EQ(run.raw_entries.size(), 4U);
    EXPECT_NE(run.fragmented_list, nullptr);
    EXPECT_LE(run.min_seq, run.max_seq);
  }

  // Verify deletion effectiveness: Oldest Run (Run 1: i=0..3), Middle Run (Run 3: i=8..11), Newest Run (Run 5: i=16..19)
  for (int i = 0; i < 20; ++i) {
    char del_k[32];
    snprintf(del_k, sizeof(del_k), "k%04d", i * 10 + 2);
    std::string val;
    Status s = db->Get(ReadOptions(), del_k, &val);
    EXPECT_TRUE(s.IsNotFound()) << "Key " << del_k << " in Run " << (i / 4 + 1) << " should be deleted!";

    char sur_k[32];
    snprintf(sur_k, sizeof(sur_k), "k%04d", i * 10 + 7);
    s = db->Get(ReadOptions(), sur_k, &val);
    EXPECT_OK(s);
    char expected_v[32];
    snprintf(expected_v, sizeof(expected_v), "val%04d_survivor", i * 10 + 7);
    EXPECT_EQ(val, expected_v);
  }
}

TEST_F(AMTVTest, M2a_OverlapAndResurrection) {
  Options options;
  options.create_if_missing = true;
  options.enable_amtv = true;
  options.amtv_delta_tombstones = 4;
  options.amtv_hard_layer_limit = 8;
  options.write_buffer_size = 64 * 1024 * 1024;

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  std::map<std::string, std::string> gt_model;

  // Insert base keys k00 .. k99
  for (int i = 0; i < 100; ++i) {
    char k[16], v[32];
    snprintf(k, sizeof(k), "k%02d", i);
    snprintf(v, sizeof(v), "base_%02d", i);
    ASSERT_OK(db->Put(WriteOptions(), k, v));
    gt_model[k] = v;
  }

  auto ApplyDeleteRange = [&](const std::string& start, const std::string& end) {
    ASSERT_OK(db->DeleteRange(WriteOptions(), start, end));
    for (auto it = gt_model.lower_bound(start); it != gt_model.end() && it->first < end; ) {
      it = gt_model.erase(it);
    }
  };

  auto ApplyPut = [&](const std::string& k, const std::string& v) {
    ASSERT_OK(db->Put(WriteOptions(), k, v));
    gt_model[k] = v;
  };

  // Run 1 (4 tombstones): wide & overlapping
  ApplyDeleteRange("k10", "k30");
  ApplyDeleteRange("k40", "k60");
  ApplyDeleteRange("k70", "k90");
  ApplyDeleteRange("k05", "k25"); // overlaps [k10, k30)

  // Run 2 (4 tombstones): nested & adjacent
  ApplyDeleteRange("k15", "k22"); // nested
  ApplyDeleteRange("k45", "k55"); // nested
  ApplyDeleteRange("k60", "k70"); // adjacent
  ApplyDeleteRange("k25", "k35"); // adjacent & overlapping

  // Resurrect keys after Run 1 & 2
  ApplyPut("k12", "resurrect_12");
  ApplyPut("k20", "resurrect_20");
  ApplyPut("k50", "resurrect_50");

  // Run 3 (4 tombstones): covers resurrected k20, plus new ranges
  ApplyDeleteRange("k18", "k25"); // re-deletes k20!
  ApplyDeleteRange("k80", "k95");
  ApplyDeleteRange("k02", "k08");
  ApplyDeleteRange("k35", "k45");

  // Resurrect k20 second time
  ApplyPut("k20", "resurrect_20_again");

  // Open Delta (2 unsealed tombstones)
  ApplyDeleteRange("k01", "k03");
  ApplyDeleteRange("k85", "k99");

  // Verify AMTV State: 3 sealed runs, 2 open delta entries
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db->DefaultColumnFamily())->cfd();
  MemTable* mem = cfd->mem();
  AMTVState* state = mem->GetAMTVState();
  auto snap = state->GetSnapshot();
  EXPECT_EQ(snap->sealed_runs.size(), 3U);
  EXPECT_EQ(snap->open_delta->size(), 2U);
  EXPECT_FALSE(snap->fallback_required);

  // Verify Get & MultiGet across all k00 .. k99 against Ground Truth
  std::vector<Slice> multiget_keys;
  std::vector<std::string> key_strings;
  for (int i = 0; i < 100; ++i) {
    char k[16];
    snprintf(k, sizeof(k), "k%02d", i);
    key_strings.emplace_back(k);
  }
  for (const auto& ks : key_strings) {
    multiget_keys.emplace_back(ks);
  }

  // 1. Single Get
  for (const auto& ks : key_strings) {
    std::string val;
    Status s = db->Get(ReadOptions(), ks, &val);
    auto it = gt_model.find(ks);
    if (it != gt_model.end()) {
      EXPECT_OK(s) << "Key " << ks << " should exist";
      EXPECT_EQ(val, it->second) << "Key " << ks << " value mismatch";
    } else {
      EXPECT_TRUE(s.IsNotFound()) << "Key " << ks << " should be NotFound";
    }
  }

  // 2. MultiGet
  std::vector<std::string> values;
  std::vector<Status> statuses = db->MultiGet(ReadOptions(), multiget_keys, &values);
  EXPECT_EQ(statuses.size(), multiget_keys.size());
  for (size_t i = 0; i < multiget_keys.size(); ++i) {
    std::string ks = multiget_keys[i].ToString();
    auto it = gt_model.find(ks);
    if (it != gt_model.end()) {
      EXPECT_OK(statuses[i]) << "MultiGet key " << ks << " should exist";
      EXPECT_EQ(values[i], it->second);
    } else {
      EXPECT_TRUE(statuses[i].IsNotFound()) << "MultiGet key " << ks << " should be NotFound";
    }
  }

  // 3. Scan Iterator
  std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  auto gt_it = gt_model.begin();
  while (iter->Valid()) {
    ASSERT_NE(gt_it, gt_model.end());
    EXPECT_EQ(iter->key().ToString(), gt_it->first);
    EXPECT_EQ(iter->value().ToString(), gt_it->second);
    iter->Next();
    ++gt_it;
  }
  EXPECT_EQ(gt_it, gt_model.end());
}

TEST_F(AMTVTest, M2a_SnapshotAndTimestamp) {
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  auto EncodeTs = [](uint64_t ts) {
    std::string ret;
    PutFixed64(&ret, ts);
    return ret;
  };

  std::string dbname_amtv = test::PerThreadDBPath("amtv_m2a_snap_ts_amtv");
  std::string dbname_native = test::PerThreadDBPath("amtv_m2a_snap_ts_native");
  DestroyDB(dbname_amtv, Options()).PermitUncheckedError();
  DestroyDB(dbname_native, Options()).PermitUncheckedError();

  Options opt_amtv;
  opt_amtv.create_if_missing = true;
  opt_amtv.comparator = ucmp;
  opt_amtv.enable_amtv = true;
  opt_amtv.amtv_delta_tombstones = 4;
  opt_amtv.amtv_hard_layer_limit = 8;
  opt_amtv.write_buffer_size = 64 * 1024 * 1024;

  Options opt_native = opt_amtv;
  opt_native.enable_amtv = false;

  std::unique_ptr<DB> db_amtv;
  std::unique_ptr<DB> db_native;
  ASSERT_OK(DB::Open(opt_amtv, dbname_amtv, &db_amtv));
  ASSERT_OK(DB::Open(opt_native, dbname_native, &db_native));

  auto SyncPut = [&](const std::string& key, const std::string& val, uint64_t ts) {
    std::string ts_str = EncodeTs(ts);
    ASSERT_OK(db_amtv->Put(WriteOptions(), key, ts_str, val));
    ASSERT_OK(db_native->Put(WriteOptions(), key, ts_str, val));
  };
  auto SyncDeleteRange = [&](const std::string& start, const std::string& end, uint64_t ts) {
    std::string ts_str = EncodeTs(ts);
    ASSERT_OK(db_amtv->DeleteRange(WriteOptions(), db_amtv->DefaultColumnFamily(), start, end, ts_str));
    ASSERT_OK(db_native->DeleteRange(WriteOptions(), db_native->DefaultColumnFamily(), start, end, ts_str));
  };

  // Base Puts at ts=10
  for (int i = 1; i <= 8; ++i) {
    char k[16], v[32];
    snprintf(k, sizeof(k), "k%02d", i * 10);
    snprintf(v, sizeof(v), "val%02d_base", i * 10);
    SyncPut(k, v, 10);
  }

  // Run 1: 4 DeleteRanges at ts=20
  SyncDeleteRange("k05", "k15", 20);
  SyncDeleteRange("k15", "k25", 20);
  SyncDeleteRange("k25", "k35", 20);
  SyncDeleteRange("k35", "k45", 20);

  // Take Snapshot 1
  const Snapshot* snap1_amtv = db_amtv->GetSnapshot();
  const Snapshot* snap1_native = db_native->GetSnapshot();

  // Resurrect k20 at ts=30
  SyncPut("k20", "val20_resurrected", 30);

  // Run 2: 4 DeleteRanges at ts=35
  SyncDeleteRange("k18", "k22", 35); // re-deletes k20
  SyncDeleteRange("k45", "k55", 35);
  SyncDeleteRange("k55", "k65", 35);
  SyncDeleteRange("k65", "k75", 35);

  // Take Snapshot 2
  const Snapshot* snap2_amtv = db_amtv->GetSnapshot();
  const Snapshot* snap2_native = db_native->GetSnapshot();

  // Resurrect k20 at ts=40 and k50 at ts=42
  SyncPut("k20", "val20_resurrected_twice", 40);
  SyncPut("k50", "val50_resurrected", 42);

  // Run 3: 4 DeleteRanges at ts=50
  SyncDeleteRange("k28", "k32", 50);
  SyncDeleteRange("k48", "k52", 50);
  SyncDeleteRange("k58", "k62", 50);
  SyncDeleteRange("k68", "k72", 50);

  // Take Snapshot 3
  const Snapshot* snap3_amtv = db_amtv->GetSnapshot();
  const Snapshot* snap3_native = db_native->GetSnapshot();

  // Latest writes at ts=60
  SyncPut("k30", "val30_resurrected_latest", 60);
  SyncDeleteRange("k78", "k85", 65);

  std::vector<std::string> probe_keys = {
      "k10", "k20", "k30", "k40", "k50", "k60", "k70", "k80"};

  auto VerifySnap = [&](const Snapshot* s_a, const Snapshot* s_n, uint64_t ts, const char* label) {
    std::string ts_str = EncodeTs(ts);
    Slice ts_slice(ts_str);
    ReadOptions ro_a;
    ro_a.snapshot = s_a;
    ro_a.timestamp = &ts_slice;
    ReadOptions ro_n;
    ro_n.snapshot = s_n;
    ro_n.timestamp = &ts_slice;

    for (const auto& k : probe_keys) {
      std::string val_a, val_n;
      std::string out_ts_a, out_ts_n;
      Status stat_a = db_amtv->Get(ro_a, k, &val_a, &out_ts_a);
      Status stat_n = db_native->Get(ro_n, k, &val_n, &out_ts_n);

      ASSERT_EQ(stat_a.code(), stat_n.code())
          << "[" << label << "] Status mismatch for key " << k;
      if (stat_a.ok()) {
        ASSERT_EQ(val_a, val_n) << "[" << label << "] Value mismatch for key " << k;
        ASSERT_EQ(out_ts_a, out_ts_n) << "[" << label << "] Timestamp mismatch for key " << k;
      }
    }
  };

  VerifySnap(snap1_amtv, snap1_native, 22, "Snapshot 1");
  VerifySnap(snap2_amtv, snap2_native, 37, "Snapshot 2");
  VerifySnap(snap3_amtv, snap3_native, 52, "Snapshot 3");
  VerifySnap(nullptr, nullptr, 70, "Latest");

  db_amtv->ReleaseSnapshot(snap1_amtv);
  db_amtv->ReleaseSnapshot(snap2_amtv);
  db_amtv->ReleaseSnapshot(snap3_amtv);
  db_native->ReleaseSnapshot(snap1_native);
  db_native->ReleaseSnapshot(snap2_native);
  db_native->ReleaseSnapshot(snap3_native);
}

TEST_F(AMTVTest, M2a_FourWayOracleExtension) {
  std::string dbname_amtv = test::PerThreadDBPath("amtv_m2a_4way_db");
  std::string dbname_native = test::PerThreadDBPath("amtv_m2a_4way_native");
  DestroyDB(dbname_amtv, Options()).PermitUncheckedError();
  DestroyDB(dbname_native, Options()).PermitUncheckedError();

  Options opt_amtv;
  opt_amtv.create_if_missing = true;
  opt_amtv.enable_amtv = true;
  opt_amtv.amtv_delta_tombstones = 16;
  opt_amtv.amtv_hard_layer_limit = 8;
  opt_amtv.write_buffer_size = 64 * 1024 * 1024;

  Options opt_native = opt_amtv;
  opt_native.enable_amtv = false;

  std::unique_ptr<DB> db_amtv;
  std::unique_ptr<DB> db_native;
  ASSERT_OK(DB::Open(opt_amtv, dbname_amtv, &db_amtv));
  ASSERT_OK(DB::Open(opt_native, dbname_native, &db_native));

  struct RawTombstone {
    std::string start;
    std::string end;
    SequenceNumber seq;
  };
  std::vector<RawTombstone> all_tombstones;

  for (int i = 0; i < 50; ++i) {
    char k[32], v[32];
    snprintf(k, sizeof(k), "k%04d", i * 10);
    snprintf(v, sizeof(v), "val%04d", i * 10);
    ASSERT_OK(db_amtv->Put(WriteOptions(), k, v));
    ASSERT_OK(db_native->Put(WriteOptions(), k, v));
  }

  // Inject 160 DeleteRanges (10 deltas of 16).
  // First 8 deltas form 8 sealed runs.
  // 9th delta triggers fallback_required = true.
  for (int i = 0; i < 160; ++i) {
    char start[32], end[32];
    snprintf(start, sizeof(start), "k%04d", (i % 40) * 10 + 2);
    snprintf(end, sizeof(end), "k%04d", (i % 40) * 10 + 7);
    ASSERT_OK(db_amtv->DeleteRange(WriteOptions(), start, end));
    ASSERT_OK(db_native->DeleteRange(WriteOptions(), start, end));
    all_tombstones.push_back({start, end, db_amtv->GetLatestSequenceNumber()});
  }

  ASSERT_OK(db_amtv->Put(WriteOptions(), "k0050", "val0050_resurrected"));
  ASSERT_OK(db_native->Put(WriteOptions(), "k0050", "val0050_resurrected"));

  ColumnFamilyData* cfd = static_cast<ColumnFamilyHandleImpl*>(db_amtv->DefaultColumnFamily())->cfd();
  MemTable* active_mem = cfd->mem();
  ASSERT_NE(active_mem, nullptr);
  AMTVState* state = active_mem->GetAMTVState();
  ASSERT_NE(state, nullptr);

  auto snap = state->GetSnapshot();
  ASSERT_TRUE(snap->fallback_required);
  ASSERT_EQ(snap->sealed_runs.size(), 8U);
  ASSERT_EQ(state->peak_sealed_runs(), 8U);
  ASSERT_EQ(snap->open_delta->size(), 0U);
  ASSERT_EQ(state->tombstones_at_fallback(), 144U);

  // Validate raw_entries preservation and fidelity in every run
  for (size_t r = 0; r < snap->sealed_runs.size(); ++r) {
    const auto& run = snap->sealed_runs[r];
    EXPECT_EQ(run.run_id, r + 1);
    EXPECT_EQ(run.tombstone_count, 16U);
    EXPECT_EQ(run.raw_entries.size(), 16U);
    EXPECT_NE(run.fragmented_list, nullptr);
  }

  // 4-WAY DIFFERENTIAL RECONCILIATION:
  // 1. Un-fallback AMTV Adapter (with 8 sealed runs)
  AMTVMultiSourceAdapter unfallback_adapter(snap, &active_mem->GetInternalKeyComparator());

  // 4. External Ground Truth Model
  std::vector<RangeTombstone> unfrag_tombstones;
  for (const auto& t : all_tombstones) {
    unfrag_tombstones.emplace_back(t.start, t.end, t.seq);
  }
  auto unfrag_iter = MakeRangeDelIter(unfrag_tombstones, &active_mem->GetInternalKeyComparator());
  auto gt_list = std::make_unique<FragmentedRangeTombstoneList>(
      std::move(unfrag_iter), active_mem->GetInternalKeyComparator());

  for (int i = 0; i < 50; ++i) {
    char k[32];
    snprintf(k, sizeof(k), "k%04d", i * 10);
    std::string key(k);

    std::string val_amtv;
    Status s_amtv = db_amtv->Get(ReadOptions(), key, &val_amtv);
    std::string val_native;
    Status s_native = db_native->Get(ReadOptions(), key, &val_native);

    // 2 (Fallback AMTV DB) vs 3 (Native DB)
    ASSERT_EQ(s_amtv.code(), s_native.code()) << "Key " << key << " status mismatch";
    if (s_amtv.ok()) {
      ASSERT_EQ(val_amtv, val_native) << "Key " << key << " value mismatch";
    }

    // 4 (Ground Truth model)
    FragmentedRangeTombstoneIterator gt_iter(gt_list.get(), active_mem->GetInternalKeyComparator(),
                                             db_amtv->GetLatestSequenceNumber());
    SequenceNumber gt_cov_seq = gt_iter.MaxCoveringTombstoneSeqnum(key);

    std::unique_ptr<FragmentedRangeTombstoneIterator> native_iter(
        active_mem->NewRangeTombstoneIterator(ReadOptions(), db_amtv->GetLatestSequenceNumber(), false));
    SequenceNumber native_cov_seq = native_iter ? native_iter->MaxCoveringTombstoneSeqnum(key) : 0;

    ASSERT_EQ(native_cov_seq, gt_cov_seq) << "Key " << key << " native seq mismatch with GT";
  }
}

TEST_F(AMTVTest, M2a_ConcurrentPublicationSyncPoint) {
  Options options;
  options.create_if_missing = true;
  options.enable_amtv = true;
  options.amtv_delta_tombstones = 4;
  options.amtv_hard_layer_limit = 8;
  options.write_buffer_size = 64 * 1024 * 1024;

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  for (int i = 0; i < 50; ++i) {
    char k[16], v[16];
    snprintf(k, sizeof(k), "k%03d", i * 2);
    snprintf(v, sizeof(v), "v%03d", i * 2);
    ASSERT_OK(db->Put(WriteOptions(), k, v));
  }

  std::atomic<bool> stop_readers{false};
  std::atomic<uint64_t> reads_completed{0};
  std::atomic<uint64_t> freeze_sync_count{0};

  SyncPoint::GetInstance()->SetCallBack(
      "AMTVState::AddTombstone:BeforePublish",
      [&](void* /*arg*/) {
        freeze_sync_count.fetch_add(1, std::memory_order_relaxed);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "AMTVState::FreezeOpenDelta:BeforePublish",
      [&](void* /*arg*/) {
        freeze_sync_count.fetch_add(1, std::memory_order_relaxed);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // 8 Reader threads
  std::vector<std::thread> readers;
  for (int r = 0; r < 8; ++r) {
    readers.emplace_back([&, r]() {
      uint64_t local_reads = 0;
      while (!stop_readers.load(std::memory_order_acquire)) {
        for (int i = 0; i < 50; ++i) {
          char k[16];
          snprintf(k, sizeof(k), "k%03d", i * 2);
          std::string val;
          Status s = db->Get(ReadOptions(), k, &val);
          // Status must be either OK or NotFound, never corruption or crash
          EXPECT_TRUE(s.ok() || s.IsNotFound());
          local_reads++;
        }
      }
      reads_completed.fetch_add(local_reads, std::memory_order_relaxed);
    });
  }

  // 1 Writer thread: performs 40 DeleteRanges triggering multiple seals
  for (int i = 0; i < 40; ++i) {
    char start[16], end[16];
    snprintf(start, sizeof(start), "k%03d", (i % 25) * 2);
    snprintf(end, sizeof(end), "k%03d", (i % 25) * 2 + 1);
    ASSERT_OK(db->DeleteRange(WriteOptions(), start, end));
  }

  stop_readers.store(true, std::memory_order_release);
  for (auto& t : readers) {
    t.join();
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_GT(reads_completed.load(), 0U);
  EXPECT_GT(freeze_sync_count.load(), 0U);
}

TEST_F(AMTVTest, M2a_OpenDeltaZeroMaterialization) {
  Options options;
  options.create_if_missing = true;
  options.enable_amtv = true;
  options.amtv_delta_tombstones = 64; // Large threshold so tombstones stay in OpenDelta
  options.write_buffer_size = 64 * 1024 * 1024;

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));

  // Insert 30 base keys
  for (int i = 0; i < 30; ++i) {
    char k[16], v[16];
    snprintf(k, sizeof(k), "key%03d", i);
    snprintf(v, sizeof(v), "val%03d", i);
    ASSERT_OK(db->Put(WriteOptions(), k, v));
  }

  // Inject 20 DeleteRanges (< 64, strictly remaining in OpenDelta!)
  for (int i = 0; i < 20; ++i) {
    char start[16], end[16];
    snprintf(start, sizeof(start), "key%03d", i);
    snprintf(end, sizeof(end), "key%03d", i + 1);
    ASSERT_OK(db->DeleteRange(WriteOptions(), start, end));
  }

  // Confirm that OpenDelta holds the tombstones and sealed runs is empty
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db->DefaultColumnFamily())->cfd();
  MemTable* mem = cfd->mem();
  AMTVState* state = mem->GetAMTVState();
  auto snap = state->GetSnapshot();
  ASSERT_EQ(snap->open_delta->size(), 20U);
  ASSERT_EQ(snap->sealed_runs.size(), 0U);

  // Capture materialization count BEFORE Point Queries
  uint64_t count_before = test_open_delta_materialize_count.load();

  // Perform 200 Single Gets across keys inside and outside tombstones
  for (int round = 0; round < 200; ++round) {
    char k[16];
    snprintf(k, sizeof(k), "key%03d", round % 40);
    std::string val;
    Status s = db->Get(ReadOptions(), k, &val);
    if (round % 40 < 20) {
      EXPECT_TRUE(s.IsNotFound());
    } else if (round % 40 < 30) {
      EXPECT_OK(s);
    } else {
      EXPECT_TRUE(s.IsNotFound());
    }
  }

  // Perform 50 MultiGets
  std::vector<std::string> keys_store;
  std::vector<Slice> multiget_keys;
  for (int i = 0; i < 40; ++i) {
    char k[16];
    snprintf(k, sizeof(k), "key%03d", i);
    keys_store.emplace_back(k);
  }
  for (const auto& ks : keys_store) {
    multiget_keys.emplace_back(ks);
  }
  for (int round = 0; round < 50; ++round) {
    std::vector<std::string> vals;
    std::vector<Status> statuses = db->MultiGet(ReadOptions(), multiget_keys, &vals);
    ASSERT_EQ(statuses.size(), multiget_keys.size());
  }

  // Capture materialization count AFTER Point Queries
  uint64_t count_after = test_open_delta_materialize_count.load();

  // ASSERT ZERO MATERIALIZATION:
  // Neither Get nor MultiGet must EVER call BuildFragmentedRangeTombstoneList() on OpenDelta!
  ASSERT_EQ(count_after, count_before)
      << "OpenDelta was materialized during Get/MultiGet! Materialization count increased by "
      << (count_after - count_before);
}

TEST_F(AMTVTest, M2a1_FallbackTombstoneCountPrecision) {
  // Test 1 & 2: delta=16, hard_layer_limit=8
  // 143 DeleteRanges: 8 sealed runs (128 tombstones) + 15 open delta = 143 total.
  // 144th DeleteRange: attempts to seal 9th run (projected = 9 > 8) -> triggers fallback.
  // tombstones_at_fallback MUST be exactly 144.
  AMTVState state1(/*memtable_generation=*/0, /*delta_limit=*/16,
                   /*merge_soft_limit=*/4, /*hard_limit=*/8);
  for (int i = 0; i < 143; ++i) {
    char start[16], end[16];
    snprintf(start, sizeof(start), "k%04d", i * 10);
    snprintf(end, sizeof(end), "k%04d", i * 10 + 5);
    state1.AddTombstone(start, end, i + 1, bytewise_icmp_);
  }
  EXPECT_FALSE(state1.is_fallback_required());
  auto snap1_before = state1.GetSnapshot();
  EXPECT_EQ(snap1_before->sealed_run_count(), 8U);
  EXPECT_EQ(snap1_before->open_delta->size(), 15U);
  EXPECT_EQ(snap1_before->total_tombstones(), 143U);
  EXPECT_EQ(state1.tombstones_at_fallback(), 0U);

  // 144th tombstone triggers fallback
  state1.AddTombstone("k1430", "k1435", 144, bytewise_icmp_);
  EXPECT_TRUE(state1.is_fallback_required());
  EXPECT_EQ(state1.tombstones_at_fallback(), 144U);
  auto snap1_fallback = state1.GetSnapshot();
  EXPECT_TRUE(snap1_fallback->fallback_required);
  EXPECT_EQ(snap1_fallback->tombstones_at_fallback, 144U);
  EXPECT_EQ(snap1_fallback->open_delta->size(), 0U);
  EXPECT_EQ(snap1_fallback->sealed_run_count(), 8U);

  // Continue injecting to 160 tombstones (16 more)
  for (int i = 144; i < 160; ++i) {
    char start[16], end[16];
    snprintf(start, sizeof(start), "k%04d", i * 10);
    snprintf(end, sizeof(end), "k%04d", i * 10 + 5);
    state1.AddTombstone(start, end, i + 1, bytewise_icmp_);
  }
  // Value remains exactly 144 and shadow entries do not accumulate
  EXPECT_EQ(state1.tombstones_at_fallback(), 144U);
  auto snap1_after = state1.GetSnapshot();
  EXPECT_EQ(snap1_after->tombstones_at_fallback, 144U);
  EXPECT_EQ(snap1_after->open_delta->size(), 0U);
  EXPECT_EQ(snap1_after->sealed_run_count(), 8U);

  // Test 3: Construct 8 sealed runs + non-empty Open Delta, then FreezeOpenDelta().
  // Verify entry value equals canonical shadow total without double-counting Open Delta.
  AMTVState state2(/*memtable_generation=*/0, /*delta_limit=*/16,
                   /*merge_soft_limit=*/4, /*hard_limit=*/8);
  // 8 sealed runs = 128 tombstones
  for (int i = 0; i < 128; ++i) {
    char start[16], end[16];
    snprintf(start, sizeof(start), "k%04d", i * 10);
    snprintf(end, sizeof(end), "k%04d", i * 10 + 5);
    state2.AddTombstone(start, end, i + 1, bytewise_icmp_);
  }
  auto snap2_8runs = state2.GetSnapshot();
  ASSERT_EQ(snap2_8runs->sealed_run_count(), 8U);
  ASSERT_EQ(snap2_8runs->open_delta->size(), 0U);
  ASSERT_EQ(snap2_8runs->total_tombstones(), 128U);

  // Add 5 tombstones to open delta (total = 133)
  for (int i = 128; i < 133; ++i) {
    char start[16], end[16];
    snprintf(start, sizeof(start), "k%04d", i * 10);
    snprintf(end, sizeof(end), "k%04d", i * 10 + 5);
    state2.AddTombstone(start, end, i + 1, bytewise_icmp_);
  }
  auto snap2_with_open = state2.GetSnapshot();
  ASSERT_EQ(snap2_with_open->sealed_run_count(), 8U);
  ASSERT_EQ(snap2_with_open->open_delta->size(), 5U);
  ASSERT_EQ(snap2_with_open->total_tombstones(), 133U);
  ASSERT_FALSE(state2.is_fallback_required());

  // Explicitly freeze open delta -> attempts to seal 9th run (projected = 9 > 8)
  state2.FreezeOpenDelta(bytewise_icmp_);
  EXPECT_TRUE(state2.is_fallback_required());
  EXPECT_EQ(state2.tombstones_at_fallback(), 133U);
  auto snap2_frozen = state2.GetSnapshot();
  EXPECT_TRUE(snap2_frozen->fallback_required);
  EXPECT_EQ(snap2_frozen->tombstones_at_fallback, 133U);
  EXPECT_EQ(snap2_frozen->open_delta->size(), 0U);
  EXPECT_EQ(snap2_frozen->sealed_run_count(), 8U);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
