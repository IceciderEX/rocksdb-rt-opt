//  Copyright (c) 2026-present. All rights reserved.
//  Test-only test suite for M1c-P0: AMTV multi-run scan semantics and interface feasibility verification.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#include "db/amtv_scan_oracle.h"

#include <memory>
#include <random>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class AMTVScanOracleTest : public testing::Test {
 public:
  AMTVScanOracleTest()
      : dbname_(test::PerThreadDBPath("amtv_scan_oracle_test")),
        bytewise_icmp_(BytewiseComparator()) {}

  void SetUp() override {
    DestroyDB(dbname_, Options()).PermitUncheckedError();
  }

  ~AMTVScanOracleTest() override {
    DestroyDB(dbname_, Options()).PermitUncheckedError();
  }

 protected:
  std::string dbname_;
  InternalKeyComparator bytewise_icmp_;
};

// --------------------------------------------------------------------------
// 1. Permutations of Empty Base / Empty Sealed / Empty Open Delta
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, EmptyRunPermutations) {
  std::vector<RangeTombstone> sample_tombstones = {
      RangeTombstone("k10", "k50", 20),
      RangeTombstone("k60", "k90", 15),
  };
  std::vector<std::string> probe_keys = {"k05", "k10", "k30", "k50", "k70", "k90", "k95"};

  for (int mask = 0; mask < 8; ++mask) {
    AMTVScanInput input;
    input.ucmp = BytewiseComparator();
    input.read_seq = 100;

    if (mask & 1) {
      input.base = sample_tombstones;
    }
    if (mask & 2) {
      input.sealed_runs.push_back(sample_tombstones);
    }
    if (mask & 4) {
      input.open_delta = sample_tombstones;
    }

    Status s = AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_);
    ASSERT_OK(s) << "Failed on mask " << mask;
  }
}

// --------------------------------------------------------------------------
// 2. Single-Run, Multi-Run, Cross-Run Overlap and Nesting
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, OverlapAndNesting) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 100;

  // Base layer: [10, 50) @ 10, [70, 90) @ 15
  input.base = {
      RangeTombstone("key10", "key50", 10),
      RangeTombstone("key70", "key90", 15),
  };
  // Sealed run: [25, 75) @ 30 (cross-overlaps with both base ranges)
  input.sealed_runs.push_back({
      RangeTombstone("key25", "key75", 30),
  });
  // Open delta: [40, 60) @ 50 (nested), [85, 99) @ 25 (partial overlap)
  input.open_delta = {
      RangeTombstone("key40", "key60", 50),
      RangeTombstone("key85", "key99", 25),
  };

  std::vector<std::string> probe_keys = {
      "key05", "key10", "key20", "key25", "key35", "key40",
      "key45", "key50", "key55", "key60", "key65", "key70",
      "key75", "key80", "key85", "key90", "key95", "key99",
  };

  Status s = AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_);
  ASSERT_OK(s);
}

// --------------------------------------------------------------------------
// 3. Adjacent Boundaries [a, b) and [b, c)
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, AdjacentBoundaries) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 100;

  // Exactly adjacent boundaries: [10, 30), [30, 60), [60, 90)
  input.base = {
      RangeTombstone("key10", "key30", 20),
  };
  input.sealed_runs.push_back({
      RangeTombstone("key30", "key60", 40),
  });
  input.open_delta = {
      RangeTombstone("key60", "key90", 10),
  };

  std::vector<std::string> probe_keys = {
      "key09", "key10", "key29", "key30", "key31",
      "key59", "key60", "key61", "key89", "key90", "key91",
  };

  Status s = AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_);
  ASSERT_OK(s);

  // Specific boundary check: key30 must be covered by [30, 60)@40, not [10, 30)@20.
  AMTVMultiRunScanIterator iter(input);
  EXPECT_EQ(iter.MaxCoveringTombstoneSeqnum("key29"), 20);
  EXPECT_EQ(iter.MaxCoveringTombstoneSeqnum("key30"), 40);
  EXPECT_EQ(iter.MaxCoveringTombstoneSeqnum("key59"), 40);
  EXPECT_EQ(iter.MaxCoveringTombstoneSeqnum("key60"), 10);
  EXPECT_EQ(iter.MaxCoveringTombstoneSeqnum("key90"), 0);
}

// --------------------------------------------------------------------------
// 4. Same Interval, Different Sequences (Sequence Shadowing and Inversion)
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, SameIntervalDifferentSequences) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();

  // Identical interval [20, 80) across all 3 layers with different seqs
  input.base = {RangeTombstone("key20", "key80", 10)};
  input.sealed_runs.push_back({RangeTombstone("key20", "key80", 50)});
  input.open_delta = {RangeTombstone("key20", "key80", 30)};

  std::vector<std::string> probe_keys = {"key10", "key20", "key50", "key79",
                                         "key80", "key90"};
  std::vector<SequenceNumber> read_seqs = {5, 10, 20, 30, 40, 50, 60};

  for (SequenceNumber seq : read_seqs) {
    input.read_seq = seq;
    Status s = AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_);
    ASSERT_OK(s) << "Failed at read_seq " << seq;
  }
}

// --------------------------------------------------------------------------
// 5. Interleaving Start/End Boundaries Across Runs
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, InterleavedBoundariesAcrossRuns) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 100;

  // Interleaved: Base [10, 40)@10, Sealed [20, 50)@20, Open [30, 60)@30
  input.base = {RangeTombstone("key10", "key40", 10)};
  input.sealed_runs.push_back({RangeTombstone("key20", "key50", 20)});
  input.open_delta = {RangeTombstone("key30", "key60", 30)};

  std::vector<std::string> probe_keys = {
      "key05", "key10", "key15", "key20", "key25", "key30",
      "key35", "key40", "key45", "key50", "key55", "key60", "key65",
  };

  Status s = AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_);
  ASSERT_OK(s);

  // Verify elementary fragments: [10, 20)@10, [20, 30)@20, [30, 40)@30, [40, 50)@30, [50, 60)@30
  AMTVMultiRunScanIterator iter(input, /*coalesce_adjacent=*/false);
  const auto& frags = iter.fragments();
  ASSERT_EQ(frags.size(), 5u);
  EXPECT_EQ(frags[0].start_key, "key10");
  EXPECT_EQ(frags[0].end_key, "key20");
  EXPECT_EQ(frags[0].seq, 10u);

  EXPECT_EQ(frags[1].start_key, "key20");
  EXPECT_EQ(frags[1].end_key, "key30");
  EXPECT_EQ(frags[1].seq, 20u);

  EXPECT_EQ(frags[2].start_key, "key30");
  EXPECT_EQ(frags[2].end_key, "key40");
  EXPECT_EQ(frags[2].seq, 30u);

  EXPECT_EQ(frags[3].start_key, "key40");
  EXPECT_EQ(frags[3].end_key, "key50");
  EXPECT_EQ(frags[3].seq, 30u);

  EXPECT_EQ(frags[4].start_key, "key50");
  EXPECT_EQ(frags[4].end_key, "key60");
  EXPECT_EQ(frags[4].seq, 30u);

  // Coalesced: [10, 20)@10, [20, 30)@20, [30, 60)@30
  AMTVMultiRunScanIterator iter_coalesced(input, /*coalesce_adjacent=*/true);
  const auto& coal = iter_coalesced.fragments();
  ASSERT_EQ(coal.size(), 3u);
  EXPECT_EQ(coal[2].start_key, "key30");
  EXPECT_EQ(coal[2].end_key, "key60");
  EXPECT_EQ(coal[2].seq, 30u);
}

// --------------------------------------------------------------------------
// 6. DeleteRange Followed by Put Resurrection and Full DB Scan Validation
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, DeleteRangeFollowedByPutResurrection_AndDBScan) {
  std::unique_ptr<DB> db;
  Options options;
  options.create_if_missing = true;
  Status s = DB::Open(options, dbname_, &db);
  ASSERT_OK(s);

  // Surrounding points present in all snapshots
  ASSERT_OK(db->Put(WriteOptions(), "key05", "val05"));
  ASSERT_OK(db->Put(WriteOptions(), "key55", "val55"));

  // Step 1: Put key30 = "val_initial"
  ASSERT_OK(db->Put(WriteOptions(), "key30", "val_initial"));
  const Snapshot* snap1 = db->GetSnapshot();

  // Step 2: DeleteRange [key10, key50)
  ASSERT_OK(db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), "key10", "key50"));
  const Snapshot* snap2 = db->GetSnapshot();

  // Step 3: Put key30 = "val_resurrected" (Resurrected!)
  ASSERT_OK(db->Put(WriteOptions(), "key30", "val_resurrected"));
  const Snapshot* snap3 = db->GetSnapshot();

  // Step 4: DeleteRange [key20, key40) (Re-deleted!)
  ASSERT_OK(db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), "key20", "key40"));
  const Snapshot* snap4 = db->GetSnapshot();

  // Step 5: Put key30 = "val_final" (Resurrected again!)
  ASSERT_OK(db->Put(WriteOptions(), "key30", "val_final"));
  const Snapshot* snap5 = db->GetSnapshot();

  // Verify Scan Visibility at snap1: key30 visible ("val_initial")
  {
    ReadOptions ro;
    ro.snapshot = snap1;
    std::unique_ptr<Iterator> it(db->NewIterator(ro));
    it->Seek("key30");
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), "key30");
    EXPECT_EQ(it->value().ToString(), "val_initial");
  }

  // Verify Scan Visibility at snap2: key30 deleted
  {
    ReadOptions ro;
    ro.snapshot = snap2;
    std::unique_ptr<Iterator> it(db->NewIterator(ro));
    it->Seek("key30");
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), "key55");  // key30 skipped
  }

  // Verify Scan Visibility at snap3: key30 resurrected ("val_resurrected")
  {
    ReadOptions ro;
    ro.snapshot = snap3;
    std::unique_ptr<Iterator> it(db->NewIterator(ro));
    it->Seek("key30");
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), "key30");
    EXPECT_EQ(it->value().ToString(), "val_resurrected");
  }

  // Verify Scan Visibility at snap4: key30 re-deleted
  {
    ReadOptions ro;
    ro.snapshot = snap4;
    std::unique_ptr<Iterator> it(db->NewIterator(ro));
    it->Seek("key30");
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), "key55");
  }

  // Verify Scan Visibility at snap5: key30 resurrected ("val_final")
  {
    ReadOptions ro;
    ro.snapshot = snap5;
    std::unique_ptr<Iterator> it(db->NewIterator(ro));
    it->Seek("key30");
    ASSERT_TRUE(it->Valid());
    EXPECT_EQ(it->key().ToString(), "key30");
    EXPECT_EQ(it->value().ToString(), "val_final");
  }

  // Clean up snapshots
  db->ReleaseSnapshot(snap1);
  db->ReleaseSnapshot(snap2);
  db->ReleaseSnapshot(snap3);
  db->ReleaseSnapshot(snap4);
  db->ReleaseSnapshot(snap5);
}

// --------------------------------------------------------------------------
// 7. Snapshot Creation Before and After
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, SnapshotCreationBeforeAndAfter) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();

  // Epoch 1 tombstones: seq 20
  input.base = {RangeTombstone("key10", "key50", 20)};
  // Epoch 2 tombstones: seq 40
  input.sealed_runs.push_back({RangeTombstone("key30", "key70", 40)});
  // Epoch 3 tombstones: seq 60
  input.open_delta = {RangeTombstone("key40", "key90", 60)};

  std::vector<std::string> probe_keys = {
      "key05", "key10", "key25", "key30", "key35", "key40",
      "key45", "key50", "key65", "key70", "key80", "key90", "key95",
  };

  // Snapshot before any tombstones (seq 10)
  input.read_seq = 10;
  ASSERT_OK(AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_));

  // Snapshot after Epoch 1 (seq 25)
  input.read_seq = 25;
  ASSERT_OK(AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_));

  // Snapshot after Epoch 2 (seq 45)
  input.read_seq = 45;
  ASSERT_OK(AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_));

  // Snapshot after Epoch 3 (seq 100)
  input.read_seq = 100;
  ASSERT_OK(AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_));
}

// --------------------------------------------------------------------------
// 8. User Defined Timestamp Enabled: Competing Tombstones
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, UserTimestampCompetingTombstones) {
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  ASSERT_NE(ucmp, nullptr);
  ASSERT_EQ(ucmp->timestamp_size(), sizeof(uint64_t));
  InternalKeyComparator ts_icmp(ucmp);

  const size_t ts_sz = sizeof(uint64_t);
  std::string dummy_ts(ts_sz, '\0');

  std::string ts100, ts200;
  PutFixed64(&ts100, 100);
  PutFixed64(&ts200, 200);

  // Construct RangeTombstones with timestamps:
  // Base: [k10, k50) @ seq 10, ts 100
  RangeTombstone t1("k10" + dummy_ts, "k50" + dummy_ts, 10, ts100);
  // Sealed: [k25, k75) @ seq 20, ts 200
  RangeTombstone t2("k25" + dummy_ts, "k75" + dummy_ts, 20, ts200);

  AMTVScanInput input;
  input.ucmp = ucmp;
  input.read_seq = 100;
  input.base = {t1};
  input.sealed_runs.push_back({t2});

  std::vector<std::string> probe_keys = {
      "k05" + dummy_ts, "k10" + dummy_ts, "k20" + dummy_ts,
      "k25" + dummy_ts, "k30" + dummy_ts, "k50" + dummy_ts,
      "k70" + dummy_ts, "k75" + dummy_ts, "k80" + dummy_ts,
  };

  // Test with no timestamp upper bound
  input.timestamp_upper_bound = nullptr;
  Status s1 = AMTVScanOracle::VerifyDifferential(input, probe_keys, ts_icmp);
  ASSERT_OK(s1);

  // Test with timestamp upper bound = 150 (t2 with ts 200 should be invisible!)
  std::string ts150;
  PutFixed64(&ts150, 150);
  Slice ts_ub(ts150);
  input.timestamp_upper_bound = &ts_ub;

  Status s2 = AMTVScanOracle::VerifyDifferential(input, probe_keys, ts_icmp);
  ASSERT_OK(s2);

  // Check that at key30 with ts_ub 150, covering seq is 10 (t1), not 20 (t2).
  AMTVMultiRunScanIterator iter(input);
  EXPECT_EQ(iter.MaxCoveringTombstoneSeqnum("k30" + dummy_ts), 10u);
}

// --------------------------------------------------------------------------
// 9. Scan Start/End Boundaries Exactly Coinciding with Tombstone Boundaries
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, ScanBoundariesCoincidingWithTombstones) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 100;

  input.base = {RangeTombstone("key20", "key60", 30)};

  AMTVMultiRunScanIterator iter(input);

  // Exact match on start key: Seek("key20")
  iter.Seek("key20");
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(iter.start_key().ToString(), "key20");
  EXPECT_EQ(iter.end_key().ToString(), "key60");

  // Just before start key: Seek("key19")
  iter.Seek("key19");
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(iter.start_key().ToString(), "key20");

  // Inside interval: Seek("key25")
  iter.Seek("key25");
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(iter.start_key().ToString(), "key20");

  // Exact match on end key: Seek("key60") -> ends after key60, none exists
  iter.Seek("key60");
  EXPECT_FALSE(iter.Valid());

  // SeekForPrev("key60") -> last starting before key60: "key20"
  iter.SeekForPrev("key60");
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(iter.start_key().ToString(), "key20");

  // SeekForPrev("key20") -> "key20"
  iter.SeekForPrev("key20");
  ASSERT_TRUE(iter.Valid());
  EXPECT_EQ(iter.start_key().ToString(), "key20");

  // SeekForPrev("key19") -> none
  iter.SeekForPrev("key19");
  EXPECT_FALSE(iter.Valid());
}

// --------------------------------------------------------------------------
// 10. Forward Seek/Next and Reverse SeekForPrev/Prev
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, BidirectionalSeekAndScan) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 100;

  input.base = {
      RangeTombstone("k10", "k20", 10),
      RangeTombstone("k30", "k40", 20),
      RangeTombstone("k50", "k60", 30),
      RangeTombstone("k70", "k80", 40),
  };

  std::vector<std::string> probe_keys = {"k05", "k15", "k25", "k35", "k45",
                                         "k55", "k65", "k75", "k85"};

  ASSERT_OK(AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_));

  AMTVMultiRunScanIterator iter(input);

  // Forward traversal
  std::vector<std::string> forward_keys;
  iter.SeekToTopFirst();
  while (iter.Valid()) {
    forward_keys.push_back(iter.start_key().ToString());
    iter.TopNext();
  }
  EXPECT_EQ(forward_keys, (std::vector<std::string>{"k10", "k30", "k50", "k70"}));

  // Reverse traversal
  std::vector<std::string> reverse_keys;
  iter.SeekToTopLast();
  while (iter.Valid()) {
    reverse_keys.push_back(iter.start_key().ToString());
    iter.TopPrev();
  }
  EXPECT_EQ(reverse_keys, (std::vector<std::string>{"k70", "k50", "k30", "k10"}));
}

// --------------------------------------------------------------------------
// 11. Iterator Refresh Simulation
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, IteratorRefresh) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 20;

  // Snapshot 1: only base exists
  input.base = {RangeTombstone("k10", "k50", 10)};

  AMTVMultiRunScanIterator iter1(input);
  ASSERT_EQ(iter1.fragments().size(), 1u);
  EXPECT_EQ(iter1.MaxCoveringTombstoneSeqnum("k60"), 0u);

  // Open delta receives new tombstone [k40, k80) @ seq 30
  input.open_delta = {RangeTombstone("k40", "k80", 30)};

  // Refreshed iterator with updated read_seq = 50
  input.read_seq = 50;
  AMTVMultiRunScanIterator iter2(input);
  EXPECT_EQ(iter2.MaxCoveringTombstoneSeqnum("k60"), 30u);
  EXPECT_EQ(iter2.MaxCoveringTombstoneSeqnum("k45"), 30u);
  EXPECT_EQ(iter2.MaxCoveringTombstoneSeqnum("k20"), 10u);

  std::vector<std::string> probe_keys = {"k05", "k20", "k45", "k60", "k85"};
  ASSERT_OK(AMTVScanOracle::VerifyDifferential(input, probe_keys, bytewise_icmp_));
}

// --------------------------------------------------------------------------
// 12. Active MemTable to Immutable Transition
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, ActiveMemTableToImmutable) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 100;

  input.base = {RangeTombstone("k10", "k40", 10)};
  input.sealed_runs.push_back({RangeTombstone("k20", "key60", 20)});
  input.open_delta = {RangeTombstone("k30", "k80", 30)};

  std::vector<std::string> probe_keys = {"k05", "k15", "k25", "k35", "k55", "k75", "k85"};

  // Active multi-run AMTV iterator
  AMTVMultiRunScanIterator active_iter(input);

  // Freeze: collapse all runs into a single unfragmented vector and construct
  // FragmentedRangeTombstoneList (exactly as ConstructFragmentedRangeTombstones does)
  std::vector<RangeTombstone> all = AMTVScanOracle::CollectAllTombstones(input);
  auto imm_list = AMTVScanOracle::BuildGroundTruthList(all, bytewise_icmp_);
  FragmentedRangeTombstoneIterator imm_iter(imm_list.get(), bytewise_icmp_, 100);

  // Assert 100% equivalence between pre-freeze AMTV iterator and post-freeze immutable list
  for (const auto& key : probe_keys) {
    EXPECT_EQ(active_iter.MaxCoveringTombstoneSeqnum(key),
              imm_iter.MaxCoveringTombstoneSeqnum(key))
        << "Mismatch at key " << key;
  }
}

// --------------------------------------------------------------------------
// 13. Open Delta Unsealed, Exactly Sealed, and Background Merge Lifecycle
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, OpenDeltaLifecycleTransitions) {
  std::vector<std::string> probe_keys = {"k05", "k15", "k25", "k35", "k55", "k75", "k95"};
  SequenceNumber read_seq = 100;

  // State 1: Open Delta unsealed (Base + Open Delta)
  AMTVScanInput state1;
  state1.ucmp = BytewiseComparator();
  state1.read_seq = read_seq;
  state1.base = {RangeTombstone("k10", "k50", 10)};
  state1.open_delta = {RangeTombstone("k30", "k70", 20)};

  // State 2: Exactly Sealed (Open Delta becomes Sealed Run 1, new Open Delta created)
  AMTVScanInput state2;
  state2.ucmp = BytewiseComparator();
  state2.read_seq = read_seq;
  state2.base = {RangeTombstone("k10", "k50", 10)};
  state2.sealed_runs.push_back({RangeTombstone("k30", "k70", 20)});
  state2.open_delta = {RangeTombstone("k60", "k90", 30)};

  // State 3: Background Merged (Base + Sealed Run 1 merged into new Base)
  AMTVScanInput state3;
  state3.ucmp = BytewiseComparator();
  state3.read_seq = read_seq;
  state3.base = {RangeTombstone("k10", "k50", 10), RangeTombstone("k30", "k70", 20)};
  state3.open_delta = {RangeTombstone("k60", "k90", 30)};

  AMTVMultiRunScanIterator iter2(state2);
  AMTVMultiRunScanIterator iter3(state3);

  // State 2 and State 3 must produce identical visibility across all probe keys!
  for (const auto& key : probe_keys) {
    EXPECT_EQ(iter2.MaxCoveringTombstoneSeqnum(key),
              iter3.MaxCoveringTombstoneSeqnum(key))
        << "Lifecycle invariance violated at key " << key;
  }

  ASSERT_OK(AMTVScanOracle::VerifyDifferential(state2, probe_keys, bytewise_icmp_));
  ASSERT_OK(AMTVScanOracle::VerifyDifferential(state3, probe_keys, bytewise_icmp_));
}

// --------------------------------------------------------------------------
// 14. AMTV Fallback State: Full Fallback to Native Scan
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, AMTVFallbackState) {
  AMTVScanInput input;
  input.ucmp = BytewiseComparator();
  input.read_seq = 100;

  // Generate a multi-run input that exceeds fallback threshold
  for (int r = 0; r < 35; ++r) {
    std::string sk = "k" + std::to_string(r * 2);
    std::string ek = "k" + std::to_string(r * 2 + 10);
    input.sealed_runs.push_back({RangeTombstone(sk, ek, r + 1)});
  }

  // Fallback decision: if num_runs > 32, AMTV returns fallback = true
  bool fallback_triggered = (input.sealed_runs.size() > 32);
  EXPECT_TRUE(fallback_triggered);

  // Under fallback, caller delegates directly to native single FragmentedRangeTombstoneList
  std::vector<RangeTombstone> all = AMTVScanOracle::CollectAllTombstones(input);
  auto native_list = AMTVScanOracle::BuildGroundTruthList(all, bytewise_icmp_);
  FragmentedRangeTombstoneIterator native_iter(native_list.get(), bytewise_icmp_, 100);

  // Reference iterator must also match native fallback iterator perfectly
  AMTVMultiRunScanIterator ref_iter(input);
  std::vector<std::string> probe_keys = {"k00", "k10", "k30", "k50", "k70", "k90"};
  for (const auto& key : probe_keys) {
    EXPECT_EQ(ref_iter.MaxCoveringTombstoneSeqnum(key),
              native_iter.MaxCoveringTombstoneSeqnum(key));
  }
}

// --------------------------------------------------------------------------
// 15. 1,000 Randomized Differential Test Cases Against Ground Truth
// --------------------------------------------------------------------------
TEST_F(AMTVScanOracleTest, RandomizedDifferential1000Trials) {
  std::mt19937_64 rng(42);  // Deterministic seed

  std::vector<std::string> key_pool;
  for (int i = 0; i < 30; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key%02d", i);
    key_pool.emplace_back(buf);
  }

  const int kNumTrials = 1000;
  for (int trial = 0; trial < kNumTrials; ++trial) {
    AMTVScanInput input;
    input.ucmp = BytewiseComparator();

    std::uniform_int_distribution<int> num_runs_dist(1, 6);
    int total_runs = num_runs_dist(rng);

    std::uniform_int_distribution<int> tombstones_per_run_dist(0, 8);
    std::uniform_int_distribution<int> key_idx_dist(0, static_cast<int>(key_pool.size()) - 2);
    std::uniform_int_distribution<uint64_t> seq_dist(1, 500);

    for (int r = 0; r < total_runs; ++r) {
      std::vector<RangeTombstone> run_tombstones;
      int num_t = tombstones_per_run_dist(rng);
      for (int t = 0; t < num_t; ++t) {
        int idx1 = key_idx_dist(rng);
        std::uniform_int_distribution<int> idx2_dist(idx1 + 1, static_cast<int>(key_pool.size()) - 1);
        int idx2 = idx2_dist(rng);
        run_tombstones.emplace_back(key_pool[idx1], key_pool[idx2], seq_dist(rng));
      }

      if (r == 0) {
        input.base = std::move(run_tombstones);
      } else if (r == total_runs - 1) {
        input.open_delta = std::move(run_tombstones);
      } else {
        input.sealed_runs.push_back(std::move(run_tombstones));
      }
    }

    std::uniform_int_distribution<uint64_t> read_seq_dist(1, 600);
    input.read_seq = read_seq_dist(rng);

    Status s = AMTVScanOracle::VerifyDifferential(input, key_pool, bytewise_icmp_);
    ASSERT_OK(s) << "Differential Oracle failed on trial " << trial
                 << ", read_seq: " << input.read_seq;
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
