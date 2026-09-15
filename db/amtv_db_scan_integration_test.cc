//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>
#include <string>
#include <vector>

#include "db/amtv_local_scan_view.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class AMTVDBScanIntegrationTest : public testing::Test {
 public:
  AMTVDBScanIntegrationTest()
      : dbname_native_(test::PerThreadDBPath("amtv_db_scan_native")),
        dbname_cand_(test::PerThreadDBPath("amtv_db_scan_cand")) {}

  ~AMTVDBScanIntegrationTest() override {
    CloseDualDB();
    EXPECT_OK(DestroyDB(dbname_native_, Options()));
    EXPECT_OK(DestroyDB(dbname_cand_, Options()));
  }

  void SetUp() override {
    EXPECT_OK(DestroyDB(dbname_native_, Options()));
    EXPECT_OK(DestroyDB(dbname_cand_, Options()));
  }

  void TearDown() override {
    CloseDualDB();
    EXPECT_OK(DestroyDB(dbname_native_, Options()));
    EXPECT_OK(DestroyDB(dbname_cand_, Options()));
  }

  void OpenDualDB(bool cand_bounded_scan = true,
                  const Comparator* ucmp = BytewiseComparator(),
                  size_t write_buffer_size = 64 * 1024 * 1024) {
    CloseDualDB();
    Options opt_native;
    opt_native.create_if_missing = true;
    opt_native.enable_amtv = true;
    opt_native.amtv_enable_bounded_scan_view = false;  // Ground-truth Native
    opt_native.write_buffer_size = write_buffer_size;
    opt_native.comparator = ucmp;
    opt_native.amtv_delta_tombstones = 4;
    opt_native.amtv_merge_soft_limit = 4;
    opt_native.amtv_hard_layer_limit = 8;
    opt_native.amtv_max_sealed_deltas = 8;

    Options opt_cand = opt_native;
    opt_cand.amtv_enable_bounded_scan_view = cand_bounded_scan;  // Candidate

    ASSERT_OK(DB::Open(opt_native, dbname_native_, &db_native_));
    ASSERT_OK(DB::Open(opt_cand, dbname_cand_, &db_cand_));
  }

  void CloseDualDB() {
    db_native_.reset();
    db_cand_.reset();
  }

  void DualPut(const Slice& key, const Slice& val) {
    ASSERT_OK(db_native_->Put(WriteOptions(), key, val));
    ASSERT_OK(db_cand_->Put(WriteOptions(), key, val));
  }

  void DualDeleteRange(const Slice& start, const Slice& end) {
    ASSERT_OK(db_native_->DeleteRange(WriteOptions(), start, end));
    ASSERT_OK(db_cand_->DeleteRange(WriteOptions(), start, end));
  }

  void DualPutWithTs(const Slice& key, const Slice& ts, const Slice& val) {
    ASSERT_OK(db_native_->Put(WriteOptions(), key, ts, val));
    ASSERT_OK(db_cand_->Put(WriteOptions(), key, ts, val));
  }

  void DualDeleteRangeWithTs(const Slice& start, const Slice& end,
                             const Slice& ts) {
    ASSERT_OK(db_native_->DeleteRange(
        WriteOptions(), db_native_->DefaultColumnFamily(), start, end, ts));
    ASSERT_OK(db_cand_->DeleteRange(
        WriteOptions(), db_cand_->DefaultColumnFamily(), start, end, ts));
  }

  // Differential equivalence verification between Native DB and Candidate DB
  void AssertIteratorsEquivalent(
      Iterator* it_native, Iterator* it_cand, const Slice* lower_bound,
      const Slice* upper_bound,
      const std::vector<std::string>& seek_targets = {},
      const Comparator* ucmp = BytewiseComparator()) {
    ASSERT_NE(it_native, nullptr);
    ASSERT_NE(it_cand, nullptr);

    // Initial status equivalence
    ASSERT_EQ(it_native->status().ToString(), it_cand->status().ToString());

    auto check_key_in_bounds = [&](const Slice& key) {
      if (lower_bound != nullptr) {
        ASSERT_GE(ucmp->CompareWithoutTimestamp(key, /*a_has_ts=*/false,
                                                *lower_bound,
                                                /*b_has_ts=*/false),
                  0)
            << "Candidate key " << key.ToString(true)
            << " violates lower bound " << lower_bound->ToString(true);
      }
      if (upper_bound != nullptr) {
        ASSERT_LT(ucmp->CompareWithoutTimestamp(key, /*a_has_ts=*/false,
                                                *upper_bound,
                                                /*b_has_ts=*/false),
                  0)
            << "Candidate key " << key.ToString(true)
            << " violates upper bound " << upper_bound->ToString(true);
      }
    };

    // 1. Forward Scan Verification (SeekToFirst -> Next -> until invalid)
    it_native->SeekToFirst();
    it_cand->SeekToFirst();

    std::vector<std::pair<std::string, std::string>> fwd_native;
    std::vector<std::pair<std::string, std::string>> fwd_cand;

    while (it_native->Valid() || it_cand->Valid()) {
      ASSERT_EQ(it_native->Valid(), it_cand->Valid())
          << "Forward Valid() mismatch at native count " << fwd_native.size()
          << ", cand count " << fwd_cand.size();
      if (!it_native->Valid()) {
        break;
      }
      check_key_in_bounds(it_cand->key());
      ASSERT_EQ(it_native->key().ToString(), it_cand->key().ToString());
      ASSERT_EQ(it_native->value().ToString(), it_cand->value().ToString());

      fwd_native.emplace_back(it_native->key().ToString(),
                              it_native->value().ToString());
      fwd_cand.emplace_back(it_cand->key().ToString(),
                            it_cand->value().ToString());

      it_native->Next();
      it_cand->Next();
    }
    ASSERT_EQ(it_native->status().ToString(), it_cand->status().ToString());
    ASSERT_EQ(fwd_native, fwd_cand);

    // 2. Reverse Scan Verification (SeekToLast -> Prev -> until invalid)
    it_native->SeekToLast();
    it_cand->SeekToLast();

    std::vector<std::pair<std::string, std::string>> rev_native;
    std::vector<std::pair<std::string, std::string>> rev_cand;

    while (it_native->Valid() || it_cand->Valid()) {
      ASSERT_EQ(it_native->Valid(), it_cand->Valid())
          << "Reverse Valid() mismatch at native count " << rev_native.size()
          << ", cand count " << rev_cand.size();
      if (!it_native->Valid()) {
        break;
      }
      check_key_in_bounds(it_cand->key());
      ASSERT_EQ(it_native->key().ToString(), it_cand->key().ToString());
      ASSERT_EQ(it_native->value().ToString(), it_cand->value().ToString());

      rev_native.emplace_back(it_native->key().ToString(),
                              it_native->value().ToString());
      rev_cand.emplace_back(it_cand->key().ToString(),
                            it_cand->value().ToString());

      it_native->Prev();
      it_cand->Prev();
    }
    ASSERT_EQ(it_native->status().ToString(), it_cand->status().ToString());
    ASSERT_EQ(rev_native, rev_cand);

    // 3. Discrete Seek and SeekForPrev Verification across specified targets
    for (const auto& target : seek_targets) {
      // Seek
      it_native->Seek(target);
      it_cand->Seek(target);
      ASSERT_EQ(it_native->Valid(), it_cand->Valid())
          << "Seek Valid() mismatch on target: " << target;
      if (it_native->Valid()) {
        check_key_in_bounds(it_cand->key());
        ASSERT_EQ(it_native->key().ToString(), it_cand->key().ToString())
            << "Seek key mismatch on target: " << target;
        ASSERT_EQ(it_native->value().ToString(), it_cand->value().ToString())
            << "Seek value mismatch on target: " << target;
      }
      ASSERT_EQ(it_native->status().ToString(), it_cand->status().ToString());

      // SeekForPrev
      it_native->SeekForPrev(target);
      it_cand->SeekForPrev(target);
      ASSERT_EQ(it_native->Valid(), it_cand->Valid())
          << "SeekForPrev Valid() mismatch on target: " << target;
      if (it_native->Valid()) {
        check_key_in_bounds(it_cand->key());
        ASSERT_EQ(it_native->key().ToString(), it_cand->key().ToString())
            << "SeekForPrev key mismatch on target: " << target;
        ASSERT_EQ(it_native->value().ToString(), it_cand->value().ToString())
            << "SeekForPrev value mismatch on target: " << target;
      }
      ASSERT_EQ(it_native->status().ToString(), it_cand->status().ToString());
    }
  }

 protected:
  std::string dbname_native_;
  std::string dbname_cand_;
  std::unique_ptr<DB> db_native_;
  std::unique_ptr<DB> db_cand_;
};

// Scenario 1: No tombstones in active memtable
TEST_F(AMTVDBScanIntegrationTest, Scenario01_NoTombstones) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  DualPut("k40", "v40");
  DualPut("k50", "v50");

  Slice l("k15");
  Slice u("k45");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"k00", "k10", "k15", "k20", "k30",
                                      "k40", "k45", "k50", "k60"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 2: All tombstones disjoint from window
TEST_F(AMTVDBScanIntegrationTest, Scenario02_DisjointTombstones) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k02", "v02");
  DualPut("k03", "v03");
  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  DualPut("k40", "v40");
  DualPut("k50", "v50");
  DualPut("k85", "v85");

  // Tombstones strictly below and strictly above window
  DualDeleteRange("k01", "k05");
  DualDeleteRange("k80", "k90");

  Slice l("k20");
  Slice u("k50");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"k05", "k10", "k20", "k30", "k40",
                                      "k50", "k60", "k70", "k80"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 3: Left boundary straddles long tombstone
TEST_F(AMTVDBScanIntegrationTest, Scenario03_LeftBoundaryStraddle) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  DualPut("k40", "v40");
  DualPut("k50", "v50");

  // Tombstone [k05, k25) covers k10, k20. Left bound k15 is strictly inside [k05, k25).
  DualDeleteRange("k05", "k25");

  Slice l("k15");
  Slice u("k45");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"k10", "k15", "k20", "k25", "k30",
                                      "k40", "k45"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 4: Right boundary straddles long tombstone
TEST_F(AMTVDBScanIntegrationTest, Scenario04_RightBoundaryStraddle) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  DualPut("k40", "v40");
  DualPut("k50", "v50");

  // Tombstone [k35, k55) covers k40, k50. Right bound k45 is strictly inside [k35, k55).
  DualDeleteRange("k35", "k55");

  Slice l("k15");
  Slice u("k45");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"k15", "k20", "k30", "k35", "k40",
                                      "k45", "k50"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 5: Tombstone completely encompasses window
TEST_F(AMTVDBScanIntegrationTest, Scenario05_EncompassingTombstone) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  DualPut("k40", "v40");
  DualPut("k50", "v50");

  // Tombstone [k05, k55) completely covers window [k15, k45)
  DualDeleteRange("k05", "k55");

  Slice l("k15");
  Slice u("k45");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"k10", "k15", "k20", "k30", "k40",
                                      "k45", "k50"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
  // Both must be completely invalid inside window
  it_cand->SeekToFirst();
  EXPECT_FALSE(it_cand->Valid());
  it_cand->SeekToLast();
  EXPECT_FALSE(it_cand->Valid());
}

// Scenario 6: Overlapping, nested, adjacent tombstones
TEST_F(AMTVDBScanIntegrationTest, Scenario06_ComplexTombstoneTopology) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  for (int i = 0; i < 100; i += 5) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    DualPut(buf, std::string("v_") + buf);
  }

  // 1. Overlapping tombstones: [k10, k30) and [k20, k40)
  DualDeleteRange("k10", "k30");
  DualDeleteRange("k20", "k40");

  // 2. Adjacent tombstone: [k40, k50)
  DualDeleteRange("k40", "k50");

  // 3. Nested tombstones: [k60, k90) and [k70, k80)
  DualDeleteRange("k60", "k90");
  DualDeleteRange("k70", "k80");

  Slice l("k05");
  Slice u("k85");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets;
  for (int i = 0; i <= 95; i += 5) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    targets.emplace_back(buf);
  }
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 7: Same start key, different end keys and sequence
TEST_F(AMTVDBScanIntegrationTest, Scenario07_SameStartDiffEndAndSeq) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  for (int i = 10; i <= 70; i += 5) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    DualPut(buf, std::string("v_") + buf);
  }

  // Three tombstones with identical start key "k20" but different end keys and seq
  DualDeleteRange("k20", "k50");  // seq 1
  DualDeleteRange("k20", "k35");  // seq 2
  DualDeleteRange("k20", "k60");  // seq 3

  Slice l("k15");
  Slice u("k65");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"k15", "k20", "k30", "k35", "k45",
                                      "k50", "k55", "k60", "k65"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 8: DeleteRange followed by Put resurrection
TEST_F(AMTVDBScanIntegrationTest, Scenario08_PutResurrection) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  DualPut("k40", "v40");
  DualPut("k50", "v50");

  // Delete [k15, k45) covering k20, k30, k40
  DualDeleteRange("k15", "k45");

  // Resurrect k30 and k20 with higher sequence numbers
  DualPut("k30", "v30_resurrected");
  DualPut("k20", "v20_resurrected");

  Slice l("k15");
  Slice u("k45");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"k15", "k20", "k25", "k30", "k35",
                                      "k40", "k45"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 9: Multiple Snapshots and read sequences
TEST_F(AMTVDBScanIntegrationTest, Scenario09_MultipleSnapshots) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  // Phase 1
  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  const Snapshot* snap1_native = db_native_->GetSnapshot();
  const Snapshot* snap1_cand = db_cand_->GetSnapshot();

  // Phase 2
  DualDeleteRange("k15", "k35");
  DualPut("k25", "v25_snap2");
  const Snapshot* snap2_native = db_native_->GetSnapshot();
  const Snapshot* snap2_cand = db_cand_->GetSnapshot();

  // Phase 3
  DualDeleteRange("k22", "k28");
  DualPut("k25", "v25_snap3");
  DualPut("k40", "v40_snap3");
  const Snapshot* snap3_native = db_native_->GetSnapshot();
  const Snapshot* snap3_cand = db_cand_->GetSnapshot();

  // Phase 4 (latest writes)
  DualPut("k28", "v28_latest");

  Slice l("k05");
  Slice u("k50");
  std::vector<std::string> targets = {"k05", "k10", "k15", "k20", "k25",
                                      "k28", "k30", "k35", "k40", "k50"};

  // Verify Snapshot 1
  {
    ReadOptions ro_nat, ro_cand;
    ro_nat.snapshot = snap1_native;
    ro_nat.iterate_lower_bound = &l;
    ro_nat.iterate_upper_bound = &u;
    ro_cand.snapshot = snap1_cand;
    ro_cand.iterate_lower_bound = &l;
    ro_cand.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro_nat));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro_cand));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), &l, &u, targets);
  }

  // Verify Snapshot 2
  {
    ReadOptions ro_nat, ro_cand;
    ro_nat.snapshot = snap2_native;
    ro_nat.iterate_lower_bound = &l;
    ro_nat.iterate_upper_bound = &u;
    ro_cand.snapshot = snap2_cand;
    ro_cand.iterate_lower_bound = &l;
    ro_cand.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro_nat));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro_cand));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), &l, &u, targets);
  }

  // Verify Snapshot 3
  {
    ReadOptions ro_nat, ro_cand;
    ro_nat.snapshot = snap3_native;
    ro_nat.iterate_lower_bound = &l;
    ro_nat.iterate_upper_bound = &u;
    ro_cand.snapshot = snap3_cand;
    ro_cand.iterate_lower_bound = &l;
    ro_cand.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro_nat));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro_cand));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), &l, &u, targets);
  }

  // Verify Latest Unsnapshotted Sequence
  {
    ReadOptions ro_nat, ro_cand;
    ro_nat.iterate_lower_bound = &l;
    ro_nat.iterate_upper_bound = &u;
    ro_cand.iterate_lower_bound = &l;
    ro_cand.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro_nat));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro_cand));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), &l, &u, targets);
  }

  db_native_->ReleaseSnapshot(snap1_native);
  db_cand_->ReleaseSnapshot(snap1_cand);
  db_native_->ReleaseSnapshot(snap2_native);
  db_cand_->ReleaseSnapshot(snap2_cand);
  db_native_->ReleaseSnapshot(snap3_native);
  db_cand_->ReleaseSnapshot(snap3_cand);
}

// Scenario 10: Legal empty user key
TEST_F(AMTVDBScanIntegrationTest, Scenario10_EmptyUserKey) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("", "v_empty");
  DualPut("\x01", "v_01");
  DualPut("a", "v_a");
  DualPut("b", "v_b");
  DualPut("c", "v_c");

  // Tombstone starting from empty key: ["", "b")
  DualDeleteRange("", "b");

  Slice l("");
  Slice u("z");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_native = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  std::vector<std::string> targets = {"", "\x01", "a", "b", "c", "z"};
  AssertIteratorsEquivalent(it_native.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 11: User-defined timestamps (UDT)
TEST_F(AMTVDBScanIntegrationTest, Scenario11_UserDefinedTimestamps) {
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  OpenDualDB(/*cand_bounded_scan=*/true, ucmp);

  auto encode_ts = [](uint64_t ts) {
    std::string ret;
    PutFixed64(&ret, ts);
    return ret;
  };

  std::string ts10 = encode_ts(10);
  std::string ts20 = encode_ts(20);
  std::string ts30 = encode_ts(30);
  std::string ts_read15 = encode_ts(15);
  std::string ts_read25 = encode_ts(25);
  std::string ts_read35 = encode_ts(35);

  // Write at ts 10
  DualPutWithTs("k10", ts10, "v10");
  DualPutWithTs("k20", ts10, "v20");
  DualPutWithTs("k30", ts10, "v30");
  DualPutWithTs("k40", ts10, "v40");
  DualPutWithTs("k50", ts10, "v50");

  // DeleteRange at ts 20: [k15, k45)
  DualDeleteRangeWithTs("k15", "k45", ts20);

  // Resurrection at ts 30
  DualPutWithTs("k30", ts30, "v30_revived");

  Slice l("k15");
  Slice u("k45");
  std::vector<std::string> targets = {"k15", "k20", "k30", "k40", "k45"};

  // 1. Read at ts 15 (before range delete): k20, k30, k40 visible
  {
    Slice read_ts(ts_read15);
    ReadOptions ro;
    ro.timestamp = &read_ts;
    ro.iterate_lower_bound = &l;
    ro.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, targets,
                              ucmp);
  }

  // 2. Read at ts 25 (after range delete, before resurrection): all deleted
  {
    Slice read_ts(ts_read25);
    ReadOptions ro;
    ro.timestamp = &read_ts;
    ro.iterate_lower_bound = &l;
    ro.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, targets,
                              ucmp);
  }

  // 3. Read at ts 35 (after resurrection): k30 visible
  {
    Slice read_ts(ts_read35);
    ReadOptions ro;
    ro.timestamp = &read_ts;
    ro.iterate_lower_bound = &l;
    ro.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, targets,
                              ucmp);
  }
}

// Scenario 12: Fallback matrix (Options disabled, AMTV failure fallback, unbounded scans)
TEST_F(AMTVDBScanIntegrationTest, Scenario12_FallbackMatrix) {
  // 12a: Option disabled on candidate DB
  {
    OpenDualDB(/*cand_bounded_scan=*/false);
    DualPut("k10", "v10");
    DualPut("k20", "v20");
    DualDeleteRange("k15", "k25");
    Slice l("k05");
    Slice u("k35");
    ReadOptions ro;
    ro.iterate_lower_bound = &l;
    ro.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), &l, &u, {"k10", "k20"});
  }

  // 12b: AMTV local build failure injection via SyncPoint
  {
    OpenDualDB(/*cand_bounded_scan=*/true);
    DualPut("k10", "v10");
    DualPut("k20", "v20");
    DualDeleteRange("k15", "k25");

    SyncPoint::GetInstance()->SetCallBack(
        "BuildActiveMemTableRangeDelIteratorForScan:LocalBuildFail",
        [](void* arg) {
          bool* fail = static_cast<bool*>(arg);
          *fail = true;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    Slice l("k05");
    Slice u("k35");
    ReadOptions ro;
    ro.iterate_lower_bound = &l;
    ro.iterate_upper_bound = &u;
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), &l, &u, {"k10", "k20"});

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  // 12c: Unbounded scan (iterate_lower_bound / upper_bound is null)
  {
    OpenDualDB(/*cand_bounded_scan=*/true);
    DualPut("k10", "v10");
    DualPut("k20", "v20");
    DualPut("k30", "v30");
    DualDeleteRange("k15", "k25");

    ReadOptions ro;
    // No bounds specified
    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), nullptr, nullptr,
                              {"k10", "k20", "k30"});
  }

  // 12d: Inverted bounds (lower >= upper)
  {
    OpenDualDB(/*cand_bounded_scan=*/true);
    DualPut("k10", "v10");
    DualPut("k20", "v20");
    DualDeleteRange("k15", "k25");

    Slice l("k30");
    Slice u("k10");
    ReadOptions ro;
    ro.iterate_lower_bound = &l;
    ro.iterate_upper_bound = &u;

    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    it_nat->SeekToFirst();
    it_c->SeekToFirst();
    EXPECT_FALSE(it_nat->Valid());
    EXPECT_FALSE(it_c->Valid());
    EXPECT_EQ(it_nat->status().ToString(), it_c->status().ToString());
  }

  // 12e: ignore_range_deletions = true
  {
    OpenDualDB(/*cand_bounded_scan=*/true);
    DualPut("k10", "v10");
    DualPut("k20", "v20");
    DualPut("k30", "v30");
    DualDeleteRange("k15", "k25");

    Slice l("k05");
    Slice u("k35");
    ReadOptions ro;
    ro.iterate_lower_bound = &l;
    ro.iterate_upper_bound = &u;
    ro.ignore_range_deletions = true;

    auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
    auto it_c = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    AssertIteratorsEquivalent(it_nat.get(), it_c.get(), &l, &u,
                              {"k10", "k20", "k30"});
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
