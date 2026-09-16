//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "db/amtv.h"
#include "db/amtv_local_scan_view.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class AMTVDBRefreshTest : public testing::Test {
 public:
  AMTVDBRefreshTest()
      : dbname_native_(test::PerThreadDBPath("amtv_db_refresh_native")),
        dbname_cand_(test::PerThreadDBPath("amtv_db_refresh_cand")) {}

  ~AMTVDBRefreshTest() override {
    CloseDualDB();
    EXPECT_OK(DestroyDB(dbname_native_, Options()));
    EXPECT_OK(DestroyDB(dbname_cand_, Options()));
  }

  void SetUp() override {
#ifndef NDEBUG
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->ClearTrace();
#endif
    EXPECT_OK(DestroyDB(dbname_native_, Options()));
    EXPECT_OK(DestroyDB(dbname_cand_, Options()));
  }

  void TearDown() override {
#ifndef NDEBUG
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->ClearTrace();
#endif
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

  void DualFlush() {
    ASSERT_OK(db_native_->Flush(FlushOptions()));
    ASSERT_OK(db_cand_->Flush(FlushOptions()));
  }

  std::string cand_last_outcome_;

  std::unique_ptr<Iterator> NewCandidateIterator(const ReadOptions& ro) {
    cand_last_outcome_.clear();
#ifndef NDEBUG
    SyncPoint::GetInstance()->SetCallBack(
        "BuildActiveMemTableRangeDelIteratorForScan:Outcome",
        [this](void* arg) {
          const char** p = reinterpret_cast<const char**>(arg);
          if (p && *p) {
            this->cand_last_outcome_ = *p;
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
#endif
    auto it = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));
    if (it != nullptr) {
      it->SeekToFirst();
    }
#ifndef NDEBUG
    SyncPoint::GetInstance()->ClearCallBack(
        "BuildActiveMemTableRangeDelIteratorForScan:Outcome");
#endif
    return it;
  }

  Status CandidateRefresh(Iterator* it, const Snapshot* snapshot = nullptr) {
    cand_last_outcome_.clear();
#ifndef NDEBUG
    SyncPoint::GetInstance()->SetCallBack(
        "BuildActiveMemTableRangeDelIteratorForScan:Outcome",
        [this](void* arg) {
          const char** p = reinterpret_cast<const char**>(arg);
          if (p && *p) {
            this->cand_last_outcome_ = *p;
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
#endif
    Status s = it->Refresh(snapshot);
#ifndef NDEBUG
    SyncPoint::GetInstance()->ClearCallBack(
        "BuildActiveMemTableRangeDelIteratorForScan:Outcome");
#endif
    return s;
  }

  // Differential equivalence verification between Native DB and Candidate DB
  void AssertIteratorsEquivalent(
      Iterator* it_native, Iterator* it_cand, const Slice* lower_bound,
      const Slice* upper_bound,
      const std::vector<std::string>& seek_targets = {},
      const Comparator* ucmp = BytewiseComparator()) {
    ASSERT_NE(it_native, nullptr);
    ASSERT_NE(it_cand, nullptr);

    // Status equivalence
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

    // 3. Discrete Seek and SeekForPrev Verification
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

// ============================================================================
// Section B4: Refresh Differential Test Scenarios
// ============================================================================

// Scenario 1: Put added after Iterator creation, visible after Refresh
TEST_F(AMTVDBRefreshTest, Scenario01_PutAfterIteratorCreation) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Before refresh: k10, k20 visible
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k20"});

  // New writes occur
  DualPut("k15", "v15_new");
  DualPut("k25", "v25_new");

  // Before refresh: new keys NOT visible
  it_cand->Seek("k15");
  EXPECT_TRUE(it_cand->Valid());
  EXPECT_EQ(it_cand->key().ToString(), "k20");

  // Refresh both iterators
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_EMPTY");
#endif

  // After refresh: k15, k25 visible and equivalent
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k10", "k15", "k20", "k25"});
}

// Scenario 2: DeleteRange added after Iterator creation, masked after Refresh
TEST_F(AMTVDBRefreshTest, Scenario02_DeleteRangeAfterIteratorCreation) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");
  DualPut("k40", "v40");

  Slice l("k05");
  Slice u("k50");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Add range deletion [k15, k35) covering k20, k30
  DualDeleteRange("k15", "k35");

  // Before refresh: k20 and k30 still visible
  it_cand->Seek("k20");
  EXPECT_TRUE(it_cand->Valid());
  EXPECT_EQ(it_cand->key().ToString(), "k20");

  // Refresh both
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_NONEMPTY");
#endif

  // After refresh: k20 and k30 masked; k10 and k40 visible
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k05", "k10", "k20", "k30", "k40", "k50"});
}

// Scenario 3: DeleteRange followed by Put resurrection, correct after Refresh
TEST_F(AMTVDBRefreshTest, Scenario03_DeleteRangeThenPutResurrection) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualPut("k30", "v30");

  Slice l("k05");
  Slice u("k45");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Delete [k15, k35) then resurrect k20
  DualDeleteRange("k15", "k35");
  DualPut("k20", "v20_resurrected");

  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_NONEMPTY");
#endif

  // After refresh: k10 visible, k20 resurrected visible, k30 deleted
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k10", "k15", "k20", "k30", "k40"});

  it_cand->Seek("k20");
  EXPECT_TRUE(it_cand->Valid());
  EXPECT_EQ(it_cand->value().ToString(), "v20_resurrected");
}

// Scenario 4: Multiple sequential refreshes
TEST_F(AMTVDBRefreshTest, Scenario04_MultipleSequentialRefreshes) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  Slice l("k00");
  Slice u("k99");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Round 1
  DualPut("k10", "v10");
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10"});

  // Round 2
  DualPut("k20", "v20");
  DualDeleteRange("k05", "k15");
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_NONEMPTY");
#endif
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k20"});

  // Round 3
  DualPut("k30", "v30");
  DualPut("k10", "v10_rev");
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k10", "k20", "k30"});

  // Round 4 (empty write refresh)
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k10", "k20", "k30"});
}

// Scenario 5: Same-SuperVersion fast path (active memtable updated in-place)
TEST_F(AMTVDBRefreshTest, Scenario05_SameSuperVersionFastPath) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_NONEMPTY");
#endif

  // Write additional tombstones without switching MemTable
  DualDeleteRange("k20", "k30");
  DualPut("k08", "v08");

  // Refresh in-place
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_NONEMPTY");
#endif

  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k05", "k08", "k10", "k20", "k30"});
}

// Scenario 6: MemTable switch triggering Full-Rebuild in Refresh
TEST_F(AMTVDBRefreshTest, Scenario06_MemTableSwitchFullRebuild) {
  // Use a small write buffer to trigger memtable switch
  OpenDualDB(/*cand_bounded_scan=*/true, BytewiseComparator(),
             /*write_buffer_size=*/4096);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k45");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Explicitly switch active MemTable to immutable without flushing to L0
  ASSERT_OK(
      static_cast_with_check<DBImpl>(db_native_.get())->TEST_SwitchMemtable());
  ASSERT_OK(
      static_cast_with_check<DBImpl>(db_cand_.get())->TEST_SwitchMemtable());

  // Active memtable switched! Refresh will detect sv_number_ != cur_sv_number
  // and trigger DoRefresh (Full-Rebuild).
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_EMPTY");
#endif

  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k05", "k10", "k20", "k30", "k40"});
}

// Scenario 7: Flush to L0 triggering Full-Rebuild in Refresh
TEST_F(AMTVDBRefreshTest, Scenario07_FlushToL0FullRebuild) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Flush to L0 SST!
  DualFlush();

  // Active memtable is now fresh/empty, tombstones are in L0 SST
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_EMPTY");
#endif

  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k05", "k10", "k20", "k30"});
}

// Scenario 8: Forward, Reverse, Seek, and SeekForPrev traversal consistency
TEST_F(AMTVDBRefreshTest, Scenario08_ForwardReverseSeekAndSeekForPrev) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  for (int i = 0; i < 50; i += 5) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    DualPut(buf, std::string("v_") + buf);
  }

  Slice l("k10");
  Slice u("k40");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Add multiple tombstones
  DualDeleteRange("k15", "k25");
  DualDeleteRange("k30", "k35");

  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));

  std::vector<std::string> targets;
  for (int i = 0; i <= 50; i += 2) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    targets.emplace_back(buf);
  }
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, targets);
}

// Scenario 9: Legal empty user key in Refresh
TEST_F(AMTVDBRefreshTest, Scenario09_EmptyUserKey) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("", "v_empty");
  DualPut("\x01", "v_01");
  DualPut("a", "v_a");
  DualPut("b", "v_b");
  DualPut("c", "v_c");

  Slice l("");
  Slice u("z");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Write tombstone covering empty key
  DualDeleteRange("", "b");

  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_NONEMPTY");
#endif

  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"", "\x01", "a", "b", "c"});
}

// Scenario 10: User-Defined Timestamps (UDT) Refresh
TEST_F(AMTVDBRefreshTest, Scenario10_UserDefinedTimestamps) {
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

  DualPutWithTs("k10", ts10, "v10");
  DualPutWithTs("k20", ts10, "v20");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;
  Slice read_ts(ts_read15);
  ro.timestamp = &read_ts;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Write range deletion at ts 20
  DualDeleteRangeWithTs("k15", "k25", ts20);

  // Refresh
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));

  // At ts 15, range deletion ts 20 is not yet visible
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k20"},
                            ucmp);

  // Now advance read timestamp to ts 25
  Slice read_ts25(ts_read25);
  ro.timestamp = &read_ts25;
  auto it_nat2 = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand2 = NewCandidateIterator(ro);
  ASSERT_OK(it_nat2->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand2.get()));

  // At ts 25, range deletion is active
  AssertIteratorsEquivalent(it_nat2.get(), it_cand2.get(), &l, &u, {"k10", "k20"},
                            ucmp);
}

// Scenario 11: Local build failure fallback during Refresh
TEST_F(AMTVDBRefreshTest, Scenario11_LocalBuildFailureFallback) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  DualDeleteRange("k15", "k25");

#ifndef NDEBUG
  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:LocalBuildFail",
      [](void* arg) {
        bool* fail = static_cast<bool*>(arg);
        *fail = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();
#endif

  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "NATIVE_FALLBACK_LOCAL_BUILD_FAILURE");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
#endif

  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k20"});
}

// Scenario 12: Final non-OK error propagation during Refresh
TEST_F(AMTVDBRefreshTest, Scenario12_FinalNonOKErrorPropagation) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_cand = NewCandidateIterator(ro);
  ASSERT_OK(it_cand->status());

#ifndef NDEBUG
  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:LocalBuildFail",
      [](void* arg) {
        bool* fail = static_cast<bool*>(arg);
        *fail = true;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:FallbackFail",
      [](void* arg) {
        bool* fail = static_cast<bool*>(arg);
        *fail = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = CandidateRefresh(it_cand.get());
  EXPECT_EQ(cand_last_outcome_, "FINAL_ERROR");
  EXPECT_TRUE(s.IsCorruption());
  EXPECT_TRUE(it_cand->status().IsCorruption());
  EXPECT_FALSE(it_cand->Valid());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
#endif
}

// Scenario 13: OK + nullptr slot replacement contract
TEST_F(AMTVDBRefreshTest, Scenario13_OKNullptrSlotReplacement) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  // Initially active memtable has range deletion in window
  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_NONEMPTY");
#endif

  // Now flush to L0: active memtable is cleared, so active memtable has 0 tombstones
  DualFlush();
  DualPut("k18", "v18");

  // Refresh: active memtable now returns OK + nullptr, clearing slot 0
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(CandidateRefresh(it_cand.get()));
#ifndef NDEBUG
  EXPECT_EQ(cand_last_outcome_, "LOCAL_VIEW_EMPTY");
#endif

  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u,
                            {"k05", "k10", "k18", "k30"});
}

// Scenario 14: Explicit Snapshot Refresh
TEST_F(AMTVDBRefreshTest, Scenario14_ExplicitSnapshotRefresh) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  const Snapshot* snap1_nat = db_native_->GetSnapshot();
  const Snapshot* snap1_cand = db_cand_->GetSnapshot();

  DualDeleteRange("k05", "k25");
  DualPut("k30", "v30");
  const Snapshot* snap2_nat = db_native_->GetSnapshot();
  const Snapshot* snap2_cand = db_cand_->GetSnapshot();

  Slice l("k00");
  Slice u("k40");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = NewCandidateIterator(ro);

  // Refresh to Snapshot 1: k10 visible, tombstone not present
  ASSERT_OK(it_nat->Refresh(snap1_nat));
  ASSERT_OK(CandidateRefresh(it_cand.get(), snap1_cand));
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k30"});

  // Refresh to Snapshot 2: k10 deleted by tombstone, k30 visible
  ASSERT_OK(it_nat->Refresh(snap2_nat));
  ASSERT_OK(CandidateRefresh(it_cand.get(), snap2_cand));
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k30"});

  // Refresh to latest (nullptr snapshot): equivalent to snap2
  ASSERT_OK(it_nat->Refresh(nullptr));
  ASSERT_OK(CandidateRefresh(it_cand.get(), nullptr));
  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k30"});

  db_native_->ReleaseSnapshot(snap1_nat);
  db_cand_->ReleaseSnapshot(snap1_cand);
  db_native_->ReleaseSnapshot(snap2_nat);
  db_cand_->ReleaseSnapshot(snap2_cand);
}

// Scenario 15: Re-operation and re-refresh after non-OK Refresh failure
TEST_F(AMTVDBRefreshTest, Scenario15_ReOperationAndReRefreshAfterNonOK) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_cand = NewCandidateIterator(ro);
  ASSERT_OK(it_cand->status());

#ifndef NDEBUG
  // Inject dual failure
  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:LocalBuildFail",
      [](void* arg) {
        bool* fail = static_cast<bool*>(arg);
        *fail = true;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:FallbackFail",
      [](void* arg) {
        bool* fail = static_cast<bool*>(arg);
        *fail = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = CandidateRefresh(it_cand.get());
  EXPECT_TRUE(s.IsCorruption());
  EXPECT_FALSE(it_cand->Valid());

  // Subsequent operations while in error state remain non-OK and invalid
  it_cand->SeekToFirst();
  EXPECT_FALSE(it_cand->Valid());
  EXPECT_TRUE(it_cand->status().IsCorruption());

  it_cand->Seek("k10");
  EXPECT_FALSE(it_cand->Valid());
  EXPECT_TRUE(it_cand->status().IsCorruption());

  // Clear failure injection and re-refresh
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Status s2 = CandidateRefresh(it_cand.get());
  EXPECT_OK(s2);
  it_cand->SeekToFirst();
  EXPECT_TRUE(it_cand->Valid());
  EXPECT_EQ(it_cand->key().ToString(), "k10");
#endif
}

// Scenario 16: Lazy initialization first activation before/after Refresh
TEST_F(AMTVDBRefreshTest, Scenario16_LazyInitFirstActivation) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  // Create iterator WITHOUT calling any Seek: internal iterator is uninitialized!
  auto it_nat = std::unique_ptr<Iterator>(db_native_->NewIterator(ro));
  auto it_cand = std::unique_ptr<Iterator>(db_cand_->NewIterator(ro));

  // Write new key
  DualPut("k20", "v20");

  // Call Refresh() on uninitialized iterator: must initialize internal iter and refresh sequence
  ASSERT_OK(it_nat->Refresh());
  ASSERT_OK(it_cand->Refresh());

  AssertIteratorsEquivalent(it_nat.get(), it_cand.get(), &l, &u, {"k10", "k20"});
}

// ============================================================================
// Section B5: Deterministic Concurrency Lifecycle Tests
// ============================================================================

// Concurrency 01: MarkImmutable occurs after reader acquires SuperVersion in Refresh
TEST_F(AMTVDBRefreshTest, Concurrency01_MarkImmutableAfterSuperVersionAcquisition) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_cand = NewCandidateIterator(ro);

#ifndef NDEBUG
  std::atomic<bool> reader_paused{false};
  std::atomic<bool> writer_done{false};

  SyncPoint::GetInstance()->SetCallBack(
      "ArenaWrappedDBIter::Refresh:SV", [&](void* /*arg*/) {
        reader_paused = true;
        while (!writer_done) {
          std::this_thread::yield();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Writer thread to switch MemTable and execute MarkImmutable
  std::thread writer_thread([&]() {
    while (!reader_paused) {
      std::this_thread::yield();
    }
    // Flush forces active MemTable to switch and call MarkImmutable()
    DualFlush();
    DualPut("k28", "v28");
    writer_done = true;
  });

  ASSERT_OK(it_cand->Refresh());
  writer_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  it_cand->SeekToFirst();
  EXPECT_TRUE(it_cand->Valid());
  EXPECT_EQ(it_cand->key().ToString(), "k10");
#endif
}

// Concurrency 02: MemTable Switch occurs during candidate extraction in local build
TEST_F(AMTVDBRefreshTest, Concurrency02_MemTableSwitchDuringLocalBuild) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_cand = NewCandidateIterator(ro);

#ifndef NDEBUG
  std::atomic<bool> reader_in_build{false};
  std::atomic<bool> writer_switched{false};

  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:BeforeCandidateExtraction",
      [&](void* /*arg*/) {
        reader_in_build = true;
        while (!writer_switched) {
          std::this_thread::yield();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread writer_thread([&]() {
    while (!reader_in_build) {
      std::this_thread::yield();
    }
    DualFlush();
    writer_switched = true;
  });

  ASSERT_OK(it_cand->Refresh());
  writer_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  it_cand->SeekToFirst();
  EXPECT_TRUE(it_cand->Valid());
  EXPECT_EQ(it_cand->key().ToString(), "k10");
#endif
}

// Concurrency 03: Flush occurs right before slot replacement in Refresh
TEST_F(AMTVDBRefreshTest, Concurrency03_FlushBeforeSlotReplacement) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_cand = NewCandidateIterator(ro);

#ifndef NDEBUG
  std::atomic<bool> reader_ready_to_replace{false};
  std::atomic<bool> writer_flushed{false};

  SyncPoint::GetInstance()->SetCallBack(
      "ArenaWrappedDBIter::Refresh:BeforeSlotReplacement", [&](void* /*arg*/) {
        reader_ready_to_replace = true;
        while (!writer_flushed) {
          std::this_thread::yield();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread writer_thread([&]() {
    while (!reader_ready_to_replace) {
      std::this_thread::yield();
    }
    DualFlush();
    DualPut("k30", "v30");
    writer_flushed = true;
  });

  ASSERT_OK(it_cand->Refresh());
  writer_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  it_cand->SeekToFirst();
  EXPECT_TRUE(it_cand->Valid());
  EXPECT_EQ(it_cand->key().ToString(), "k10");
#endif
}

// Concurrency 04: AMTV Run merge publishes new snapshot during Refresh
TEST_F(AMTVDBRefreshTest, Concurrency04_AMTVRunMergeDuringRefresh) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualDeleteRange("k15", "k25");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  auto it_cand = NewCandidateIterator(ro);

#ifndef NDEBUG
  std::atomic<bool> reader_pinned_snap{false};
  std::atomic<bool> background_merge_done{false};

  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:BeforeCandidateExtraction",
      [&](void* /*arg*/) {
        reader_pinned_snap = true;
        while (!background_merge_done) {
          std::this_thread::yield();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread bg_thread([&]() {
    while (!reader_pinned_snap) {
      std::this_thread::yield();
    }
    // Write multiple tombstones to trigger AMTV delta seal and merge
    for (int i = 0; i < 10; ++i) {
      char buf_s[16], buf_e[16];
      snprintf(buf_s, sizeof(buf_s), "d%02d", i * 2);
      snprintf(buf_e, sizeof(buf_e), "d%02d", i * 2 + 1);
      DualDeleteRange(buf_s, buf_e);
    }
    background_merge_done = true;
  });

  ASSERT_OK(it_cand->Refresh());
  bg_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  it_cand->SeekToFirst();
  EXPECT_TRUE(it_cand->Valid());
#endif
}

// Concurrency 05: Old Iterator retains old view while new SuperVersion and Snapshot are published
TEST_F(AMTVDBRefreshTest, Concurrency05_OldIteratorRetainsOldView) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  DualPut("k10", "v10");
  DualPut("k20", "v20");

  Slice l("k05");
  Slice u("k35");
  ReadOptions ro;
  ro.iterate_lower_bound = &l;
  ro.iterate_upper_bound = &u;

  // Reader 1 creates Iterator 1
  auto it_reader1 = NewCandidateIterator(ro);
  it_reader1->SeekToFirst();
  EXPECT_TRUE(it_reader1->Valid());
  EXPECT_EQ(it_reader1->key().ToString(), "k10");

  // Writer thread performs extensive updates, switches and flushes
  DualDeleteRange("k05", "k25");
  DualPut("k30", "v30");
  DualFlush();

  // Reader 2 creates Iterator 2 with new state
  auto it_reader2 = NewCandidateIterator(ro);
  it_reader2->SeekToFirst();
  EXPECT_TRUE(it_reader2->Valid());
  EXPECT_EQ(it_reader2->key().ToString(), "k30");

  // Reader 1 still safely holds old SuperVersion and old view!
  it_reader1->Seek("k10");
  EXPECT_TRUE(it_reader1->Valid());
  EXPECT_EQ(it_reader1->key().ToString(), "k10");
  it_reader1->Seek("k20");
  EXPECT_TRUE(it_reader1->Valid());
  EXPECT_EQ(it_reader1->key().ToString(), "k20");

  // Reader 1 refreshes to catch up to new state
  ASSERT_OK(it_reader1->Refresh());
  it_reader1->SeekToFirst();
  EXPECT_TRUE(it_reader1->Valid());
  EXPECT_EQ(it_reader1->key().ToString(), "k30");
}

// Concurrency 06: Controlled multi-threaded lifecycle stress
TEST_F(AMTVDBRefreshTest, Concurrency06_MultiThreadControlledLifecycle) {
  OpenDualDB(/*cand_bounded_scan=*/true);

  const int kTotalBatches = 50;
  std::atomic<int> current_batch{0};
  std::atomic<bool> stop_flag{false};

  // Pre-populate data
  for (int i = 0; i < 100; i += 10) {
    char buf[16];
    snprintf(buf, sizeof(buf), "k%02d", i);
    DualPut(buf, "val_init");
  }

  // 1 Writer thread
  std::thread writer_thread([&]() {
    for (int b = 1; b <= kTotalBatches; ++b) {
      char kbuf[16], sbuf[16], ebuf[16];
      snprintf(kbuf, sizeof(kbuf), "k%02d", (b * 7) % 100);
      snprintf(sbuf, sizeof(sbuf), "k%02d", (b * 11) % 90);
      snprintf(ebuf, sizeof(ebuf), "k%02d", ((b * 11) % 90) + 5);

      DualPut(kbuf, "val_batch");
      DualDeleteRange(sbuf, ebuf);
      current_batch.store(b, std::memory_order_release);
      std::this_thread::yield();
    }
    stop_flag.store(true, std::memory_order_release);
  });

  // 1 Maintenance thread (periodic switch and flush)
  std::thread maint_thread([&]() {
    while (!stop_flag.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      DualFlush();
    }
  });

  // 4 Independent Reader threads, each with its OWN dedicated Iterator
  std::vector<std::thread> reader_threads;
  std::atomic<uint64_t> total_scans{0};

  for (int r = 0; r < 4; ++r) {
    reader_threads.emplace_back([&, r]() {
      Slice l("k10");
      Slice u("k80");
      ReadOptions ro;
      ro.iterate_lower_bound = &l;
      ro.iterate_upper_bound = &u;

      auto it = NewCandidateIterator(ro);
      int last_seen_batch = 0;

      while (!stop_flag.load(std::memory_order_acquire) ||
             last_seen_batch < kTotalBatches) {
        int b = current_batch.load(std::memory_order_acquire);
        if (b > last_seen_batch) {
          ASSERT_OK(it->Refresh());
          last_seen_batch = b;
        }

        // Forward scan
        it->SeekToFirst();
        int count = 0;
        while (it->Valid() && count < 20) {
          count++;
          it->Next();
        }

        // Reverse scan
        it->SeekToLast();
        count = 0;
        while (it->Valid() && count < 20) {
          count++;
          it->Prev();
        }

        total_scans.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::yield();
      }
    });
  }

  writer_thread.join();
  maint_thread.join();
  for (auto& t : reader_threads) {
    t.join();
  }

  EXPECT_GT(total_scans.load(), 50u);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
