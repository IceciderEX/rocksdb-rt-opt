//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv_local_scan_view.h"

#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/memtable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_type.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class AMTVLocalScanViewTest : public testing::Test {
 public:
  AMTVLocalScanViewTest()
      : dbname_(test::PerThreadDBPath("amtv_local_scan_view_test")) {}

  ~AMTVLocalScanViewTest() override {
    CloseDB();
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  void SetUp() override {
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  void TearDown() override {
    CloseDB();
    EXPECT_OK(DestroyDB(dbname_, Options()));
  }

  void OpenDB(bool enable_amtv = true,
              bool amtv_enable_bounded_scan_view = true) {
    CloseDB();
    Options options;
    options.create_if_missing = true;
    options.enable_amtv = enable_amtv;
    options.amtv_enable_bounded_scan_view = amtv_enable_bounded_scan_view;
    options.amtv_delta_tombstones = 4;
    options.amtv_merge_soft_limit = 4;
    options.amtv_hard_layer_limit = 8;
    options.amtv_max_sealed_deltas = 8;
    Status s = DB::Open(options, dbname_, &db_);
    ASSERT_OK(s);
  }

  void CloseDB() {
    db_.reset();
  }

  MemTable* GetActiveMemTable() {
    if (db_ == nullptr) return nullptr;
    ColumnFamilyData* cfd =
        static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
            ->cfd();
    return cfd->mem();
  }

  const InternalKeyComparator& GetInternalKeyComparator() {
    ColumnFamilyData* cfd =
        static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
            ->cfd();
    return cfd->internal_comparator();
  }

 protected:
  std::string dbname_;
  std::unique_ptr<DB> db_;
};

// ============================================================================
// 1. Options Specifications & Testing
// ============================================================================

TEST_F(AMTVLocalScanViewTest, OptionsDefaultValue) {
  ColumnFamilyOptions cf_opt;
  // Must be strictly false by default
  EXPECT_FALSE(cf_opt.amtv_enable_bounded_scan_view);

  Options opt;
  EXPECT_FALSE(opt.amtv_enable_bounded_scan_view);
}

TEST_F(AMTVLocalScanViewTest, OptionsStringParsing) {
  ConfigOptions config_options;
  ColumnFamilyOptions base_opt;
  ColumnFamilyOptions new_opt;

  // Test parsing 'true'
  Status s = GetColumnFamilyOptionsFromString(
      config_options, base_opt, "amtv_enable_bounded_scan_view=true", &new_opt);
  ASSERT_OK(s);
  EXPECT_TRUE(new_opt.amtv_enable_bounded_scan_view);

  // Test parsing 'false'
  s = GetColumnFamilyOptionsFromString(
      config_options, base_opt, "amtv_enable_bounded_scan_view=false", &new_opt);
  ASSERT_OK(s);
  EXPECT_FALSE(new_opt.amtv_enable_bounded_scan_view);

  // Test invalid value reports error
  s = GetColumnFamilyOptionsFromString(
      config_options, base_opt, "amtv_enable_bounded_scan_view=not_a_bool",
      &new_opt);
  ASSERT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST_F(AMTVLocalScanViewTest, OptionsSerializationAndRecovery) {
  ConfigOptions config_options;
  ColumnFamilyOptions cf_opt;
  cf_opt.amtv_enable_bounded_scan_view = true;

  std::string opt_str;
  ASSERT_OK(GetStringFromColumnFamilyOptions(config_options, cf_opt, &opt_str));
  EXPECT_NE(opt_str.find("amtv_enable_bounded_scan_view=true"),
            std::string::npos);

  ColumnFamilyOptions recovered_opt;
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, ColumnFamilyOptions(),
                                             opt_str, &recovered_opt));
  EXPECT_TRUE(recovered_opt.amtv_enable_bounded_scan_view);
}

TEST_F(AMTVLocalScanViewTest, OptionsDBReopenPersistence) {
  OpenDB(/*enable_amtv=*/true, /*amtv_enable_bounded_scan_view=*/true);
  ASSERT_OK(db_->Put(WriteOptions(), "k10", "v10"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k30"));
  CloseDB();

  // Reopen DB and verify correctness
  OpenDB(/*enable_amtv=*/true, /*amtv_enable_bounded_scan_view=*/true);
  std::string val;
  ASSERT_OK(db_->Get(ReadOptions(), "k10", &val));
  EXPECT_EQ(val, "v10");
  CloseDB();
}

TEST_F(AMTVLocalScanViewTest, NativeDefaultZeroBehaviorChange) {
  // DB with default options (amtv_enable_bounded_scan_view = false)
  OpenDB(/*enable_amtv=*/false, /*amtv_enable_bounded_scan_view=*/false);
  ASSERT_OK(db_->Put(WriteOptions(), "key_a", "val_a"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "key_m", "key_z"));

  std::string val;
  ASSERT_OK(db_->Get(ReadOptions(), "key_a", &val));
  EXPECT_EQ(val, "val_a");

  // ReadOptions bounded scan
  ReadOptions ropt;
  Slice l("key_a");
  Slice u("key_z");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;
  std::unique_ptr<Iterator> it(db_->NewIterator(ropt));
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key(), "key_a");
  it->Next();
  EXPECT_FALSE(it->Valid());
  it.reset();
  CloseDB();
}

// ============================================================================
// 2. Candidate Intersection Logic
// ============================================================================

TEST_F(AMTVLocalScanViewTest, CandidateIntersectionLogic) {
  const Comparator* ucmp = BytewiseComparator();
  Slice l("k20");
  Slice u("k50");

  // Tombstone completely inside: [k25, k35)
  EXPECT_TRUE(AMTVScanIsCandidateIntersecting("k25", "k35", &l, &u, ucmp));

  // Straddles lower bound: [k10, k30)
  EXPECT_TRUE(AMTVScanIsCandidateIntersecting("k10", "k30", &l, &u, ucmp));

  // Straddles upper bound: [k40, k60)
  EXPECT_TRUE(AMTVScanIsCandidateIntersecting("k40", "k60", &l, &u, ucmp));

  // Completely encompasses window: [k10, k60)
  EXPECT_TRUE(AMTVScanIsCandidateIntersecting("k10", "k60", &l, &u, ucmp));

  // Completely to the left: [k05, k15) (end <= lower)
  EXPECT_FALSE(AMTVScanIsCandidateIntersecting("k05", "k15", &l, &u, ucmp));
  // Exactly abuts lower bound: [k05, k20) (end == lower)
  EXPECT_FALSE(AMTVScanIsCandidateIntersecting("k05", "k20", &l, &u, ucmp));

  // Completely to the right: [k55, k70) (start >= upper)
  EXPECT_FALSE(AMTVScanIsCandidateIntersecting("k55", "k70", &l, &u, ucmp));
  // Exactly abuts upper bound: [k50, k70) (start == upper)
  EXPECT_FALSE(AMTVScanIsCandidateIntersecting("k50", "k70", &l, &u, ucmp));

  // Inverted window: [k50, k20)
  Slice inv_l("k50");
  Slice inv_u("k20");
  EXPECT_FALSE(AMTVScanIsCandidateIntersecting("k25", "k35", &inv_l, &inv_u, ucmp));
}

// ============================================================================
// 3. Unified Factory Semantic Contract Tests
// ============================================================================

TEST_F(AMTVLocalScanViewTest, FactoryEmptyMemTable) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ReadOptions ropt;
  Slice l("k10");
  Slice u("k50");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  ASSERT_OK(s);
  EXPECT_EQ(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kEmpty);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kNone);
  EXPECT_EQ(meta.raw_tombstone_count, 0U);
  EXPECT_EQ(meta.candidate_tombstone_count, 0U);
}

TEST_F(AMTVLocalScanViewTest, FactoryLocalBoundedHit) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k35", "k60"));

  ReadOptions ropt;
  Slice l("k10");
  Slice u("k50");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  ASSERT_OK(s);
  ASSERT_NE(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kLocal);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kNone);
  EXPECT_EQ(meta.raw_tombstone_count, 2U);
  EXPECT_EQ(meta.candidate_tombstone_count, 2U);

  // Validate navigation on the returned iterator
  out_iter->Seek("k25");
  ASSERT_TRUE(out_iter->Valid());
  EXPECT_LE(out_iter->start_key().user_key.compare("k25"), 0);
  EXPECT_GT(out_iter->end_key().user_key.compare("k25"), 0);

  // Boundary truncation verification: start >= l (k10) and end <= u (k50)
  out_iter->SeekToFirst();
  while (out_iter->Valid()) {
    EXPECT_GE(out_iter->start_key().user_key.compare(l), 0);
    EXPECT_LE(out_iter->end_key().user_key.compare(u), 0);
    out_iter->Next();
  }
}

TEST_F(AMTVLocalScanViewTest, FactoryNonIntersectingTombstones) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  // Tombstones in [k20, k40)
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  // Window entirely outside: [k70, k90)
  ReadOptions ropt;
  Slice l("k70");
  Slice u("k90");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  ASSERT_OK(s);
  // No candidates intersect [k70, k90) -> legitimate empty result
  EXPECT_EQ(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kEmpty);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kNone);
  EXPECT_EQ(meta.raw_tombstone_count, 1U);
  EXPECT_EQ(meta.candidate_tombstone_count, 0U);
}

TEST_F(AMTVLocalScanViewTest, FactoryFallbackDisabledOption) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  ReadOptions ropt;
  Slice l("k10");
  Slice u("k50");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  // Pass enable_bounded_scan_view = false
  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/false, &out_iter, &meta);

  ASSERT_OK(s);
  ASSERT_NE(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kNativeFallback);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kDisabled);

  // Native full iterator covers [k20, k40)
  out_iter->Seek("k25");
  ASSERT_TRUE(out_iter->Valid());
  EXPECT_EQ(out_iter->start_key().user_key, "k20");
  EXPECT_EQ(out_iter->end_key().user_key, "k40");
}

TEST_F(AMTVLocalScanViewTest, FactoryFallbackUnbounded) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  // Lower bound set, upper bound null
  ReadOptions ropt;
  Slice l("k10");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = nullptr;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  ASSERT_OK(s);
  ASSERT_NE(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kNativeFallback);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kUnbounded);
}

TEST_F(AMTVLocalScanViewTest, FactoryFallbackInvertedBounds) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  // Inverted: L >= U
  ReadOptions ropt;
  Slice l("k60");
  Slice u("k20");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  ASSERT_OK(s);
  ASSERT_NE(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kNativeFallback);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kInvertedBounds);
}

TEST_F(AMTVLocalScanViewTest, FactoryActiveMemTableCallerContract) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  // Active memtable contract: mem is mutable active memtable
  ASSERT_FALSE(mem->IsImmutable());

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  ReadOptions ropt;
  Slice l("k10");
  Slice u("k50");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  ASSERT_OK(s);
  ASSERT_NE(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kLocal);
}

TEST_F(AMTVLocalScanViewTest, FactoryFallbackAMTVFailure) {
  CloseDB();
  Options options;
  options.create_if_missing = true;
  options.enable_amtv = true;
  options.amtv_enable_bounded_scan_view = true;
  options.amtv_delta_tombstones = 4;
  options.amtv_merge_soft_limit = 16;
  options.amtv_hard_layer_limit = 8;
  options.write_buffer_size = 64 * 1024 * 1024;
  ASSERT_OK(DB::Open(options, dbname_, &db_));

  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  // Write 40 DeleteRanges (10 deltas of 4 tombstones).
  // First 8 deltas seal 8 runs. The 9th delta triggers hard_layer_limit (8) fallback!
  for (int i = 0; i < 40; ++i) {
    char start[32];
    char end[32];
    snprintf(start, sizeof(start), "k%04d", i * 10);
    snprintf(end, sizeof(end), "k%04d", i * 10 + 5);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), start, end));
  }

  AMTVState* state = mem->GetAMTVState();
  ASSERT_NE(state, nullptr);
  ASSERT_TRUE(state->is_fallback_required());

  ReadOptions ropt;
  Slice l("k0010");
  Slice u("k0100");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  ASSERT_OK(s);
  ASSERT_NE(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kNativeFallback);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kAMTVUnavailable);
}

#ifndef NDEBUG
TEST_F(AMTVLocalScanViewTest, FactorySyncPointFailureInjection) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  // Inject local build failure
  SyncPoint::GetInstance()->SetCallBack(
      "BuildActiveMemTableRangeDelIteratorForScan:LocalBuildFail",
      [](void* arg) {
        bool* flag = static_cast<bool*>(arg);
        *flag = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions ropt;
  Slice l("k10");
  Slice u("k50");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  AMTVScanBuildMetadata meta;

  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(s);
  // Safely fell back to Native full iterator!
  ASSERT_NE(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kNativeFallback);
  EXPECT_EQ(meta.fallback_reason, AMTVScanFallbackReason::kLocalBuildFailed);

  out_iter->Seek("k25");
  ASSERT_TRUE(out_iter->Valid());
  EXPECT_EQ(out_iter->start_key().user_key, "k20");
  EXPECT_EQ(out_iter->end_key().user_key, "k40");
}
#endif

TEST_F(AMTVLocalScanViewTest, FactoryStateAliasingLifetimeAndASan) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  std::unique_ptr<TruncatedRangeDelIterator> persistent_iter;

  // Inner scope creates temporary string buffers and ReadOptions
  {
    std::string temp_lower = "k10";
    std::string temp_upper = "k50";
    ReadOptions ropt;
    Slice l(temp_lower);
    Slice u(temp_upper);
    ropt.iterate_lower_bound = &l;
    ropt.iterate_upper_bound = &u;

    AMTVScanBuildMetadata meta;
    Status s = BuildActiveMemTableRangeDelIteratorForScan(
        mem, ropt, kMaxSequenceNumber, icmp,
        /*enable_bounded_scan_view=*/true, &persistent_iter, &meta);

    ASSERT_OK(s);
    ASSERT_NE(persistent_iter, nullptr);
    EXPECT_EQ(meta.mode, AMTVScanMode::kLocal);
  }
  // temp_lower and temp_upper have gone out of scope and are destroyed!
  // But persistent_iter owns LocalRangeDelViewState via aliasing shared_ptr.

  ASSERT_NE(persistent_iter, nullptr);
  persistent_iter->Seek("k25");
  ASSERT_TRUE(persistent_iter->Valid());
  EXPECT_EQ(persistent_iter->start_key().user_key, "k20");
  EXPECT_EQ(persistent_iter->end_key().user_key, "k40");

  persistent_iter->SeekToFirst();
  ASSERT_TRUE(persistent_iter->Valid());

  // Destroy persistent_iter. ASan will verify clean deallocation with no UAF or leak.
  persistent_iter.reset();
  EXPECT_EQ(persistent_iter, nullptr);
}

TEST_F(AMTVLocalScanViewTest, FactoryRefreshClearingOldSlot0) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), "k20", "k40"));

  // 1. Initial construction: Slot 0 gets an iterator
  ReadOptions ropt;
  Slice l("k10");
  Slice u("k50");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  std::unique_ptr<TruncatedRangeDelIterator> slot0;
  AMTVScanBuildMetadata meta;
  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &slot0, &meta);
  ASSERT_OK(s);
  ASSERT_NE(slot0, nullptr);

  // 2. Simulate Refresh for a non-overlapping window [k70, k90)
  // Factory returns Status::OK() + new_iter == nullptr
  Slice l2("k70");
  Slice u2("k90");
  ropt.iterate_lower_bound = &l2;
  ropt.iterate_upper_bound = &u2;

  std::unique_ptr<TruncatedRangeDelIterator> new_iter;
  s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &new_iter, &meta);
  ASSERT_OK(s);
  EXPECT_EQ(new_iter, nullptr);

  // Refresh Contract: Unconditionally replace Slot 0 when Status::OK()
  slot0 = std::move(new_iter);

  // Old Slot 0 must be cleared!
  EXPECT_EQ(slot0, nullptr);
}

TEST_F(AMTVLocalScanViewTest, FactoryInvalidArgumentChecks) {
  OpenDB();
  MemTable* mem = GetActiveMemTable();
  ASSERT_NE(mem, nullptr);
  const auto& icmp = GetInternalKeyComparator();

  ReadOptions ropt;
  Slice l("k10");
  Slice u("k50");
  ropt.iterate_lower_bound = &l;
  ropt.iterate_upper_bound = &u;

  // 1. out_iter is null
  Status s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, /*out_iter=*/nullptr);
  EXPECT_TRUE(s.IsInvalidArgument());

  // 2. memtable is null
  std::unique_ptr<TruncatedRangeDelIterator> out_iter;
  s = BuildActiveMemTableRangeDelIteratorForScan(
      /*memtable=*/nullptr, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter);
  EXPECT_TRUE(s.IsInvalidArgument());

  // 3. ignore_range_deletions = true
  ropt.ignore_range_deletions = true;
  AMTVScanBuildMetadata meta;
  s = BuildActiveMemTableRangeDelIteratorForScan(
      mem, ropt, kMaxSequenceNumber, icmp,
      /*enable_bounded_scan_view=*/true, &out_iter, &meta);
  ASSERT_OK(s);
  EXPECT_EQ(out_iter, nullptr);
  EXPECT_EQ(meta.mode, AMTVScanMode::kEmpty);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
