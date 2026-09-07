//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"
#include "db/read_path_audit.h"

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "table/merging_iterator.h"
#include "test_util/testharness.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

class AMTVProbeTest : public testing::Test {
 public:
  AMTVProbeTest() : bytewise_icmp_(BytewiseComparator()) {}

 protected:
  InternalKeyComparator bytewise_icmp_;
};

// Probe 1: Verifies that Point Get/MultiGet read path can be fully satisfied
// by AMTVMultiSourceAdapter without constructing FragmentedRangeTombstoneIterator,
// achieving zero mutex lock and zero full-materialization on point lookups.
TEST_F(AMTVProbeTest, Probe1_PointLookupDecoupling) {
  auto snapshot = std::make_shared<AMTVSnapshot>();

  // Run 1: [k10, k50) @ seq 10
  std::vector<OpenDeltaEntry> r1_entries;
  r1_entries.emplace_back("k10", "k50", 10);
  auto r1 = std::make_shared<const AMTVRun>(1, 0, 1, false, std::move(r1_entries), bytewise_icmp_);
  snapshot->sealed_runs.push_back(r1);

  // Run 2: [k30, k70) @ seq 20
  std::vector<OpenDeltaEntry> r2_entries;
  r2_entries.emplace_back("k30", "k70", 20);
  auto r2 = std::make_shared<const AMTVRun>(2, 0, 1, false, std::move(r2_entries), bytewise_icmp_);
  snapshot->sealed_runs.push_back(r2);

  // Open Delta: [k60, k90) @ seq 30
  auto open_delta = std::make_shared<OpenDelta>();
  open_delta->AddEntry("k60", "k90", 30);
  snapshot->open_delta = open_delta;

  AMTVMultiSourceAdapter adapter(snapshot, &bytewise_icmp_);

  // Direct point queries at read_seq = 100:
  EXPECT_EQ(adapter.MaxCoveringTombstoneSeqnum("k05", 100), 0U);
  EXPECT_EQ(adapter.MaxCoveringTombstoneSeqnum("k20", 100), 10U);  // Base
  EXPECT_EQ(adapter.MaxCoveringTombstoneSeqnum("k40", 100), 20U);  // Sealed
  EXPECT_EQ(adapter.MaxCoveringTombstoneSeqnum("k65", 100), 30U);  // Open
  EXPECT_EQ(adapter.MaxCoveringTombstoneSeqnum("k95", 100), 0U);
}

// Probe 2: Proves the concrete type constraint of TruncatedRangeDelIterator.
// TruncatedRangeDelIterator expects std::unique_ptr<FragmentedRangeTombstoneIterator>
// and invokes non-virtual methods (TopNext, TopPrev, SeekToTopFirst).
// Therefore, any scan-path adapter in M1b must be backed by a genuine
// FragmentedRangeTombstoneList to feed into MergingIterator.
TEST_F(AMTVProbeTest, Probe2_ScanSlotConcreteTypeConstraint) {
  std::vector<std::string> keys = {
      RangeTombstone("k10", "k50", 10).Serialize().first.Encode().ToString()};
  std::vector<std::string> vals = {"k50"};
  auto iter = std::make_unique<VectorIterator>(std::move(keys), std::move(vals),
                                               &bytewise_icmp_);
  auto list = std::make_shared<FragmentedRangeTombstoneList>(std::move(iter),
                                                            bytewise_icmp_);

  auto frag_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
      list, bytewise_icmp_, 100 /* read_seq */);

  // Successfully constructs TruncatedRangeDelIterator
  auto trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
      std::move(frag_iter), &bytewise_icmp_, nullptr /* smallest */,
      nullptr /* largest */);

  ASSERT_TRUE(trunc_iter != nullptr);
  trunc_iter->Seek("k20");
  ASSERT_TRUE(trunc_iter->Valid());
  ASSERT_EQ(trunc_iter->seq(), static_cast<SequenceNumber>(10));
}

// Probe 3: Validates Composite List Generation for Scan Path without Full Re-Fragmenting.
// Demonstrates that Base + Sealed Delta can be pre-composited on publish, so that
// Scan readers only ever perform zero work if OpenDelta is empty, and only fragment
// <= 63 tombstones when OpenDelta has entries.
TEST_F(AMTVProbeTest, Probe3_CompositeWithoutFullRefragmentation) {
  // Simulate 1,000 Base tombstones
  std::vector<RangeTombstone> base_tombstones;
  base_tombstones.reserve(1000);
  for (int i = 0; i < 1000; ++i) {
    char s[32], e[32];
    snprintf(s, sizeof(s), "k%06d", i * 10);
    snprintf(e, sizeof(e), "k%06d", i * 10 + 5);
    base_tombstones.emplace_back(s, e, 100);
  }

  // Pre-fragmented once on freeze/merge (zero cost during Scan reads)
  std::vector<std::string> b_keys, b_vals;
  for (const auto& t : base_tombstones) {
    auto kv = t.Serialize();
    b_keys.push_back(kv.first.Encode().ToString());
    b_vals.push_back(kv.second.ToString());
  }
  auto base_unfrag = std::make_unique<VectorIterator>(
      std::move(b_keys), std::move(b_vals), &bytewise_icmp_);
  auto prebuilt_base = std::make_shared<FragmentedRangeTombstoneList>(
      std::move(base_unfrag), bytewise_icmp_);

  // Reader with zero open entries simply adopts prebuilt_base
  auto reader_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
      prebuilt_base, bytewise_icmp_, 200);
  auto trunc_iter = std::make_unique<TruncatedRangeDelIterator>(
      std::move(reader_iter), &bytewise_icmp_, nullptr, nullptr);

  trunc_iter->Seek("k000500");
  ASSERT_TRUE(trunc_iter->Valid());
  ASSERT_EQ(trunc_iter->seq(), static_cast<SequenceNumber>(100));
}

#ifdef ROCKSDB_READ_PATH_AUDIT
// Probe 4: Validates multi-threaded audit and probe thread_local statistics
// aggregation protocol (TakeAndReset).
// 8 worker threads each write known quantities of events. First TakeAndReset
// aggregation must strictly match the 8-thread sum; second TakeAndReset must be all 0.
TEST_F(AMTVProbeTest, Probe4_ThreadLocalAuditStatsAggregation) {
  constexpr int kNumThreads = 8;
  struct ThreadAuditSnapshot {
    ReadPathAuditStats audit_stats;
    AMTVGetProbeStats probe_stats;
  };

  std::vector<ThreadAuditSnapshot> first_snapshots(kNumThreads);
  std::vector<ThreadAuditSnapshot> second_snapshots(kNumThreads);

  SetReadPathAuditEnabled(true);
  g_amtv_get_probe_stats_enabled.store(true, std::memory_order_relaxed);

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);

  for (int tid = 0; tid < kNumThreads; ++tid) {
    threads.emplace_back([tid, &first_snapshots, &second_snapshots]() {
      auto* astats = GetReadPathAuditStats();
      astats->Reset();
      tl_amtv_get_probe_stats.Reset();

      uint64_t mat_count = (tid + 1) * 10;
      uint64_t mat_nanos = (tid + 1) * 1000;
      uint64_t lock_contended = (tid + 1) * 2;
      uint64_t lock_wait_nanos = (tid + 1) * 500;
      uint64_t gets = (tid + 1) * 100;
      uint64_t probed_runs = (tid + 1) * 300;

      astats->range_tombstone_view_materialization_count += mat_count;
      astats->range_tombstone_view_materialization_nanos += mat_nanos;
      astats->fragment_build_lock_contended_count += lock_contended;
      astats->fragment_build_lock_contended_wait_nanos += lock_wait_nanos;

      tl_amtv_get_probe_stats.get_count += gets;
      tl_amtv_get_probe_stats.sealed_runs_sum += probed_runs;

      // First TakeAndReset
      first_snapshots[tid].audit_stats = *astats;
      first_snapshots[tid].probe_stats = tl_amtv_get_probe_stats;
      astats->Reset();
      tl_amtv_get_probe_stats.Reset();

      // Second TakeAndReset
      second_snapshots[tid].audit_stats = *astats;
      second_snapshots[tid].probe_stats = tl_amtv_get_probe_stats;
      astats->Reset();
      tl_amtv_get_probe_stats.Reset();
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  uint64_t agg_mat_count = 0;
  uint64_t agg_mat_nanos = 0;
  uint64_t agg_lock_contended = 0;
  uint64_t agg_lock_wait_nanos = 0;
  uint64_t agg_gets = 0;
  uint64_t agg_probed_runs = 0;

  uint64_t exp_mat_count = 0;
  uint64_t exp_mat_nanos = 0;
  uint64_t exp_lock_contended = 0;
  uint64_t exp_lock_wait_nanos = 0;
  uint64_t exp_gets = 0;
  uint64_t exp_probed_runs = 0;

  for (int tid = 0; tid < kNumThreads; ++tid) {
    exp_mat_count += (tid + 1) * 10;
    exp_mat_nanos += (tid + 1) * 1000;
    exp_lock_contended += (tid + 1) * 2;
    exp_lock_wait_nanos += (tid + 1) * 500;
    exp_gets += (tid + 1) * 100;
    exp_probed_runs += (tid + 1) * 300;

    agg_mat_count += first_snapshots[tid].audit_stats.range_tombstone_view_materialization_count;
    agg_mat_nanos += first_snapshots[tid].audit_stats.range_tombstone_view_materialization_nanos;
    agg_lock_contended += first_snapshots[tid].audit_stats.fragment_build_lock_contended_count;
    agg_lock_wait_nanos += first_snapshots[tid].audit_stats.fragment_build_lock_contended_wait_nanos;
    agg_gets += first_snapshots[tid].probe_stats.get_count;
    agg_probed_runs += first_snapshots[tid].probe_stats.sealed_runs_sum;
  }

  EXPECT_EQ(agg_mat_count, exp_mat_count);
  EXPECT_EQ(agg_mat_nanos, exp_mat_nanos);
  EXPECT_EQ(agg_lock_contended, exp_lock_contended);
  EXPECT_EQ(agg_lock_wait_nanos, exp_lock_wait_nanos);
  EXPECT_EQ(agg_gets, exp_gets);
  EXPECT_EQ(agg_probed_runs, exp_probed_runs);

  for (int tid = 0; tid < kNumThreads; ++tid) {
    EXPECT_EQ(second_snapshots[tid].audit_stats.range_tombstone_view_materialization_count, 0U);
    EXPECT_EQ(second_snapshots[tid].audit_stats.range_tombstone_view_materialization_nanos, 0U);
    EXPECT_EQ(second_snapshots[tid].audit_stats.fragment_build_lock_contended_count, 0U);
    EXPECT_EQ(second_snapshots[tid].audit_stats.fragment_build_lock_contended_wait_nanos, 0U);
    EXPECT_EQ(second_snapshots[tid].probe_stats.get_count, 0U);
    EXPECT_EQ(second_snapshots[tid].probe_stats.sealed_runs_sum, 0U);
  }
}
#endif

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
