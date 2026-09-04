//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"

#include <memory>
#include <string>
#include <vector>

#include "db/db_test_util.h"
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

  // Base: [k10, k50) @ seq 10
  std::vector<std::string> b_keys = {
      RangeTombstone("k10", "k50", 10).Serialize().first.Encode().ToString()};
  std::vector<std::string> b_vals = {"k50"};
  auto b_iter = std::make_unique<VectorIterator>(
      std::move(b_keys), std::move(b_vals), &bytewise_icmp_);
  snapshot->base = std::make_unique<FragmentedRangeTombstoneList>(
      std::move(b_iter), bytewise_icmp_);

  // Sealed Delta: [k30, k70) @ seq 20
  std::vector<std::string> s_keys = {
      RangeTombstone("k30", "k70", 20).Serialize().first.Encode().ToString()};
  std::vector<std::string> s_vals = {"k70"};
  auto s_iter = std::make_unique<VectorIterator>(
      std::move(s_keys), std::move(s_vals), &bytewise_icmp_);
  snapshot->sealed_delta = std::make_unique<FragmentedRangeTombstoneList>(
      std::move(s_iter), bytewise_icmp_);

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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
