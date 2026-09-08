//  Copyright (c) 2026-present. All rights reserved.
//  Test-only reference implementation and differential Oracle for M1c-P0:
//  AMTV multi-run scan semantics and interface feasibility verification.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Represents a single contiguous range tombstone fragment [start_key, end_key)
// with its maximum visible sequence number and optional user-defined timestamp.
struct AMTVFragment {
  std::string start_key;
  std::string end_key;
  SequenceNumber seq = 0;
  std::string timestamp;  // Non-empty if user-defined timestamp is enabled

  AMTVFragment() = default;
  AMTVFragment(std::string sk, std::string ek, SequenceNumber s,
               std::string ts = "")
      : start_key(std::move(sk)),
        end_key(std::move(ek)),
        seq(s),
        timestamp(std::move(ts)) {}

  bool operator==(const AMTVFragment& o) const {
    return start_key == o.start_key && end_key == o.end_key && seq == o.seq &&
           timestamp == o.timestamp;
  }
  bool operator!=(const AMTVFragment& o) const { return !(*this == o); }
};

// Input snapshot describing all runs in an AMTV active/frozen MemTable.
struct AMTVScanInput {
  std::vector<RangeTombstone> base;
  std::vector<std::vector<RangeTombstone>> sealed_runs;
  std::vector<RangeTombstone> open_delta;
  SequenceNumber read_seq = kMaxSequenceNumber;
  const Slice* timestamp_upper_bound = nullptr;
  const Comparator* ucmp = BytewiseComparator();
};

// Test-only Dynamic K-Way Sweep-Line Reference Iterator.
// Demonstrates the theoretical interface and navigation semantics required
// to consume multi-run AMTV range deletions during Scan.
class AMTVMultiRunScanIterator {
 public:
  // Constructs the iterator. If coalesce_adjacent is true, adjacent fragments
  // with identical covering sequence and timestamp are merged.
  // If false, elementary fragments matching native fragmentation are preserved.
  AMTVMultiRunScanIterator(const AMTVScanInput& input,
                           bool coalesce_adjacent = false);

  void SeekToTopFirst();
  void SeekToTopLast();
  void Seek(const Slice& target);
  void SeekForPrev(const Slice& target);
  void TopNext();
  void TopPrev();

  bool Valid() const;
  Slice start_key() const;
  Slice end_key() const;
  SequenceNumber seq() const;
  Slice timestamp() const;

  // Max covering sequence number for a discrete user key.
  SequenceNumber MaxCoveringTombstoneSeqnum(const Slice& user_key) const;

  // Point deletion check equivalent to RocksDB RangeDelAggregator::ShouldDelete.
  bool ShouldDelete(const ParsedInternalKey& parsed) const;

  // Access all materialized fragments.
  const std::vector<AMTVFragment>& fragments() const { return fragments_; }

  // Complexity & Lifecycle Quantifications.
  size_t num_input_runs() const { return num_runs_; }
  size_t num_raw_tombstones() const { return num_raw_tombstones_; }
  size_t num_boundary_events() const { return num_boundary_events_; }
  size_t num_output_fragments() const { return fragments_.size(); }

 private:
  void MaterializeFragments(const AMTVScanInput& input, bool coalesce_adjacent);

  const Comparator* ucmp_;
  size_t num_runs_ = 0;
  size_t num_raw_tombstones_ = 0;
  size_t num_boundary_events_ = 0;
  std::vector<AMTVFragment> fragments_;
  int64_t current_idx_ = -1;
};

// Differential Oracle for verifying multi-run scan semantics against Native Ground Truth.
class AMTVScanOracle {
 public:
  // Collect all raw RangeTombstones from Base, Sealed runs, and Open Delta.
  static std::vector<RangeTombstone> CollectAllTombstones(
      const AMTVScanInput& input);

  // Construct native FragmentedRangeTombstoneList as single-source Ground Truth.
  static std::unique_ptr<FragmentedRangeTombstoneList> BuildGroundTruthList(
      const std::vector<RangeTombstone>& tombstones,
      const InternalKeyComparator& icmp);

  // Extract all visible fragments from native FragmentedRangeTombstoneIterator.
  static std::vector<AMTVFragment> ExtractNativeFragments(
      FragmentedRangeTombstoneIterator* iter,
      const Comparator* ucmp = nullptr);

  // Differential verification:
  // 1. Output fragments strictly key-ascending;
  // 2. Any adjacent fragments non-overlapping;
  // 3. Max covering sequence on each discrete key matches Ground Truth;
  // 4. ShouldDelete decision matches Ground Truth;
  // 5. Bidirectional Seek / SeekForPrev / TopNext / TopPrev navigation matches.
  static Status VerifyDifferential(
      const AMTVScanInput& input, const std::vector<std::string>& probe_keys,
      const InternalKeyComparator& icmp);
};

}  // namespace ROCKSDB_NAMESPACE
