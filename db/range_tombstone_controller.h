//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <unordered_map>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct MutableCFOptions;

// This is intentionally an internal decision record. It keeps the controller
// policy separate from the DBImpl scheduling mechanics so experiments can
// evolve the policy without changing range deletion semantics.
enum class RangeTombstoneControllerDecision : uint8_t {
  kDisabled,
  kAtomicFlush,
  kEmptyMemtable,
  kFlushAlreadyScheduled,
  kBelowRangeDeletionThreshold,
  kBelowMemtableBytesThreshold,
  kLevel0Pressure,
  kPendingCompactionPressure,
  kCooldown,
  kObserve,
  kRequestFlush,
};

struct RangeTombstoneControllerSnapshot {
  uint64_t memtable_id = 0;
  uint64_t num_range_deletions = 0;
  uint64_t memtable_bytes = 0;
  uint64_t pending_compaction_bytes = 0;
  uint64_t now_micros = 0;
  int num_level0_files = 0;
  int level0_slowdown_writes_trigger = 0;
  int level0_stop_writes_trigger = 0;
  bool atomic_flush = false;
  bool empty_memtable = true;
  bool flush_scheduled = false;
};

// All calls are made with DB mutex held. The state is deliberately per column
// family because a flush request and its cooldown must not leak across column
// families or across successive active memtables.
class RangeTombstoneController {
 public:
  RangeTombstoneControllerDecision Evaluate(
      uint32_t column_family_id, const MutableCFOptions& options,
      const RangeTombstoneControllerSnapshot& snapshot) const;

  // Returns true only once per memtable in observe-only mode.
  bool RecordObservation(uint32_t column_family_id, uint64_t memtable_id);

  void RecordFlushRequest(uint32_t column_family_id, uint64_t memtable_id,
                          uint64_t request_micros);

  // Consumes the request only when the scheduled memtable is the same one that
  // the controller evaluated. A stale request can result from a manual flush.
  bool ConsumeFlushRequest(uint32_t column_family_id, uint64_t memtable_id);

 private:
  struct ColumnFamilyState {
    uint64_t last_flush_request_micros = 0;
    uint64_t pending_memtable_id = 0;
    uint64_t last_observed_memtable_id = 0;
  };

  std::unordered_map<uint32_t, ColumnFamilyState> states_;
};

}  // namespace ROCKSDB_NAMESPACE
