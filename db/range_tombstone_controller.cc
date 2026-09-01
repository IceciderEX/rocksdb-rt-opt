//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_controller.h"

#include <algorithm>

#include "options/cf_options.h"

namespace ROCKSDB_NAMESPACE {

namespace {

bool AtOrPastLevel0WritePressure(int num_level0_files, int trigger) {
  return trigger > 0 && num_level0_files >= trigger;
}

}  // namespace

RangeTombstoneControllerDecision RangeTombstoneController::Evaluate(
    uint32_t column_family_id, const MutableCFOptions& options,
    const RangeTombstoneControllerSnapshot& snapshot) const {
  if (!options.enable_range_tombstone_controller) {
    return RangeTombstoneControllerDecision::kDisabled;
  }
  if (snapshot.atomic_flush) {
    // Atomic flush has a database-wide request reason. V1 deliberately leaves
    // that path unchanged rather than attributing an atomic multi-CF request
    // to a single range-tombstone controller decision.
    return RangeTombstoneControllerDecision::kAtomicFlush;
  }
  if (snapshot.empty_memtable) {
    return RangeTombstoneControllerDecision::kEmptyMemtable;
  }
  if (snapshot.flush_scheduled) {
    return RangeTombstoneControllerDecision::kFlushAlreadyScheduled;
  }
  if (snapshot.num_range_deletions <
      options.range_tombstone_controller_min_range_deletions) {
    return RangeTombstoneControllerDecision::kBelowRangeDeletionThreshold;
  }
  if (snapshot.memtable_bytes <
      options.range_tombstone_controller_min_memtable_bytes) {
    return RangeTombstoneControllerDecision::kBelowMemtableBytesThreshold;
  }
  if (AtOrPastLevel0WritePressure(snapshot.num_level0_files,
                                  options.level0_slowdown_writes_trigger) ||
      AtOrPastLevel0WritePressure(snapshot.num_level0_files,
                                  options.level0_stop_writes_trigger)) {
    return RangeTombstoneControllerDecision::kLevel0Pressure;
  }
  if (options.soft_pending_compaction_bytes_limit > 0 &&
      snapshot.pending_compaction_bytes >=
          options.soft_pending_compaction_bytes_limit) {
    return RangeTombstoneControllerDecision::kPendingCompactionPressure;
  }

  const auto state_it = states_.find(column_family_id);
  if (state_it != states_.end() &&
      state_it->second.last_flush_request_micros != 0 &&
      snapshot.now_micros >= state_it->second.last_flush_request_micros &&
      snapshot.now_micros - state_it->second.last_flush_request_micros <
          options.range_tombstone_controller_cooldown_micros) {
    return RangeTombstoneControllerDecision::kCooldown;
  }
  if (options.range_tombstone_controller_observe_only) {
    return RangeTombstoneControllerDecision::kObserve;
  }
  return RangeTombstoneControllerDecision::kRequestFlush;
}

bool RangeTombstoneController::RecordObservation(uint32_t column_family_id,
                                                 uint64_t memtable_id) {
  auto& state = states_[column_family_id];
  if (state.last_observed_memtable_id == memtable_id) {
    return false;
  }
  state.last_observed_memtable_id = memtable_id;
  return true;
}

void RangeTombstoneController::RecordFlushRequest(uint32_t column_family_id,
                                                  uint64_t memtable_id,
                                                  uint64_t request_micros) {
  auto& state = states_[column_family_id];
  state.pending_memtable_id = memtable_id;
  state.last_flush_request_micros = request_micros;
}

bool RangeTombstoneController::ConsumeFlushRequest(uint32_t column_family_id,
                                                   uint64_t memtable_id) {
  const auto state_it = states_.find(column_family_id);
  if (state_it == states_.end()) {
    return false;
  }
  auto& state = state_it->second;
  if (state.pending_memtable_id == memtable_id) {
    state.pending_memtable_id = 0;
    return true;
  }
  if (state.pending_memtable_id < memtable_id) {
    state.pending_memtable_id = 0;
  }
  return false;
}

}  // namespace ROCKSDB_NAMESPACE
