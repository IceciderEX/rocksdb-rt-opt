//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"

#include <algorithm>

#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

void OpenDelta::AddEntry(const Slice& start_user_key,
                         const Slice& end_user_key, SequenceNumber seq) {
  entries_.emplace_back(start_user_key, end_user_key, seq);
}

SequenceNumber OpenDelta::MaxCoveringTombstoneSeqnum(
    const Slice& user_key, const Comparator* ucmp, SequenceNumber read_seq,
    std::string* out_ts) const {
  SequenceNumber max_seq = 0;
  const OpenDeltaEntry* best_entry = nullptr;

  for (const auto& entry : entries_) {
    if (entry.sequence() <= read_seq) {
      if (ucmp->CompareWithoutTimestamp(entry.user_start_key(), user_key) <= 0 &&
          ucmp->CompareWithoutTimestamp(user_key, entry.user_end_key()) < 0) {
        if (entry.sequence() > max_seq) {
          max_seq = entry.sequence();
          best_entry = &entry;
        }
      }
    }
  }

  if (best_entry != nullptr && out_ts != nullptr) {
    size_t ts_sz = ucmp->timestamp_size();
    if (ts_sz > 0) {
      Slice ts = best_entry->timestamp(ts_sz);
      out_ts->assign(ts.data(), ts.size());
    }
  }
  return max_seq;
}

std::unique_ptr<FragmentedRangeTombstoneList>
OpenDelta::BuildFragmentedRangeTombstoneList(
    const InternalKeyComparator& icmp) const {
  if (entries_.empty()) {
    return nullptr;
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(entries_.size());
  values.reserve(entries_.size());

  for (const auto& entry : entries_) {
    keys.emplace_back(entry.ikey.Encode().ToString());
    values.emplace_back(entry.end_key);
  }

  auto iter = std::make_unique<VectorIterator>(std::move(keys),
                                               std::move(values), &icmp);
  return std::make_unique<FragmentedRangeTombstoneList>(std::move(iter), icmp);
}

std::shared_ptr<OpenDelta> OpenDelta::Clone() const {
  return std::make_shared<OpenDelta>(entries_);
}

// --------------------------------------------------------------------------
// AMTVMultiSourceAdapter
// --------------------------------------------------------------------------

SequenceNumber AMTVMultiSourceAdapter::MaxCoveringTombstoneSeqnum(
    const Slice& user_key, SequenceNumber read_seq, std::string* out_ts) const {
  SequenceNumber max_seq = 0;
  std::string best_ts;
  const auto* ucmp = icmp_->user_comparator();

  // 1. Query Base
  if (snapshot_->base && !snapshot_->base->empty()) {
    FragmentedRangeTombstoneIterator base_iter(snapshot_->base.get(), *icmp_,
                                              read_seq);
    SequenceNumber s = base_iter.MaxCoveringTombstoneSeqnum(user_key);
    if (s > max_seq) {
      max_seq = s;
      if (ucmp->timestamp_size() > 0) {
        best_ts.assign(base_iter.timestamp().data(),
                       base_iter.timestamp().size());
      }
    }
  }

  // 2. Query Sealed Delta
  if (snapshot_->sealed_delta && !snapshot_->sealed_delta->empty()) {
    FragmentedRangeTombstoneIterator sealed_iter(snapshot_->sealed_delta.get(),
                                                *icmp_, read_seq);
    SequenceNumber s = sealed_iter.MaxCoveringTombstoneSeqnum(user_key);
    if (s > max_seq) {
      max_seq = s;
      if (ucmp->timestamp_size() > 0) {
        best_ts.assign(sealed_iter.timestamp().data(),
                       sealed_iter.timestamp().size());
      }
    }
  }

  // 3. Query Open Delta
  if (snapshot_->open_delta && !snapshot_->open_delta->empty()) {
    std::string open_ts;
    SequenceNumber s = snapshot_->open_delta->MaxCoveringTombstoneSeqnum(
        user_key, ucmp, read_seq, &open_ts);
    if (s > max_seq) {
      max_seq = s;
      best_ts = std::move(open_ts);
    }
  }

  if (out_ts != nullptr && !best_ts.empty()) {
    *out_ts = std::move(best_ts);
  }
  return max_seq;
}

void AMTVMultiSourceAdapter::AddToRangeDelAggregator(
    ReadRangeDelAggregator* agg, SequenceNumber read_seq,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneList>>*
        pinned_open_lists) const {
  assert(agg != nullptr);

  // 1. Add Base
  if (snapshot_->base && !snapshot_->base->empty()) {
    agg->AddTombstones(std::make_unique<FragmentedRangeTombstoneIterator>(
        snapshot_->base.get(), *icmp_, read_seq));
  }

  // 2. Add Sealed Delta
  if (snapshot_->sealed_delta && !snapshot_->sealed_delta->empty()) {
    agg->AddTombstones(std::make_unique<FragmentedRangeTombstoneIterator>(
        snapshot_->sealed_delta.get(), *icmp_, read_seq));
  }

  // 3. Add Open Delta (via on-the-fly FragmentedRangeTombstoneList)
  if (snapshot_->open_delta && !snapshot_->open_delta->empty()) {
    auto open_list =
        snapshot_->open_delta->BuildFragmentedRangeTombstoneList(*icmp_);
    if (open_list && !open_list->empty()) {
      auto* raw_ptr = open_list.get();
      if (pinned_open_lists != nullptr) {
        pinned_open_lists->push_back(std::move(open_list));
      }
      agg->AddTombstones(std::make_unique<FragmentedRangeTombstoneIterator>(
          raw_ptr, *icmp_, read_seq));
    }
  }
}

// --------------------------------------------------------------------------
// AMTVState
// --------------------------------------------------------------------------

AMTVState::AMTVState(uint64_t memtable_generation,
                     uint32_t delta_tombstones_limit)
    : memtable_generation_(memtable_generation),
      delta_tombstones_limit_(delta_tombstones_limit) {
  auto init_snap = std::make_shared<AMTVSnapshot>();
  init_snap->memtable_generation = memtable_generation_;
  init_snap->open_delta = std::make_shared<OpenDelta>();
  AtomicSharedPtrStore(&snapshot_,
                       std::shared_ptr<const AMTVSnapshot>(std::move(init_snap)),
                       std::memory_order_relaxed);
}

void AMTVState::AddTombstone(const Slice& start_user_key,
                             const Slice& end_user_key, SequenceNumber seq,
                             const InternalKeyComparator& icmp) {
  MutexLock l(&write_mutex_);
  auto cur_snap = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
  auto new_snap = std::make_shared<AMTVSnapshot>(*cur_snap);
  new_snap->publish_epoch++;

  std::shared_ptr<OpenDelta> new_open =
      cur_snap->open_delta ? cur_snap->open_delta->Clone()
                           : std::make_shared<OpenDelta>();
  new_open->AddEntry(start_user_key, end_user_key, seq);

  if (new_open->size() >= delta_tombstones_limit_) {
    // Freeze open delta into sealed delta
    auto sealed = new_open->BuildFragmentedRangeTombstoneList(icmp);
    new_snap->sealed_delta = std::move(sealed);
    new_snap->open_delta = std::make_shared<OpenDelta>();
  } else {
    new_snap->open_delta = std::move(new_open);
  }

  AtomicSharedPtrStore(&snapshot_,
                       std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                       std::memory_order_release);
}

void AMTVState::SetBase(
    std::shared_ptr<FragmentedRangeTombstoneList> base) {
  MutexLock l(&write_mutex_);
  auto cur_snap = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
  auto new_snap = std::make_shared<AMTVSnapshot>(*cur_snap);
  new_snap->publish_epoch++;
  new_snap->base = std::move(base);
  AtomicSharedPtrStore(&snapshot_,
                       std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                       std::memory_order_release);
}

void AMTVState::FreezeOpenDelta(const InternalKeyComparator& icmp) {
  MutexLock l(&write_mutex_);
  auto cur_snap = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
  if (!cur_snap->open_delta || cur_snap->open_delta->empty()) {
    return;
  }
  auto new_snap = std::make_shared<AMTVSnapshot>(*cur_snap);
  new_snap->publish_epoch++;
  auto sealed = cur_snap->open_delta->BuildFragmentedRangeTombstoneList(icmp);
  new_snap->sealed_delta = std::move(sealed);
  new_snap->open_delta = std::make_shared<OpenDelta>();
  AtomicSharedPtrStore(&snapshot_,
                       std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                       std::memory_order_release);
}

}  // namespace ROCKSDB_NAMESPACE
