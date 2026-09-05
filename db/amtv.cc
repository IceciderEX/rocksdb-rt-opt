//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"

#include <algorithm>

#include "test_util/sync_point.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

std::atomic<uint64_t> test_open_delta_materialize_count{0};

AMTVRun::AMTVRun(uint64_t id, std::vector<OpenDeltaEntry> entries,
                 const InternalKeyComparator& icmp)
    : run_id(id),
      raw_entries(std::move(entries)),
      tombstone_count(raw_entries.size()),
      min_seq(kMaxSequenceNumber),
      max_seq(0) {
  if (raw_entries.empty()) {
    return;
  }
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(raw_entries.size());
  values.reserve(raw_entries.size());

  for (const auto& entry : raw_entries) {
    keys.emplace_back(entry.ikey.Encode().ToString());
    values.emplace_back(entry.end_key);
    if (entry.sequence() < min_seq) {
      min_seq = entry.sequence();
    }
    if (entry.sequence() > max_seq) {
      max_seq = entry.sequence();
    }
  }

  auto iter = std::make_unique<VectorIterator>(std::move(keys),
                                               std::move(values), &icmp);
  fragmented_list =
      std::make_shared<FragmentedRangeTombstoneList>(std::move(iter), icmp);
}

void OpenDelta::AddEntry(const Slice& start_user_key,
                         const Slice& end_user_key, SequenceNumber seq) {
  entries_.emplace_back(start_user_key, end_user_key, seq);
}

SequenceNumber OpenDelta::MaxCoveringTombstoneSeqnum(
    const Slice& user_key, const Comparator* ucmp, SequenceNumber read_seq,
    std::string* out_ts, const Slice* ts_upper_bound) const {
  SequenceNumber max_seq = 0;
  const OpenDeltaEntry* best_entry = nullptr;
  size_t ts_sz = ucmp->timestamp_size();

  for (const auto& entry : entries_) {
    if (entry.sequence() <= read_seq) {
      if (ts_sz > 0 && ts_upper_bound != nullptr && !ts_upper_bound->empty()) {
        Slice entry_ts = entry.timestamp(ts_sz);
        if (ucmp->CompareTimestamp(entry_ts, *ts_upper_bound) > 0) {
          continue;
        }
      }
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
  test_open_delta_materialize_count.fetch_add(1, std::memory_order_relaxed);
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
    const Slice& user_key, SequenceNumber read_seq, std::string* out_ts,
    const Slice* ts_upper_bound) const {
  if (snapshot_ == nullptr) {
    if (out_ts != nullptr) {
      out_ts->clear();
    }
    return 0;
  }
  SequenceNumber max_seq = 0;
  std::string best_ts;
  const auto* ucmp = icmp_->user_comparator();

  // 1. Query Sealed Runs (pre-built FragmentedRangeTombstoneList)
  for (const auto& run : snapshot_->sealed_runs) {
    if (run && run->fragmented_list && !run->fragmented_list->empty()) {
      FragmentedRangeTombstoneIterator run_iter(run->fragmented_list.get(),
                                                *icmp_, read_seq,
                                                ts_upper_bound);
      SequenceNumber s = run_iter.MaxCoveringTombstoneSeqnum(user_key);
      if (s > max_seq) {
        max_seq = s;
        if (ucmp->timestamp_size() > 0) {
          best_ts.assign(run_iter.timestamp().data(),
                         run_iter.timestamp().size());
        }
      }
    }
  }

  // 2. Query Open Delta (direct linear scan on raw entries, ZERO materialization!)
  if (snapshot_->open_delta && !snapshot_->open_delta->empty()) {
    std::string open_ts;
    SequenceNumber s = snapshot_->open_delta->MaxCoveringTombstoneSeqnum(
        user_key, ucmp, read_seq, &open_ts, ts_upper_bound);
    if (s > max_seq) {
      max_seq = s;
      best_ts = std::move(open_ts);
    }
  }

  if (out_ts != nullptr) {
    if (max_seq > 0 && ucmp->timestamp_size() > 0) {
      *out_ts = std::move(best_ts);
    } else {
      out_ts->clear();
    }
  }
  return max_seq;
}

void AMTVMultiSourceAdapter::AddToRangeDelAggregator(
    ReadRangeDelAggregator* agg, SequenceNumber read_seq,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneList>>&
        pinned_open_lists) const {
  assert(agg != nullptr);

  // 1. Add Sealed Runs
  for (const auto& run : snapshot_->sealed_runs) {
    if (run && run->fragmented_list && !run->fragmented_list->empty()) {
      agg->AddTombstones(std::make_unique<FragmentedRangeTombstoneIterator>(
          run->fragmented_list.get(), *icmp_, read_seq));
    }
  }

  // 2. Add Open Delta (via on-the-fly FragmentedRangeTombstoneList for test reconciliation)
  if (snapshot_->open_delta && !snapshot_->open_delta->empty()) {
    auto open_list =
        snapshot_->open_delta->BuildFragmentedRangeTombstoneList(*icmp_);
    if (open_list && !open_list->empty()) {
      auto* raw_ptr = open_list.get();
      pinned_open_lists.push_back(std::move(open_list));
      agg->AddTombstones(std::make_unique<FragmentedRangeTombstoneIterator>(
          raw_ptr, *icmp_, read_seq));
    }
  }
}

// --------------------------------------------------------------------------
// AMTVState
// --------------------------------------------------------------------------

AMTVState::AMTVState(uint64_t memtable_generation,
                     uint32_t delta_tombstones_limit,
                     uint32_t merge_soft_limit,
                     uint32_t hard_layer_limit,
                     const InternalKeyComparator* icmp,
                     Env* env)
    : memtable_generation_(memtable_generation),
      delta_tombstones_limit_(delta_tombstones_limit),
      merge_soft_limit_(merge_soft_limit),
      hard_layer_limit_(hard_layer_limit),
      icmp_(icmp),
      env_(env ? env : Env::Default()) {
  if (icmp_ == nullptr) {
    fallback_icmp_ =
        std::make_unique<InternalKeyComparator>(BytewiseComparator());
    icmp_ = fallback_icmp_.get();
  }
  auto init_snap = std::make_shared<AMTVSnapshot>();
  init_snap->memtable_generation = memtable_generation_;
  init_snap->open_delta = std::make_shared<OpenDelta>();
  AtomicSharedPtrStore(&snapshot_,
                       std::shared_ptr<const AMTVSnapshot>(std::move(init_snap)),
                       std::memory_order_relaxed);
}

AMTVState::~AMTVState() {
  Invalidate();
}

void AMTVState::AddTombstone(const Slice& start_user_key,
                             const Slice& end_user_key, SequenceNumber seq,
                             const InternalKeyComparator& icmp) {
  if (fallback_icmp_ != nullptr) {
    icmp_ = &icmp;
  }
  // P0 constraint 1: If fallback has already been triggered for this memtable,
  // cease all AMTV shadow delta accumulation immediately.
  if (fallback_required_.load(std::memory_order_relaxed)) {
    return;
  }

  bool should_schedule_merge = false;
  {
    MutexLock l(&write_mutex_);
    if (fallback_required_.load(std::memory_order_relaxed)) {
      return;
    }

    auto cur_snap = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
    auto new_snap = std::make_shared<AMTVSnapshot>(*cur_snap);
    new_snap->publish_epoch++;

    std::shared_ptr<OpenDelta> new_open =
        cur_snap->open_delta ? cur_snap->open_delta->Clone()
                             : std::make_shared<OpenDelta>();
    new_open->AddEntry(start_user_key, end_user_key, seq);

    if (new_open->size() >= delta_tombstones_limit_) {
      // P0 constraint 2: projected_sealed_runs > amtv_hard_layer_limit
      // triggers fallback_required. Up to hard_layer_limit_ (e.g. 8) sealed
      // runs are served normally; only when a 9th sealed run would form do we fall back.
      uint32_t projected_sealed_runs = cur_snap->sealed_run_count() + 1;
      if (projected_sealed_runs > hard_layer_limit_) {
        new_snap->fallback_required = true;
        // Fixed in M2a.1: cur_snap->total_tombstones() + 1 exactly reflects total
        // tombstones at fallback entry (current snap total + 1 newly added tombstone).
        new_snap->tombstones_at_fallback = cur_snap->total_tombstones() + 1;
        tombstones_at_fallback_.store(new_snap->tombstones_at_fallback,
                                      std::memory_order_relaxed);
        fallback_required_.store(true, std::memory_order_relaxed);
        fallback_event_count_.fetch_add(1, std::memory_order_relaxed);
        new_snap->open_delta = std::make_shared<OpenDelta>();
      } else {
        auto new_run = std::make_shared<const AMTVRun>(
            next_run_id_++, new_open->entries(), icmp);
        new_snap->sealed_runs.push_back(std::move(new_run));
        new_snap->open_delta = std::make_shared<OpenDelta>();
        uint32_t current_runs = new_snap->sealed_run_count();
        uint32_t prev_peak = peak_sealed_layers_.load(std::memory_order_relaxed);
        while (current_runs > prev_peak &&
               !peak_sealed_layers_.compare_exchange_weak(
                   prev_peak, current_runs, std::memory_order_relaxed)) {
        }
        if (current_runs >= merge_soft_limit_) {
          should_schedule_merge = true;
        }
      }
    } else {
      new_snap->open_delta = std::move(new_open);
    }

    TEST_SYNC_POINT("AMTVState::AddTombstone:BeforePublish");
    AtomicSharedPtrStore(&snapshot_,
                         std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                         std::memory_order_release);
    TEST_SYNC_POINT("AMTVState::AddTombstone:AfterPublish");
  }

  if (should_schedule_merge) {
    MaybeScheduleMerge();
  }
}

void AMTVState::FreezeOpenDelta(const InternalKeyComparator& icmp) {
  if (fallback_icmp_ != nullptr) {
    icmp_ = &icmp;
  }
  if (fallback_required_.load(std::memory_order_relaxed)) {
    return;
  }
  bool should_schedule_merge = false;
  {
    MutexLock l(&write_mutex_);
    if (fallback_required_.load(std::memory_order_relaxed)) {
      return;
    }
    auto cur_snap = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
    if (!cur_snap->open_delta || cur_snap->open_delta->empty()) {
      return;
    }
    auto new_snap = std::make_shared<AMTVSnapshot>(*cur_snap);
    new_snap->publish_epoch++;

    uint32_t projected_sealed_runs = cur_snap->sealed_run_count() + 1;
    if (projected_sealed_runs > hard_layer_limit_) {
      new_snap->fallback_required = true;
      // Fixed in M2a.1: cur_snap->total_tombstones() already includes cur_snap->open_delta->size().
      // Must NOT add open_delta again.
      new_snap->tombstones_at_fallback = cur_snap->total_tombstones();
      tombstones_at_fallback_.store(new_snap->tombstones_at_fallback,
                                    std::memory_order_relaxed);
      fallback_required_.store(true, std::memory_order_relaxed);
      fallback_event_count_.fetch_add(1, std::memory_order_relaxed);
      new_snap->open_delta = std::make_shared<OpenDelta>();
    } else {
      auto new_run = std::make_shared<const AMTVRun>(
          next_run_id_++, cur_snap->open_delta->entries(), icmp);
      new_snap->sealed_runs.push_back(std::move(new_run));
      new_snap->open_delta = std::make_shared<OpenDelta>();
      uint32_t current_runs = new_snap->sealed_run_count();
      uint32_t prev_peak = peak_sealed_layers_.load(std::memory_order_relaxed);
      while (current_runs > prev_peak &&
             !peak_sealed_layers_.compare_exchange_weak(
                 prev_peak, current_runs, std::memory_order_relaxed)) {
      }
      if (current_runs >= merge_soft_limit_) {
        should_schedule_merge = true;
      }
    }

    TEST_SYNC_POINT("AMTVState::FreezeOpenDelta:BeforePublish");
    AtomicSharedPtrStore(&snapshot_,
                         std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                         std::memory_order_release);
    TEST_SYNC_POINT("AMTVState::FreezeOpenDelta:AfterPublish");
  }

  if (should_schedule_merge) {
    MaybeScheduleMerge();
  }
}

void AMTVState::MaybeScheduleMerge() {
  if (is_immutable_.load(std::memory_order_relaxed) ||
      is_invalidated_.load(std::memory_order_relaxed) ||
      fallback_required_.load(std::memory_order_relaxed)) {
    return;
  }
  auto snap = GetSnapshot();
  if (!snap || snap->fallback_required ||
      snap->sealed_runs.size() < merge_soft_limit_) {
    return;
  }

  bool expected = false;
  if (!merge_in_progress_.compare_exchange_strong(expected, true)) {
    return;
  }

  merge_requested_.fetch_add(1, std::memory_order_relaxed);

  std::shared_ptr<AMTVState> self;
  try {
    self = shared_from_this();
  } catch (const std::bad_weak_ptr&) {
    merge_in_progress_.store(false, std::memory_order_release);
    return;
  }

  Env* env = env_ ? env_ : Env::Default();
  auto* arg = new std::shared_ptr<AMTVState>(self);
  TEST_SYNC_POINT("AMTVState::MaybeScheduleMerge:BeforeSchedule");
  env->Schedule(&AMTVState::BGMergeWrapper, arg, Env::Priority::LOW);
}

void AMTVState::BGMergeWrapper(void* arg) {
  std::unique_ptr<std::shared_ptr<AMTVState>> holder(
      static_cast<std::shared_ptr<AMTVState>*>(arg));
  std::shared_ptr<AMTVState> state = *holder;
  if (state) {
    state->BGMergeTask();
  }
}

bool AMTVState::RunMergeSynchronously() {
  bool expected = false;
  if (!merge_in_progress_.compare_exchange_strong(expected, true)) {
    return false;
  }
  merge_requested_.fetch_add(1, std::memory_order_relaxed);
  BGMergeTask();
  return true;
}

void AMTVState::BGMergeTask() {
  uint64_t start_time = (env_ ? env_ : Env::Default())->NowNanos();

  // 1. Initial snapshot check
  auto snap = GetSnapshot();
  if (!snap || snap->fallback_required ||
      is_immutable_.load(std::memory_order_relaxed) ||
      is_invalidated_.load(std::memory_order_relaxed) ||
      snap->sealed_runs.size() < merge_soft_limit_) {
    merge_discarded_.fetch_add(1, std::memory_order_relaxed);
    merge_in_progress_.store(false, std::memory_order_release);
    return;
  }

  // 2. Select oldest 4 runs
  const size_t kMergeInputs = 4;
  std::vector<std::shared_ptr<const AMTVRun>> input_runs;
  std::vector<uint64_t> input_run_ids;
  input_runs.reserve(kMergeInputs);
  input_run_ids.reserve(kMergeInputs);

  for (size_t i = 0; i < kMergeInputs; ++i) {
    input_runs.push_back(snap->sealed_runs[i]);
    input_run_ids.push_back(snap->sealed_runs[i]->run_id);
  }

  uint64_t total_input_tombstones = 0;
  for (const auto& r : input_runs) {
    total_input_tombstones += r->raw_entries.size();
  }

  TEST_SYNC_POINT("AMTVState::BGMerge:BeforeMerge");

  // 3. Rebuild merged run outside of write_mutex_ from raw_entries
  std::vector<OpenDeltaEntry> merged_entries;
  merged_entries.reserve(total_input_tombstones);
  for (const auto& r : input_runs) {
    merged_entries.insert(merged_entries.end(), r->raw_entries.begin(),
                          r->raw_entries.end());
  }

  const InternalKeyComparator* icmp_to_use = icmp_;
  assert(icmp_to_use != nullptr);

  uint64_t merged_run_id = next_run_id_.fetch_add(1, std::memory_order_relaxed);
  auto merged_run = std::make_shared<const AMTVRun>(
      merged_run_id, std::move(merged_entries), *icmp_to_use);

  TEST_SYNC_POINT("AMTVState::BGMerge:AfterMergeBeforePublish");

  // 4. Critical Section: check conditions and publish
  bool reschedule = false;
  {
    MutexLock l(&write_mutex_);

    auto cur_snap = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
    bool can_publish = true;

    // Check generation still valid
    if (!cur_snap || cur_snap->memtable_generation != memtable_generation_) {
      can_publish = false;
    }
    // Check not immutable, invalidated, or in fallback
    if (is_immutable_.load(std::memory_order_relaxed) ||
        is_invalidated_.load(std::memory_order_relaxed) ||
        fallback_required_.load(std::memory_order_relaxed) ||
        (cur_snap && cur_snap->fallback_required)) {
      can_publish = false;
    }

    // Check input run_ids still exist at index 0..3 in cur_snap->sealed_runs
    if (can_publish) {
      if (cur_snap->sealed_runs.size() < kMergeInputs) {
        can_publish = false;
      } else {
        for (size_t i = 0; i < kMergeInputs; ++i) {
          if (cur_snap->sealed_runs[i]->run_id != input_run_ids[i]) {
            can_publish = false;
            break;
          }
        }
      }
    }

    if (can_publish) {
      auto new_snap = std::make_shared<AMTVSnapshot>();
      new_snap->memtable_generation = cur_snap->memtable_generation;
      new_snap->publish_epoch = cur_snap->publish_epoch + 1;
      new_snap->fallback_required = false;
      new_snap->tombstones_at_fallback = 0;

      new_snap->sealed_runs.reserve(cur_snap->sealed_runs.size() - kMergeInputs + 1);
      new_snap->sealed_runs.push_back(merged_run);
      for (size_t i = kMergeInputs; i < cur_snap->sealed_runs.size(); ++i) {
        new_snap->sealed_runs.push_back(cur_snap->sealed_runs[i]);
      }

      new_snap->open_delta = cur_snap->open_delta;

      TEST_SYNC_POINT("AMTVState::BGMerge:BeforeAtomicPublish");
      AtomicSharedPtrStore(&snapshot_,
                           std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                           std::memory_order_release);

      merge_completed_.fetch_add(1, std::memory_order_relaxed);
      merge_input_run_count_.fetch_add(kMergeInputs, std::memory_order_relaxed);
      merge_input_tombstones_.fetch_add(total_input_tombstones,
                                        std::memory_order_relaxed);
      uint64_t elapsed = (env_ ? env_ : Env::Default())->NowNanos() - start_time;
      merge_wall_time_nanos_.fetch_add(elapsed, std::memory_order_relaxed);

      auto published = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
      if (published && published->sealed_runs.size() >= merge_soft_limit_) {
        reschedule = true;
      }
    } else {
      merge_discarded_.fetch_add(1, std::memory_order_relaxed);
      if (cur_snap && !cur_snap->fallback_required &&
          !is_immutable_.load(std::memory_order_relaxed) &&
          !is_invalidated_.load(std::memory_order_relaxed) &&
          cur_snap->sealed_runs.size() >= merge_soft_limit_) {
        reschedule = true;
      }
    }
    TEST_SYNC_POINT("AMTVState::BGMerge:AfterPublish");
  }

  merge_in_progress_.store(false, std::memory_order_release);

  if (reschedule) {
    MaybeScheduleMerge();
  }
}

std::string AMTVState::GetAuditSummary() const {
  char buf[512];
  snprintf(buf, sizeof(buf),
           "sealed_run_peak: %u, merge_requested: %llu, merge_completed: %llu, "
           "merge_discarded: %llu, merge_failed: %llu, "
           "merge_input_runs: %llu, merge_input_tombstones: %llu, "
           "merge_wall_time_us: %llu, fallback_event_count: %llu, "
           "fallback_get_count: %llu, tombstones_at_fallback: %llu",
           peak_sealed_runs(),
           static_cast<unsigned long long>(merge_requested()),
           static_cast<unsigned long long>(merge_completed()),
           static_cast<unsigned long long>(merge_discarded()),
           static_cast<unsigned long long>(merge_failed()),
           static_cast<unsigned long long>(merge_input_run_count()),
           static_cast<unsigned long long>(merge_input_tombstones()),
           static_cast<unsigned long long>(merge_wall_time_nanos() / 1000),
           static_cast<unsigned long long>(fallback_event_count()),
           static_cast<unsigned long long>(get_fallback_to_native_count()),
           static_cast<unsigned long long>(tombstones_at_fallback()));
  return std::string(buf);
}

}  // namespace ROCKSDB_NAMESPACE
