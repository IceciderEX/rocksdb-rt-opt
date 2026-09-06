//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"

#include <algorithm>
#include <limits>
#include <map>
#include <time.h>

#include "test_util/sync_point.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

std::atomic<uint64_t> test_open_delta_materialize_count{0};

AMTVRun::AMTVRun(uint64_t id, uint32_t lvl, uint64_t chunk_count, bool partial,
                 std::vector<OpenDeltaEntry> entries,
                 const InternalKeyComparator& icmp)
    : run_id(id),
      level(lvl),
      source_chunk_count(chunk_count),
      is_partial(partial),
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

bool FindMergePair(
    const std::vector<std::shared_ptr<const AMTVRun>>& sealed_runs,
    std::shared_ptr<const AMTVRun>* out_run_a,
    std::shared_ptr<const AMTVRun>* out_run_b) {
  if (sealed_runs.size() < 2) {
    return false;
  }
  uint32_t min_level = std::numeric_limits<uint32_t>::max();
  bool found_level = false;

  // Find lowest level that has at least two non-partial runs with same source_chunk_count
  for (size_t i = 0; i < sealed_runs.size(); ++i) {
    const auto& r1 = sealed_runs[i];
    if (!r1 || r1->is_partial) continue;
    for (size_t j = i + 1; j < sealed_runs.size(); ++j) {
      const auto& r2 = sealed_runs[j];
      if (!r2 || r2->is_partial) continue;
      if (CanMergeRuns(*r1, *r2)) {
        if (!found_level || r1->level < min_level) {
          min_level = r1->level;
          found_level = true;
        }
      }
    }
  }

  if (!found_level) {
    return false;
  }

  std::vector<std::shared_ptr<const AMTVRun>> candidates;
  for (const auto& r : sealed_runs) {
    if (r && !r->is_partial && r->level == min_level) {
      candidates.push_back(r);
    }
  }

  std::sort(candidates.begin(), candidates.end(),
            [](const std::shared_ptr<const AMTVRun>& a,
               const std::shared_ptr<const AMTVRun>& b) {
              return a->run_id < b->run_id;
            });

  for (size_t i = 0; i < candidates.size(); ++i) {
    for (size_t j = i + 1; j < candidates.size(); ++j) {
      if (CanMergeRuns(*candidates[i], *candidates[j])) {
        if (out_run_a) *out_run_a = candidates[i];
        if (out_run_b) *out_run_b = candidates[j];
        return true;
      }
    }
  }

  return false;
}

bool HasMergeablePair(
    const std::vector<std::shared_ptr<const AMTVRun>>& sealed_runs) {
  return FindMergePair(sealed_runs, nullptr, nullptr);
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
      env_(env ? env : Env::Default()),
      task_cond_(&task_mu_) {
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
  CancelAndDrain();
}

void AMTVState::CancelAndDrain() {
  // 1. Prohibit any new task submissions
  is_invalidated_.store(true, std::memory_order_release);
  TEST_SYNC_POINT("AMTVState::CancelAndDrain:AfterInvalidate");

  // 2. If a thread is currently in kSubmitting, wait for it to finish submitting (transitions to kQueued or kIdle)
  {
    MutexLock l(&task_mu_);
    while (task_state_ == MergeTaskState::kSubmitting) {
      task_cond_.Wait();
    }
  }

  // 3. Now that no thread is submitting and no new thread can enter submitting,
  // revoke any queued tasks from BOTTOM and LOW queues in Env.
  Env* env = env_ ? env_ : Env::Default();
  if (env) {
    env->UnSchedule(this, Env::Priority::BOTTOM);
    env->UnSchedule(this, Env::Priority::LOW);
  }

  // 4. Wait until task_state_ is strictly kIdle without holding write_mutex_
  {
    MutexLock l(&task_mu_);
    while (task_state_ != MergeTaskState::kIdle) {
      task_cond_.Wait();
    }
  }
}

bool AMTVState::IsMergeStable() const {
  MutexLock l(&task_mu_);
  if (task_state_ != MergeTaskState::kIdle) {
    return false;
  }
  if (is_invalidated_.load(std::memory_order_relaxed)) {
    return true;
  }
  auto snap = GetSnapshot();
  if (!snap || snap->fallback_required) {
    return true;
  }
  return !HasMergeablePair(snap->sealed_runs);
}

void AMTVState::WaitForMergeStable() {
  MutexLock l(&task_mu_);
  while (true) {
    if (task_state_ == MergeTaskState::kIdle) {
      if (is_invalidated_.load(std::memory_order_relaxed)) {
        break;
      }
      auto snap = GetSnapshot();
      if (!snap || snap->fallback_required ||
          !HasMergeablePair(snap->sealed_runs)) {
        break;
      }
    }
    task_cond_.Wait();
  }
}

MergeTaskState AMTVState::task_state() const {
  MutexLock l(&task_mu_);
  return task_state_;
}

int AMTVState::queued_tasks() const {
  MutexLock l(&task_mu_);
  return task_state_ == MergeTaskState::kQueued ? 1 : 0;
}

int AMTVState::running_tasks() const {
  MutexLock l(&task_mu_);
  return task_state_ == MergeTaskState::kRunning ? 1 : 0;
}

bool AMTVState::is_merge_in_progress() const {
  MutexLock l(&task_mu_);
  return task_state_ != MergeTaskState::kIdle;
}

std::string AMTVState::priority_used() const {
  MutexLock l(&task_mu_);
  return priority_used_;
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
      // P0 constraint 2: projected_sealed_runs > amtv_hard_layer_limit (hard_run_limit)
      // triggers fallback_required. Up to hard_layer_limit_ (e.g. 8) sealed
      // runs are served normally; only when a 9th sealed run would form do we fall back.
      uint32_t projected_sealed_runs = cur_snap->sealed_run_count() + 1;
      if (projected_sealed_runs > hard_layer_limit_) {
        new_snap->fallback_required = true;
        // cur_snap->total_tombstones() + 1 reflects exact total tombstones at fallback entry
        new_snap->tombstones_at_fallback = cur_snap->total_tombstones() + 1;
        tombstones_at_fallback_.store(new_snap->tombstones_at_fallback,
                                      std::memory_order_relaxed);
        runs_at_fallback_.store(cur_snap->sealed_run_count(),
                                std::memory_order_relaxed);
        fallback_required_.store(true, std::memory_order_relaxed);
        fallback_event_count_.fetch_add(1, std::memory_order_relaxed);
        new_snap->open_delta = std::make_shared<OpenDelta>();
      } else {
        auto new_run = std::make_shared<const AMTVRun>(
            next_run_id_++, /*level=*/0, /*chunk_count=*/1, /*is_partial=*/false,
            new_open->entries(), icmp);
        new_snap->sealed_runs.push_back(std::move(new_run));
        new_snap->open_delta = std::make_shared<OpenDelta>();
        uint32_t current_runs = new_snap->sealed_run_count();
        uint32_t prev_peak = peak_sealed_layers_.load(std::memory_order_relaxed);
        while (current_runs > prev_peak &&
               !peak_sealed_layers_.compare_exchange_weak(
                   prev_peak, current_runs, std::memory_order_relaxed)) {
        }
        std::map<uint32_t, uint32_t> current_level_runs;
        for (const auto& r : new_snap->sealed_runs) {
          if (r) current_level_runs[r->level]++;
        }
        for (const auto& p : current_level_runs) {
          peak_run_level_histogram_[p.first] =
              std::max(peak_run_level_histogram_[p.first], p.second);
        }

        uint64_t current_raw_bytes = 0;
        for (const auto& r : new_snap->sealed_runs) {
          if (r) current_raw_bytes += r->raw_entries.size() * sizeof(OpenDeltaEntry);
        }
        uint64_t prev_raw_peak = raw_entries_struct_bytes_peak_.load(std::memory_order_relaxed);
        while (current_raw_bytes > prev_raw_peak &&
               !raw_entries_struct_bytes_peak_.compare_exchange_weak(
                   prev_raw_peak, current_raw_bytes, std::memory_order_relaxed)) {
        }

        if (current_runs >= merge_soft_limit_ &&
            HasMergeablePair(new_snap->sealed_runs)) {
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
      new_snap->tombstones_at_fallback = cur_snap->total_tombstones();
      tombstones_at_fallback_.store(new_snap->tombstones_at_fallback,
                                    std::memory_order_relaxed);
      runs_at_fallback_.store(cur_snap->sealed_run_count(),
                              std::memory_order_relaxed);
      fallback_required_.store(true, std::memory_order_relaxed);
      fallback_event_count_.fetch_add(1, std::memory_order_relaxed);
      new_snap->open_delta = std::make_shared<OpenDelta>();
    } else {
      bool is_partial =
          (cur_snap->open_delta->size() < delta_tombstones_limit_);
      uint64_t chunk_count = is_partial ? 0 : 1;
      auto new_run = std::make_shared<const AMTVRun>(
          next_run_id_++, /*level=*/0, chunk_count, is_partial,
          cur_snap->open_delta->entries(), icmp);
      new_snap->sealed_runs.push_back(std::move(new_run));
      new_snap->open_delta = std::make_shared<OpenDelta>();
      uint32_t current_runs = new_snap->sealed_run_count();
      uint32_t prev_peak = peak_sealed_layers_.load(std::memory_order_relaxed);
      while (current_runs > prev_peak &&
             !peak_sealed_layers_.compare_exchange_weak(
                 prev_peak, current_runs, std::memory_order_relaxed)) {
      }
      std::map<uint32_t, uint32_t> current_level_runs;
      for (const auto& r : new_snap->sealed_runs) {
        if (r) current_level_runs[r->level]++;
      }
      for (const auto& p : current_level_runs) {
        peak_run_level_histogram_[p.first] =
            std::max(peak_run_level_histogram_[p.first], p.second);
      }

      if (!is_partial && current_runs >= merge_soft_limit_ &&
          HasMergeablePair(new_snap->sealed_runs)) {
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
      snap->sealed_runs.size() < merge_soft_limit_ ||
      !HasMergeablePair(snap->sealed_runs)) {
    return;
  }

  Env* env = env_ ? env_ : Env::Default();
  Env::Priority priority = Env::Priority::LOW;
  std::string pri_str;
  if (env->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
    priority = Env::Priority::BOTTOM;
    pri_str = "BOTTOM";
  } else {
    priority = Env::Priority::LOW;
    pri_str = "LOW (Env::Priority::BOTTOM threads == 0)";
  }

  uint64_t sched_time = env->NowNanos();

  // Enter kSubmitting under task_mu_
  {
    MutexLock l(&task_mu_);
    if (is_invalidated_.load(std::memory_order_relaxed) ||
        is_immutable_.load(std::memory_order_relaxed) ||
        fallback_required_.load(std::memory_order_relaxed)) {
      return;
    }
    if (task_state_ != MergeTaskState::kIdle) {
      return;
    }
    task_state_ = MergeTaskState::kSubmitting;
    merge_in_progress_.store(true, std::memory_order_release);
    last_scheduled_priority_ = priority;
    priority_used_ = std::move(pri_str);
    last_scheduled_time_nanos_ = sched_time;
  }

  merge_requested_.fetch_add(1, std::memory_order_relaxed);

  std::shared_ptr<AMTVState> self;
  try {
    self = shared_from_this();
  } catch (const std::bad_weak_ptr&) {
    MutexLock l(&task_mu_);
    task_state_ = MergeTaskState::kIdle;
    merge_in_progress_.store(false, std::memory_order_release);
    task_cond_.SignalAll();
    return;
  }

  auto* arg = new std::shared_ptr<AMTVState>(self);
  TEST_SYNC_POINT("AMTVState::MaybeScheduleMerge:BeforeSchedule");
  env->Schedule(&AMTVState::BGMergeWrapper, arg, priority, this,
                &AMTVState::BGMergeUnschedule);

  // Transition from kSubmitting to kQueued under task_mu_
  {
    MutexLock l(&task_mu_);
    task_state_ = MergeTaskState::kQueued;
    task_cond_.SignalAll();
  }
}

void AMTVState::BGMergeWrapper(void* arg) {
  std::unique_ptr<std::shared_ptr<AMTVState>> holder(
      static_cast<std::shared_ptr<AMTVState>*>(arg));
  std::shared_ptr<AMTVState> state = *holder;
  if (state) {
    state->BGMergeTask();
  }
}

void AMTVState::BGMergeUnschedule(void* arg) {
  std::unique_ptr<std::shared_ptr<AMTVState>> holder(
      static_cast<std::shared_ptr<AMTVState>*>(arg));
  std::shared_ptr<AMTVState> state = *holder;
  if (state) {
    state->OnTaskUnscheduled();
  }
}

void AMTVState::OnTaskUnscheduled() {
  {
    MutexLock l(&task_mu_);
    task_state_ = MergeTaskState::kIdle;
    merge_in_progress_.store(false, std::memory_order_release);
    merge_unscheduled_.fetch_add(1, std::memory_order_relaxed);
    task_cond_.SignalAll();
  }
}

bool AMTVState::TEST_RunMergeSynchronously() {
  auto snap = GetSnapshot();
  if (!snap || snap->fallback_required ||
      snap->sealed_runs.size() < merge_soft_limit_ ||
      !HasMergeablePair(snap->sealed_runs)) {
    return false;
  }
  {
    MutexLock l(&task_mu_);
    if (is_invalidated_.load(std::memory_order_relaxed) ||
        is_immutable_.load(std::memory_order_relaxed) ||
        fallback_required_.load(std::memory_order_relaxed) ||
        task_state_ != MergeTaskState::kIdle) {
      return false;
    }
    task_state_ = MergeTaskState::kRunning;
    merge_in_progress_.store(true, std::memory_order_release);
  }
  merge_requested_.fetch_add(1, std::memory_order_relaxed);
  BGMergeTask();
  return true;
}

void AMTVState::BGMergeTask() {
  Env* env = env_ ? env_ : Env::Default();
  uint64_t start_wall_time = env->NowNanos();

  struct timespec start_cpu_ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_cpu_ts);

  {
    MutexLock l(&task_mu_);
    task_state_ = MergeTaskState::kRunning;
    if (last_scheduled_time_nanos_ > 0 &&
        start_wall_time >= last_scheduled_time_nanos_) {
      task_queue_wait_time_nanos_.fetch_add(
          start_wall_time - last_scheduled_time_nanos_,
          std::memory_order_relaxed);
    }
  }

  auto cleanup_running = [&]() {
    {
      MutexLock l(&task_mu_);
      task_state_ = MergeTaskState::kIdle;
      merge_in_progress_.store(false, std::memory_order_release);
      task_cond_.SignalAll();
    }
    TEST_SYNC_POINT("AMTVState::BGMerge:TaskEnd");
    // P0-2: Unconditionally try to schedule the next merge before exiting
    MaybeScheduleMerge();
  };

  // 1. Initial snapshot check
  auto snap = GetSnapshot();
  if (!snap || snap->fallback_required ||
      is_immutable_.load(std::memory_order_relaxed) ||
      is_invalidated_.load(std::memory_order_relaxed) ||
      snap->sealed_runs.size() < merge_soft_limit_) {
    merge_discarded_.fetch_add(1, std::memory_order_relaxed);
    cleanup_running();
    return;
  }

  // 2. Select lowest level pair of same-level non-partial runs
  std::shared_ptr<const AMTVRun> run_a, run_b;
  if (!FindMergePair(snap->sealed_runs, &run_a, &run_b)) {
    merge_discarded_.fetch_add(1, std::memory_order_relaxed);
    cleanup_running();
    return;
  }

  uint64_t run_a_id = run_a->run_id;
  uint64_t run_b_id = run_b->run_id;
  uint32_t input_level = run_a->level;
  uint64_t input_chunk_count = run_a->source_chunk_count;
  uint32_t new_level = input_level + 1;
  uint64_t new_chunk_count =
      run_a->source_chunk_count + run_b->source_chunk_count;
  uint64_t total_input_tombstones =
      run_a->raw_entries.size() + run_b->raw_entries.size();

  uint64_t in_flight_bytes = total_input_tombstones * sizeof(OpenDeltaEntry);
  uint64_t prev_in_flight =
      in_flight_merge_struct_bytes_peak_.load(std::memory_order_relaxed);
  while (in_flight_bytes > prev_in_flight &&
         !in_flight_merge_struct_bytes_peak_.compare_exchange_weak(
             prev_in_flight, in_flight_bytes, std::memory_order_relaxed)) {
  }

  TEST_SYNC_POINT("AMTVState::BGMerge:BeforeMerge");

  // 3. Rebuild merged run outside of write_mutex_ from raw_entries
  std::vector<OpenDeltaEntry> merged_entries;
  merged_entries.reserve(total_input_tombstones);
  // Deterministic order: smaller run_id first
  merged_entries.insert(merged_entries.end(), run_a->raw_entries.begin(),
                        run_a->raw_entries.end());
  merged_entries.insert(merged_entries.end(), run_b->raw_entries.begin(),
                        run_b->raw_entries.end());

  const InternalKeyComparator* icmp_to_use = icmp_;
  assert(icmp_to_use != nullptr);

  uint64_t merged_run_id = next_run_id_.fetch_add(1, std::memory_order_relaxed);
  auto merged_run = std::make_shared<const AMTVRun>(
      merged_run_id, new_level, new_chunk_count, /*is_partial=*/false,
      std::move(merged_entries), *icmp_to_use);

  TEST_SYNC_POINT("AMTVState::BGMerge:AfterMergeBeforePublish");

  // 4. Critical Section: check conditions and publish
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

    // Check both run_a_id and run_b_id still exist in cur_snap->sealed_runs
    // with same level & chunk_count
    if (can_publish) {
      bool found_a = false, found_b = false;
      for (const auto& r : cur_snap->sealed_runs) {
        if (r && r->run_id == run_a_id && r->level == input_level &&
            r->source_chunk_count == input_chunk_count && !r->is_partial) {
          found_a = true;
        }
        if (r && r->run_id == run_b_id && r->level == input_level &&
            r->source_chunk_count == input_chunk_count && !r->is_partial) {
          found_b = true;
        }
      }
      if (!found_a || !found_b) {
        can_publish = false;
      }
    }

    if (can_publish) {
      auto new_snap = std::make_shared<AMTVSnapshot>();
      new_snap->memtable_generation = cur_snap->memtable_generation;
      new_snap->publish_epoch = cur_snap->publish_epoch + 1;
      new_snap->fallback_required = false;
      new_snap->tombstones_at_fallback = 0;
      new_snap->open_delta = cur_snap->open_delta;

      new_snap->sealed_runs.reserve(cur_snap->sealed_runs.size() - 1);
      bool replaced_first = false;
      for (const auto& r : cur_snap->sealed_runs) {
        if (r && (r->run_id == run_a_id || r->run_id == run_b_id)) {
          if (!replaced_first) {
            new_snap->sealed_runs.push_back(merged_run);
            replaced_first = true;
          }
        } else {
          new_snap->sealed_runs.push_back(r);
        }
      }

      std::map<uint32_t, uint32_t> current_level_runs;
      for (const auto& r : new_snap->sealed_runs) {
        if (r) current_level_runs[r->level]++;
      }
      for (const auto& p : current_level_runs) {
        peak_run_level_histogram_[p.first] =
            std::max(peak_run_level_histogram_[p.first], p.second);
      }

      TEST_SYNC_POINT("AMTVState::BGMerge:BeforeAtomicPublish");
      AtomicSharedPtrStore(&snapshot_,
                           std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                           std::memory_order_release);

      merge_completed_.fetch_add(1, std::memory_order_relaxed);
      merge_input_run_count_.fetch_add(2, std::memory_order_relaxed);
      merge_input_tombstones_.fetch_add(total_input_tombstones,
                                        std::memory_order_relaxed);
      merge_count_per_level_[input_level]++;
      merge_input_tombstones_per_level_[input_level] += total_input_tombstones;

      uint64_t elapsed_wall = env->NowNanos() - start_wall_time;
      merge_wall_time_nanos_.fetch_add(elapsed_wall, std::memory_order_relaxed);

      struct timespec end_cpu_ts;
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_cpu_ts);
      uint64_t elapsed_cpu =
          (end_cpu_ts.tv_sec - start_cpu_ts.tv_sec) * 1000000000ULL +
          (end_cpu_ts.tv_nsec - start_cpu_ts.tv_nsec);
      merge_cpu_time_nanos_.fetch_add(elapsed_cpu, std::memory_order_relaxed);
    } else {
      merge_discarded_.fetch_add(1, std::memory_order_relaxed);
    }
    TEST_SYNC_POINT("AMTVState::BGMerge:AfterPublish");
  }

  cleanup_running();
}

std::string AMTVState::GetAuditSummary(uint64_t original_tombstones) const {
  std::map<uint32_t, uint32_t> curr_level_hist;
  auto snap = GetSnapshot();
  uint64_t current_raw_entries = 0;
  if (snap) {
    for (const auto& r : snap->sealed_runs) {
      if (r) {
        curr_level_hist[r->level]++;
        current_raw_entries += r->raw_entries.size();
      }
    }
    if (snap->open_delta) {
      current_raw_entries += snap->open_delta->size();
    }
  }

  std::string curr_hist_str = "{";
  bool first = true;
  for (const auto& p : curr_level_hist) {
    if (!first) curr_hist_str += ", ";
    curr_hist_str += "L" + std::to_string(p.first) + ": " + std::to_string(p.second);
    first = false;
  }
  curr_hist_str += "}";

  std::string peak_hist_str = "{";
  first = true;
  {
    MutexLock l(&write_mutex_);
    for (const auto& p : peak_run_level_histogram_) {
      if (!first) peak_hist_str += ", ";
      peak_hist_str += "L" + std::to_string(p.first) + ": " + std::to_string(p.second);
      first = false;
    }
  }
  peak_hist_str += "}";

  std::string per_level_merges_str = "{";
  first = true;
  {
    MutexLock l(&write_mutex_);
    for (const auto& p : merge_count_per_level_) {
      if (!first) per_level_merges_str += ", ";
      uint64_t tombstones = 0;
      auto it = merge_input_tombstones_per_level_.find(p.first);
      if (it != merge_input_tombstones_per_level_.end()) {
        tombstones = it->second;
      }
      per_level_merges_str += "L" + std::to_string(p.first) + "->L" +
                              std::to_string(p.first + 1) + ": (" +
                              std::to_string(p.second) + " merges, " +
                              std::to_string(tombstones) + " tombstones)";
      first = false;
    }
  }
  per_level_merges_str += "}";

  double amp_ratio = 0.0;
  if (original_tombstones > 0) {
    amp_ratio =
        static_cast<double>(merge_input_tombstones()) / original_tombstones;
  }

  char buf[2048];
  snprintf(
      buf, sizeof(buf),
      "run_level_histogram: %s\n"
      "peak_run_level_histogram: %s\n"
      "sealed_run_peak (hard_run_limit: %u): %u\n"
      "per_level_merges: %s\n"
      "merge_requested: %llu\n"
      "merge_completed: %llu\n"
      "merge_discarded: %llu\n"
      "merge_unscheduled: %llu\n"
      "merge_input_runs: %llu\n"
      "merge_input_tombstones: %llu\n"
      "AMTV Run reconstruction input amplification (merge_input_tombstones / original_tombstones): %.2fx\n"
      "merge_wall_time_us: %llu\n"
      "merge_cpu_time_us: %llu\n"
      "task_queue_wait_time_us: %llu\n"
      "raw_entries_struct_bytes_peak (proxy only; excludes heap strings, FragmentedRangeTombstoneList, vector capacity, coexisting snapshots): %llu\n"
      "in_flight_merge_struct_bytes_peak (proxy only; excludes heap strings, FragmentedRangeTombstoneList, vector capacity, coexisting snapshots): %llu\n"
      "priority_used: %s\n"
      "fallback_event_count: %llu\n"
      "fallback_get_count: %llu\n"
      "tombstones_at_fallback: %llu\n"
      "runs_at_fallback: %u\n"
      "tombstone_conservation: %llu / %llu (%s)",
      curr_hist_str.c_str(), peak_hist_str.c_str(), hard_run_limit(),
      peak_sealed_runs(), per_level_merges_str.c_str(),
      static_cast<unsigned long long>(merge_requested()),
      static_cast<unsigned long long>(merge_completed()),
      static_cast<unsigned long long>(merge_discarded()),
      static_cast<unsigned long long>(merge_unscheduled()),
      static_cast<unsigned long long>(merge_input_run_count()),
      static_cast<unsigned long long>(merge_input_tombstones()), amp_ratio,
      static_cast<unsigned long long>(merge_wall_time_nanos() / 1000),
      static_cast<unsigned long long>(
          merge_cpu_time_nanos_.load(std::memory_order_relaxed) / 1000),
      static_cast<unsigned long long>(
          task_queue_wait_time_nanos_.load(std::memory_order_relaxed) / 1000),
      static_cast<unsigned long long>(
          raw_entries_struct_bytes_peak_.load(std::memory_order_relaxed)),
      static_cast<unsigned long long>(
          in_flight_merge_struct_bytes_peak_.load(std::memory_order_relaxed)),
      priority_used().c_str(),
      static_cast<unsigned long long>(fallback_event_count()),
      static_cast<unsigned long long>(get_fallback_to_native_count()),
      static_cast<unsigned long long>(tombstones_at_fallback()),
      runs_at_fallback(),
      static_cast<unsigned long long>(current_raw_entries),
      static_cast<unsigned long long>(
          original_tombstones > 0 ? original_tombstones : current_raw_entries),
      (original_tombstones == 0 || current_raw_entries == original_tombstones)
          ? "PASSED"
          : "FAILED");
  return std::string(buf);
}

}  // namespace ROCKSDB_NAMESPACE
