//  Copyright (c) 2026-present, Facebook, Inc. and its affiliates. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/amtv.h"
#include "db/read_path_audit.h"

#include <algorithm>
#include <limits>
#include <map>
#include <time.h>

#include "test_util/sync_point.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

std::atomic<uint64_t> test_open_delta_materialize_count{0};
#ifdef ROCKSDB_READ_PATH_AUDIT
thread_local AMTVGetProbeStats tl_amtv_get_probe_stats;
std::atomic<bool> g_amtv_get_probe_stats_enabled{false};
#endif

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
      max_seq(0),
      raw_payload_bytes(0),
      raw_capacity_proxy_bytes(0),
      fragment_payload_bytes(0) {
  if (raw_entries.empty()) {
    return;
  }
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(raw_entries.size());
  values.reserve(raw_entries.size());

  for (const auto& entry : raw_entries) {
    raw_payload_bytes += entry.ikey.size() + entry.end_key.size();
    keys.emplace_back(entry.ikey.Encode().ToString());
    values.emplace_back(entry.end_key);
    if (entry.sequence() < min_seq) {
      min_seq = entry.sequence();
    }
    if (entry.sequence() > max_seq) {
      max_seq = entry.sequence();
    }
  }
  raw_capacity_proxy_bytes =
      raw_entries.capacity() * sizeof(OpenDeltaEntry) + raw_payload_bytes;

  auto iter = std::make_unique<VectorIterator>(std::move(keys),
                                               std::move(values), &icmp);
  fragmented_list =
      std::make_shared<FragmentedRangeTombstoneList>(std::move(iter), icmp);
  if (fragmented_list) {
    fragment_payload_bytes = fragmented_list->total_tombstone_payload_bytes();
  }
  sidecar_index_ = AMTVRunSidecarIndex(raw_entries, icmp);
}

AMTVRunSidecarIndex::AMTVRunSidecarIndex(
    const std::vector<OpenDeltaEntry>& raw_entries,
    const InternalKeyComparator& icmp) {
  const size_t n = raw_entries.size();
  if (n == 0) {
    return;
  }

  const auto* ucmp = icmp.user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);

  sorted_indices_.resize(n);
  for (size_t i = 0; i < n; ++i) {
    sorted_indices_[i] = i;
  }

  std::sort(sorted_indices_.begin(), sorted_indices_.end(),
            [&raw_entries, ucmp, has_ts, &icmp](size_t i, size_t j) {
              const auto& a = raw_entries[i];
              const auto& b = raw_entries[j];
              int c = ucmp->CompareWithoutTimestamp(a.user_start_key(), has_ts,
                                                    b.user_start_key(), has_ts);
              if (c != 0) {
                return c < 0;
              }
              int ic = icmp.Compare(a.ikey.Encode(), b.ikey.Encode());
              if (ic != 0) {
                return ic < 0;
              }
              if (a.sequence() != b.sequence()) {
                return a.sequence() > b.sequence();
              }
              return ucmp->CompareWithoutTimestamp(a.user_end_key(), has_ts,
                                                   b.user_end_key(), has_ts) < 0;
            });

  prefix_max_end_index_.resize(n);
  prefix_max_end_index_[0] = sorted_indices_[0];
  for (size_t i = 1; i < n; ++i) {
    size_t curr_entry = sorted_indices_[i];
    size_t prev_max_entry = prefix_max_end_index_[i - 1];
    int c = ucmp->CompareWithoutTimestamp(
        raw_entries[curr_entry].user_end_key(), has_ts,
        raw_entries[prev_max_entry].user_end_key(), has_ts);
    if (c > 0) {
      prefix_max_end_index_[i] = curr_entry;
    } else {
      prefix_max_end_index_[i] = prev_max_entry;
    }
  }
}

void AMTVRunSidecarIndex::CollectIntersectingIndices(
    const std::vector<OpenDeltaEntry>& raw_entries,
    const Slice* lower_bound, const Slice* upper_bound,
    const InternalKeyComparator& icmp,
    std::vector<size_t>* out_indices,
    AMTVRunIntervalIndexAuditInfo* out_audit) const {
  // Overwrite contract: always clear caller's output container first.
  if (out_indices) {
    out_indices->clear();
  }
  if (out_audit) {
    *out_audit = AMTVRunIntervalIndexAuditInfo();
    out_audit->raw_entries_count = raw_entries.size();
    out_audit->index_bytes = memory_bytes();
  }
  if (raw_entries.empty() || sorted_indices_.empty()) {
    return;
  }

  const auto* ucmp = icmp.user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);

  // Boundary convention: if both L and U are non-null (bounded), and L >= U,
  // the interval [L, U) is empty. An actual empty Slice ("") is a valid user key, NOT an unbounded sentinel.
  if (lower_bound != nullptr && upper_bound != nullptr) {
    if (ucmp->CompareWithoutTimestamp(*lower_bound, false, *upper_bound,
                                      false) >= 0) {
      return;
    }
  }

  // Right bound binary search on sorted_indices_: first entry with start >= U
  size_t right = sorted_indices_.size();
  if (upper_bound != nullptr) {
    auto it_right = std::lower_bound(
        sorted_indices_.begin(), sorted_indices_.end(), *upper_bound,
        [&raw_entries, ucmp, has_ts](size_t idx, const Slice& u) {
          return ucmp->CompareWithoutTimestamp(
                     raw_entries[idx].user_start_key(), has_ts, u, false) < 0;
        });
    right = static_cast<size_t>(std::distance(sorted_indices_.begin(), it_right));
  }

  // Left bound binary search on prefix_max_end_index_: first entry with prefix_max_end > L
  size_t left = 0;
  if (lower_bound != nullptr) {
    auto it_left = std::lower_bound(
        prefix_max_end_index_.begin(), prefix_max_end_index_.end(), *lower_bound,
        [&raw_entries, ucmp, has_ts](size_t entry_idx, const Slice& l) {
          return ucmp->CompareWithoutTimestamp(
                     raw_entries[entry_idx].user_end_key(), has_ts, l, false) <= 0;
        });
    left = static_cast<size_t>(std::distance(prefix_max_end_index_.begin(), it_left));
  }

  if (left > right) {
    left = right;
  }

  if (out_audit) {
    out_audit->left = left;
    out_audit->right = right;
    out_audit->span = (right >= left) ? (right - left) : 0;
  }

  const size_t initial_indices_size = out_indices ? out_indices->size() : 0;

  // Exact filtering on [left, right)
  for (size_t i = left; i < right; ++i) {
    size_t entry_idx = sorted_indices_[i];
    const auto& entry = raw_entries[entry_idx];

    bool match = true;
    if (upper_bound != nullptr) {
      if (ucmp->CompareWithoutTimestamp(entry.user_start_key(), has_ts,
                                         *upper_bound, false) >= 0) {
        match = false;
      }
    }
    if (lower_bound != nullptr) {
      if (ucmp->CompareWithoutTimestamp(entry.user_end_key(), has_ts,
                                         *lower_bound, false) <= 0) {
        match = false;
      }
    }
    if (match) {
      if (out_indices) {
        out_indices->push_back(entry_idx);
      }
      if (out_audit) {
        out_audit->candidate_count++;
      }
    }
  }

  // Debug development assertion (eliminated under -DNDEBUG):
  // single-run candidates added in this query never exceed raw entries size
  assert((out_indices == nullptr || (out_indices->size() - initial_indices_size) <= raw_entries.size()) &&
         (out_audit == nullptr || out_audit->candidate_count <= raw_entries.size()));
}

bool AMTVRunSidecarIndex::VerifyInvariants(
    const std::vector<OpenDeltaEntry>& raw_entries,
    const InternalKeyComparator& icmp,
    std::string* out_error) const {
  const size_t n = raw_entries.size();
  if (sorted_indices_.size() != n || prefix_max_end_index_.size() != n) {
    if (out_error) {
      *out_error = "Array lengths mismatch: raw_entries=" + std::to_string(n) +
                   ", sorted=" + std::to_string(sorted_indices_.size()) +
                   ", prefix_max=" + std::to_string(prefix_max_end_index_.size());
    }
    return false;
  }
  if (n == 0) return true;

  const auto* ucmp = icmp.user_comparator();
  const size_t ts_sz = ucmp->timestamp_size();
  const bool has_ts = (ts_sz > 0);

  // Invariant 1: Valid indices and start key monotonicity in sorted_indices_
  std::vector<bool> seen(n, false);
  for (size_t i = 0; i < n; ++i) {
    size_t idx = sorted_indices_[i];
    if (idx >= n) {
      if (out_error) {
        *out_error = "sorted_indices[" + std::to_string(i) + "] = " +
                     std::to_string(idx) + " >= n (" + std::to_string(n) + ")";
      }
      return false;
    }
    if (seen[idx]) {
      if (out_error) {
        *out_error = "Duplicate index in sorted_indices: " + std::to_string(idx);
      }
      return false;
    }
    seen[idx] = true;

    if (i > 0) {
      size_t prev_idx = sorted_indices_[i - 1];
      int c = ucmp->CompareWithoutTimestamp(
          raw_entries[prev_idx].user_start_key(), has_ts,
          raw_entries[idx].user_start_key(), has_ts);
      if (c > 0) {
        if (out_error) {
          *out_error = "Start key inversion at " + std::to_string(i);
        }
        return false;
      }
    }
  }

  // Invariant 2: Valid indices and end key prefix maximum monotonicity
  for (size_t i = 0; i < n; ++i) {
    size_t max_entry = prefix_max_end_index_[i];
    if (max_entry >= n) {
      if (out_error) {
        *out_error = "prefix_max_end_index[" + std::to_string(i) + "] = " +
                     std::to_string(max_entry) + " >= n";
      }
      return false;
    }
    if (i > 0) {
      size_t prev_max = prefix_max_end_index_[i - 1];
      int c = ucmp->CompareWithoutTimestamp(
          raw_entries[max_entry].user_end_key(), has_ts,
          raw_entries[prev_max].user_end_key(), has_ts);
      if (c < 0) {
        if (out_error) {
          *out_error = "Prefix-max-end non-monotonicity at " + std::to_string(i);
        }
        return false;
      }
    }
    // Also verify that raw_entries[max_entry].end >= all sorted_indices[0..i].end
    size_t curr_entry = sorted_indices_[i];
    int c = ucmp->CompareWithoutTimestamp(
        raw_entries[max_entry].user_end_key(), has_ts,
        raw_entries[curr_entry].user_end_key(), has_ts);
    if (c < 0) {
      if (out_error) {
        *out_error = "prefix_max_end does not cover sorted entry at " + std::to_string(i);
      }
      return false;
    }
  }
  return true;
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

#ifdef ROCKSDB_READ_PATH_AUDIT
  if (g_amtv_get_probe_stats_enabled.load(std::memory_order_relaxed)) {
    size_t n_runs = snapshot_->sealed_runs.size();
    size_t n_open = snapshot_->open_delta ? snapshot_->open_delta->size() : 0;
    tl_amtv_get_probe_stats.get_count++;
    tl_amtv_get_probe_stats.sealed_runs_sum += n_runs;
    tl_amtv_get_probe_stats.open_delta_entries_sum += n_open;
    if (n_runs > tl_amtv_get_probe_stats.sealed_runs_max) {
      tl_amtv_get_probe_stats.sealed_runs_max = static_cast<uint32_t>(n_runs);
    }
    if (n_open > tl_amtv_get_probe_stats.open_delta_entries_max) {
      tl_amtv_get_probe_stats.open_delta_entries_max = static_cast<uint32_t>(n_open);
    }
    if (n_runs < 64) {
      tl_amtv_get_probe_stats.sealed_runs_hist[n_runs]++;
    }
    if (n_open < 1024) {
      tl_amtv_get_probe_stats.open_delta_hist[n_open]++;
    }
  }
#endif

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

std::shared_ptr<const AMTVSnapshot> AMTVState::GetSnapshot() const {
  return AtomicSharedPtrLoad(&snapshot_, std::memory_order_acquire);
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

bool AMTVState::WaitForMergeStable(uint64_t timeout_micros) {
  MutexLock l(&task_mu_);
  uint64_t start_time = env_ ? env_->NowMicros() : 0;
  while (true) {
    if (task_state_ == MergeTaskState::kIdle) {
      if (is_invalidated_.load(std::memory_order_relaxed)) {
        return true;
      }
      auto snap = GetSnapshot();
      if (!snap || snap->fallback_required ||
          !HasMergeablePair(snap->sealed_runs)) {
        return true;
      }
    }
    if (timeout_micros > 0 && env_) {
      uint64_t now = env_->NowMicros();
      if (now - start_time >= timeout_micros) {
        return false;
      }
      uint64_t wait_us = std::min<uint64_t>(100000, timeout_micros - (now - start_time));
      task_cond_.TimedWait(now + wait_us);
    } else {
      task_cond_.Wait();
    }
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

void AMTVState::UpdateMemoryProxyPeaks(const AMTVSnapshot* snap) {
  if (!snap) return;
  uint64_t total_payload = 0;
  uint64_t total_capacity = 0;
  uint64_t total_fragment = 0;
  for (const auto& r : snap->sealed_runs) {
    if (r) {
      total_payload += r->raw_payload_bytes;
      total_capacity += r->raw_capacity_proxy_bytes;
      total_fragment += r->fragment_payload_bytes;
    }
  }
  if (snap->open_delta) {
    total_payload += snap->open_delta->raw_payload_bytes();
    total_capacity += snap->open_delta->raw_capacity_proxy_bytes();
  }

  auto update_peak = [](std::atomic<uint64_t>& peak, uint64_t val) {
    uint64_t cur = peak.load(std::memory_order_relaxed);
    while (val > cur && !peak.compare_exchange_weak(cur, val, std::memory_order_relaxed)) {}
  };
  update_peak(raw_entry_payload_bytes_peak_, total_payload);
  update_peak(raw_entry_capacity_proxy_bytes_peak_, total_capacity);
  update_peak(fragment_payload_proxy_bytes_peak_, total_fragment);
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
#ifdef ROCKSDB_READ_PATH_AUDIT
  uint64_t lock_wait_start_ns = 0;
  if (IsReadPathAuditEnabled()) {
    lock_wait_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
  }
#endif
  {
    MutexLock l(&write_mutex_);
#ifdef ROCKSDB_READ_PATH_AUDIT
    if (lock_wait_start_ns > 0) {
      uint64_t lock_acq_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      write_state_lock_wait_nanos_.fetch_add(lock_acq_ns - lock_wait_start_ns,
                                             std::memory_order_relaxed);
    }
#endif
    if (fallback_required_.load(std::memory_order_relaxed)) {
      return;
    }

#ifdef ROCKSDB_READ_PATH_AUDIT
    uint64_t clone_start_ns = 0;
    if (IsReadPathAuditEnabled()) {
      clone_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
    }
#endif

    auto cur_snap = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed);
    auto new_snap = std::make_shared<AMTVSnapshot>(*cur_snap);
    new_snap->publish_epoch++;

    std::shared_ptr<OpenDelta> new_open =
        cur_snap->open_delta ? cur_snap->open_delta->Clone()
                             : std::make_shared<OpenDelta>();

#ifdef ROCKSDB_READ_PATH_AUDIT
    if (clone_start_ns > 0) {
      uint64_t clone_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      write_snapshot_clone_nanos_.fetch_add(clone_end_ns - clone_start_ns,
                                            std::memory_order_relaxed);
    }
    uint64_t append_start_ns = 0;
    if (IsReadPathAuditEnabled()) {
      append_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
    }
#endif

    new_open->AddEntry(start_user_key, end_user_key, seq);

#ifdef ROCKSDB_READ_PATH_AUDIT
    if (append_start_ns > 0) {
      uint64_t append_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      write_append_nanos_.fetch_add(append_end_ns - append_start_ns,
                                    std::memory_order_relaxed);
    }
#endif

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

        AMTVTimelineRecord rec;
        rec.event_type = "FALLBACK";
        rec.open_delta_size = static_cast<uint32_t>(new_open->size());
        rec.sealed_run_count = cur_snap->sealed_run_count();
        rec.hard_run_limit = hard_layer_limit_;
        rec.pre_publish_hist = FormatRunLevelHistogram(cur_snap->sealed_runs);
        rec.post_publish_hist = rec.pre_publish_hist;
        rec.fallback_details = "tombstones=" + std::to_string(new_snap->tombstones_at_fallback) +
                               ",delete_range_idx=" + std::to_string(AMTVTimelineLogger::Get().GetDeleteRanges()) +
                               ",projected_runs=" + std::to_string(projected_sealed_runs) +
                               ",hard_limit=" + std::to_string(hard_layer_limit_);
        AMTVTimelineLogger::Get().LogEvent(rec);
      } else {
#ifdef ROCKSDB_READ_PATH_AUDIT
        uint64_t seal_start_ns = 0;
        if (IsReadPathAuditEnabled()) {
          seal_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count();
        }
#endif
        auto new_run = std::make_shared<const AMTVRun>(
            next_run_id_++, /*level=*/0, /*chunk_count=*/1, /*is_partial=*/false,
            new_open->entries(), icmp);
#ifdef ROCKSDB_READ_PATH_AUDIT
        if (seal_start_ns > 0) {
          uint64_t seal_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count();
          write_seal_build_nanos_.fetch_add(seal_end_ns - seal_start_ns,
                                            std::memory_order_relaxed);
        }
#endif
        uint64_t new_run_id = new_run->run_id;
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

        AMTVTimelineRecord rec;
        rec.event_type = "SEAL";
        rec.open_delta_size = 0;
        rec.sealed_run_count = new_snap->sealed_run_count();
        rec.hard_run_limit = hard_layer_limit_;
        rec.output_run_id = std::to_string(new_run_id);
        rec.output_level = "0";
        rec.output_chunk_count = "1";
        rec.output_tombstone_count = std::to_string(delta_tombstones_limit_);
        rec.pre_publish_hist = FormatRunLevelHistogram(cur_snap->sealed_runs);
        rec.post_publish_hist = FormatRunLevelHistogram(new_snap->sealed_runs);
        AMTVTimelineLogger::Get().LogEvent(rec);

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

    UpdateMemoryProxyPeaks(new_snap.get());
    TEST_SYNC_POINT("AMTVState::AddTombstone:BeforePublish");

#ifdef ROCKSDB_READ_PATH_AUDIT
    uint64_t pub_start_ns = 0;
    if (IsReadPathAuditEnabled()) {
      pub_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
    }
#endif
    AtomicSharedPtrStore(&snapshot_,
                         std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                         std::memory_order_release);
#ifdef ROCKSDB_READ_PATH_AUDIT
    if (pub_start_ns > 0) {
      uint64_t pub_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      write_publish_nanos_.fetch_add(pub_end_ns - pub_start_ns,
                                     std::memory_order_relaxed);
    }
#endif
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

      AMTVTimelineRecord rec;
      rec.event_type = "FALLBACK";
      rec.open_delta_size = static_cast<uint32_t>(cur_snap->open_delta ? cur_snap->open_delta->size() : 0);
      rec.sealed_run_count = cur_snap->sealed_run_count();
      rec.hard_run_limit = hard_layer_limit_;
      rec.pre_publish_hist = FormatRunLevelHistogram(cur_snap->sealed_runs);
      rec.post_publish_hist = rec.pre_publish_hist;
      rec.fallback_details = "FreezeOpenDelta:tombstones=" + std::to_string(new_snap->tombstones_at_fallback);
      AMTVTimelineLogger::Get().LogEvent(rec);
    } else {
      bool is_partial =
          (cur_snap->open_delta->size() < delta_tombstones_limit_);
      uint64_t chunk_count = is_partial ? 0 : 1;
      auto new_run = std::make_shared<const AMTVRun>(
          next_run_id_++, /*level=*/0, chunk_count, is_partial,
          cur_snap->open_delta->entries(), icmp);
      uint64_t new_run_id = new_run->run_id;
      uint64_t t_count = new_run->tombstone_count;
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

      AMTVTimelineRecord rec;
      rec.event_type = "SEAL";
      rec.open_delta_size = 0;
      rec.sealed_run_count = new_snap->sealed_run_count();
      rec.hard_run_limit = hard_layer_limit_;
      rec.output_run_id = std::to_string(new_run_id);
      rec.output_level = "0";
      rec.output_chunk_count = std::to_string(chunk_count);
      rec.output_tombstone_count = std::to_string(t_count);
      rec.pre_publish_hist = FormatRunLevelHistogram(cur_snap->sealed_runs);
      rec.post_publish_hist = FormatRunLevelHistogram(new_snap->sealed_runs);
      AMTVTimelineLogger::Get().LogEvent(rec);

      if (!is_partial && current_runs >= merge_soft_limit_ &&
          HasMergeablePair(new_snap->sealed_runs)) {
        should_schedule_merge = true;
      }
    }

    UpdateMemoryProxyPeaks(new_snap.get());
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
    diagnostic_phase_.store(AMTVDiagnosticPhase::kSubmitting, std::memory_order_relaxed);
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
    diagnostic_phase_.store(AMTVDiagnosticPhase::kIdle, std::memory_order_relaxed);
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
    diagnostic_phase_.store(AMTVDiagnosticPhase::kQueued, std::memory_order_relaxed);
    task_cond_.SignalAll();
  }

  AMTVTimelineRecord rec;
  rec.event_type = "MERGE_SUBMIT";
  rec.sealed_run_count = snap->sealed_run_count();
  rec.hard_run_limit = hard_layer_limit_;
  rec.open_delta_size = static_cast<uint32_t>(snap->open_delta ? snap->open_delta->size() : 0);
  rec.pre_publish_hist = FormatRunLevelHistogram(snap->sealed_runs);
  rec.post_publish_hist = rec.pre_publish_hist;
  AMTVTimelineLogger::Get().LogEvent(rec);
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
    diagnostic_phase_.store(AMTVDiagnosticPhase::kIdle, std::memory_order_relaxed);
    claimed_input_runs_.store(0, std::memory_order_relaxed);
    merge_in_progress_.store(false, std::memory_order_release);
    task_cond_.SignalAll();
  }
  merge_unscheduled_.fetch_add(1, std::memory_order_relaxed);
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
    diagnostic_phase_.store(AMTVDiagnosticPhase::kComputing, std::memory_order_relaxed);
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

  uint64_t queue_wait_us = 0;
  {
    MutexLock l(&task_mu_);
    task_state_ = MergeTaskState::kRunning;
    diagnostic_phase_.store(AMTVDiagnosticPhase::kComputing, std::memory_order_relaxed);
    if (last_scheduled_time_nanos_ > 0 &&
        start_wall_time >= last_scheduled_time_nanos_) {
      uint64_t wait_nanos = start_wall_time - last_scheduled_time_nanos_;
      task_queue_wait_time_nanos_.fetch_add(wait_nanos, std::memory_order_relaxed);
      queue_wait_us = wait_nanos / 1000;
    }
  }

  auto cleanup_running = [&]() {
    {
      MutexLock l(&task_mu_);
      task_state_ = MergeTaskState::kIdle;
      diagnostic_phase_.store(AMTVDiagnosticPhase::kIdle, std::memory_order_relaxed);
      claimed_input_runs_.store(0, std::memory_order_relaxed);
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
    AMTVTimelineRecord rec;
    rec.event_type = "MERGE_DISCARD";
    rec.merge_queue_wait_us = queue_wait_us;
    rec.sealed_run_count = snap ? snap->sealed_run_count() : 0;
    rec.hard_run_limit = hard_layer_limit_;
    rec.pre_publish_hist = snap ? FormatRunLevelHistogram(snap->sealed_runs) : "";
    rec.post_publish_hist = rec.pre_publish_hist;
    AMTVTimelineLogger::Get().LogEvent(rec);
    cleanup_running();
    return;
  }

  // 2. Select lowest level pair of same-level non-partial runs
  std::shared_ptr<const AMTVRun> run_a, run_b;
  if (!FindMergePair(snap->sealed_runs, &run_a, &run_b)) {
    merge_discarded_.fetch_add(1, std::memory_order_relaxed);
    AMTVTimelineRecord rec;
    rec.event_type = "MERGE_DISCARD";
    rec.merge_queue_wait_us = queue_wait_us;
    rec.sealed_run_count = snap ? snap->sealed_run_count() : 0;
    rec.hard_run_limit = hard_layer_limit_;
    rec.pre_publish_hist = snap ? FormatRunLevelHistogram(snap->sealed_runs) : "";
    rec.post_publish_hist = rec.pre_publish_hist;
    AMTVTimelineLogger::Get().LogEvent(rec);
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

  claimed_input_runs_.store(2, std::memory_order_relaxed);
  uint64_t in_flight_payload = run_a->raw_payload_bytes + run_b->raw_payload_bytes;
  uint64_t prev_inflight_peak = inflight_payload_proxy_bytes_peak_.load(std::memory_order_relaxed);
  while (in_flight_payload > prev_inflight_peak &&
         !inflight_payload_proxy_bytes_peak_.compare_exchange_weak(
             prev_inflight_peak, in_flight_payload, std::memory_order_relaxed)) {}

  {
    AMTVTimelineRecord rec;
    rec.event_type = "MERGE_START";
    rec.merge_queue_wait_us = queue_wait_us;
    rec.sealed_run_count = snap->sealed_run_count();
    rec.hard_run_limit = hard_layer_limit_;
    rec.input_run_ids = "[" + std::to_string(run_a_id) + "," + std::to_string(run_b_id) + "]";
    rec.input_levels = "[" + std::to_string(input_level) + "," + std::to_string(input_level) + "]";
    rec.input_chunks = "[" + std::to_string(input_chunk_count) + "," + std::to_string(input_chunk_count) + "]";
    rec.input_tombstones = "[" + std::to_string(run_a->raw_entries.size()) + "," + std::to_string(run_b->raw_entries.size()) + "]";
    rec.pre_publish_hist = FormatRunLevelHistogram(snap->sealed_runs);
    rec.post_publish_hist = rec.pre_publish_hist;
    AMTVTimelineLogger::Get().LogEvent(rec);
  }

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

  uint64_t compute_wall_nanos = env->NowNanos() - start_wall_time;
  struct timespec mid_cpu_ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mid_cpu_ts);
  uint64_t compute_cpu_nanos =
      (mid_cpu_ts.tv_sec - start_cpu_ts.tv_sec) * 1000000000ULL +
      (mid_cpu_ts.tv_nsec - start_cpu_ts.tv_nsec);

  merge_computed_.fetch_add(1, std::memory_order_relaxed);
  total_computed_merge_wall_time_nanos_.fetch_add(compute_wall_nanos, std::memory_order_relaxed);
  total_computed_merge_cpu_time_nanos_.fetch_add(compute_cpu_nanos, std::memory_order_relaxed);

  uint64_t prev_c_wall = max_computed_merge_wall_time_nanos_.load(std::memory_order_relaxed);
  while (compute_wall_nanos > prev_c_wall &&
         !max_computed_merge_wall_time_nanos_.compare_exchange_weak(
             prev_c_wall, compute_wall_nanos, std::memory_order_relaxed)) {}
  if (compute_wall_nanos >= prev_c_wall) {
    max_computed_merge_cpu_time_nanos_.store(compute_cpu_nanos, std::memory_order_relaxed);
    max_computed_merge_level_.store(input_level, std::memory_order_relaxed);
  }

  {
    AMTVTimelineRecord rec;
    rec.event_type = "MERGE_DONE";
    rec.merge_queue_wait_us = queue_wait_us;
    rec.merge_wall_time_us = compute_wall_nanos / 1000;
    rec.merge_cpu_time_us = compute_cpu_nanos / 1000;
    rec.output_run_id = std::to_string(merged_run_id);
    rec.output_level = std::to_string(new_level);
    rec.output_chunk_count = std::to_string(new_chunk_count);
    rec.output_tombstone_count = std::to_string(total_input_tombstones);
    AMTVTimelineLogger::Get().LogEvent(rec);
  }

  TEST_SYNC_POINT("AMTVState::BGMerge:AfterMergeBeforePublish");

  diagnostic_phase_.store(AMTVDiagnosticPhase::kPublishing, std::memory_order_relaxed);

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
      UpdateMemoryProxyPeaks(new_snap.get());
      AtomicSharedPtrStore(&snapshot_,
                           std::shared_ptr<const AMTVSnapshot>(std::move(new_snap)),
                           std::memory_order_release);

      merge_completed_.fetch_add(1, std::memory_order_relaxed);
      merge_published_.fetch_add(1, std::memory_order_relaxed);
      merge_input_run_count_.fetch_add(2, std::memory_order_relaxed);
      merge_input_tombstones_.fetch_add(total_input_tombstones,
                                        std::memory_order_relaxed);
      merge_count_per_level_[input_level]++;
      merge_input_tombstones_per_level_[input_level] += total_input_tombstones;

      uint64_t elapsed_wall = env->NowNanos() - start_wall_time;
      merge_wall_time_nanos_.fetch_add(elapsed_wall, std::memory_order_relaxed);
      total_published_merge_wall_time_nanos_.fetch_add(elapsed_wall, std::memory_order_relaxed);

      struct timespec end_cpu_ts;
      clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_cpu_ts);
      uint64_t elapsed_cpu =
          (end_cpu_ts.tv_sec - start_cpu_ts.tv_sec) * 1000000000ULL +
          (end_cpu_ts.tv_nsec - start_cpu_ts.tv_nsec);
      merge_cpu_time_nanos_.fetch_add(elapsed_cpu, std::memory_order_relaxed);
      total_published_merge_cpu_time_nanos_.fetch_add(elapsed_cpu, std::memory_order_relaxed);

      merge_wall_time_nanos_per_level_[input_level] += elapsed_wall;
      merge_cpu_time_nanos_per_level_[input_level] += elapsed_cpu;
      merge_queue_wait_nanos_per_level_[input_level] += queue_wait_us * 1000ULL;

      uint64_t prev_max_wall = max_single_merge_wall_time_nanos_.load(std::memory_order_relaxed);
      while (elapsed_wall > prev_max_wall &&
             !max_single_merge_wall_time_nanos_.compare_exchange_weak(
                 prev_max_wall, elapsed_wall, std::memory_order_relaxed)) {}
      if (elapsed_wall >= prev_max_wall) {
        max_single_merge_cpu_time_nanos_.store(elapsed_cpu, std::memory_order_relaxed);
        max_single_merge_level_.store(input_level, std::memory_order_relaxed);
      }

      uint64_t prev_pub_wall = max_published_merge_wall_time_nanos_.load(std::memory_order_relaxed);
      while (elapsed_wall > prev_pub_wall &&
             !max_published_merge_wall_time_nanos_.compare_exchange_weak(
                 prev_pub_wall, elapsed_wall, std::memory_order_relaxed)) {}
      if (elapsed_wall >= prev_pub_wall) {
        max_published_merge_cpu_time_nanos_.store(elapsed_cpu, std::memory_order_relaxed);
        max_published_merge_level_.store(input_level, std::memory_order_relaxed);
      }

      AMTVTimelineRecord rec;
      rec.event_type = "MERGE_PUBLISH";
      rec.merge_queue_wait_us = queue_wait_us;
      rec.merge_wall_time_us = elapsed_wall / 1000;
      rec.merge_cpu_time_us = elapsed_cpu / 1000;
      rec.sealed_run_count = AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed)->sealed_run_count();
      rec.hard_run_limit = hard_layer_limit_;
      rec.output_run_id = std::to_string(merged_run_id);
      rec.output_level = std::to_string(new_level);
      rec.output_chunk_count = std::to_string(new_chunk_count);
      rec.output_tombstone_count = std::to_string(total_input_tombstones);
      rec.pre_publish_hist = FormatRunLevelHistogram(cur_snap->sealed_runs);
      rec.post_publish_hist = FormatRunLevelHistogram(AtomicSharedPtrLoad(&snapshot_, std::memory_order_relaxed)->sealed_runs);
      AMTVTimelineLogger::Get().LogEvent(rec);
    } else {
      merge_discarded_.fetch_add(1, std::memory_order_relaxed);
      total_discarded_merge_wall_time_nanos_.fetch_add(compute_wall_nanos, std::memory_order_relaxed);
      total_discarded_merge_cpu_time_nanos_.fetch_add(compute_cpu_nanos, std::memory_order_relaxed);

      AMTVTimelineRecord rec;
      rec.event_type = "MERGE_DISCARD";
      rec.merge_queue_wait_us = queue_wait_us;
      rec.merge_wall_time_us = (env->NowNanos() - start_wall_time) / 1000;
      rec.sealed_run_count = cur_snap ? cur_snap->sealed_run_count() : 0;
      rec.hard_run_limit = hard_layer_limit_;
      rec.pre_publish_hist = cur_snap ? FormatRunLevelHistogram(cur_snap->sealed_runs) : "";
      rec.post_publish_hist = rec.pre_publish_hist;
      AMTVTimelineLogger::Get().LogEvent(rec);
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
