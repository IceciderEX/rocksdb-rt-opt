//  Copyright (c) 2026-present. All rights reserved.
//  Test-only reference implementation and differential Oracle for M1c-P0:
//  AMTV multi-run scan semantics and interface feasibility verification.
//  STRICTLY TEST-ONLY: DO NOT USE IN PRODUCTION SCAN READ PATH.

#include "db/amtv_scan_oracle.h"

#include <algorithm>
#include <sstream>

#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Helper to compare keys respecting timestamp presence.
int CompareKeys(const Comparator* ucmp, const Slice& a, const Slice& b) {
  if (ucmp->timestamp_size() > 0) {
    return ucmp->CompareWithoutTimestamp(a, b);
  }
  return ucmp->Compare(a, b);
}

}  // namespace

AMTVMultiRunScanIterator::AMTVMultiRunScanIterator(const AMTVScanInput& input,
                                                   bool coalesce_adjacent)
    : ucmp_(input.ucmp) {
  assert(ucmp_ != nullptr);
  MaterializeFragments(input, coalesce_adjacent);
}

void AMTVMultiRunScanIterator::MaterializeFragments(const AMTVScanInput& input,
                                                   bool coalesce_adjacent) {
  fragments_.clear();
  current_idx_ = -1;

  num_runs_ = (input.base.empty() ? 0 : 1) + input.sealed_runs.size() +
              (input.open_delta.empty() ? 0 : 1);

  std::vector<RangeTombstone> all_raw = AMTVScanOracle::CollectAllTombstones(input);
  num_raw_tombstones_ = all_raw.size();

  if (all_raw.empty()) {
    num_boundary_events_ = 0;
    return;
  }

  // 1. Filter raw tombstones visible at read_seq and timestamp_upper_bound.
  std::vector<RangeTombstone> visible_tombstones;
  visible_tombstones.reserve(all_raw.size());
  for (const auto& t : all_raw) {
    if (t.seq_ > input.read_seq) {
      continue;
    }
    if (input.timestamp_upper_bound != nullptr && !input.timestamp_upper_bound->empty() &&
        ucmp_->timestamp_size() > 0 && !t.ts_.empty()) {
      if (ucmp_->CompareTimestamp(t.ts_, *input.timestamp_upper_bound) > 0) {
        continue;
      }
    }
    visible_tombstones.push_back(t);
  }

  if (visible_tombstones.empty()) {
    num_boundary_events_ = 0;
    return;
  }

  // 2. Extract and sort all unique boundary event keys from all input tombstones.
  // Native FragmentedRangeTombstoneList fragments over the union of all endpoints
  // present in the MemTable snapshot.
  std::vector<std::string> endpoints;
  endpoints.reserve(all_raw.size() * 2);
  for (const auto& t : all_raw) {
    endpoints.push_back(t.start_key_.ToString());
    endpoints.push_back(t.end_key_.ToString());
  }

  auto endpoint_less = [this](const std::string& a, const std::string& b) {
    return CompareKeys(ucmp_, a, b) < 0;
  };
  auto endpoint_equal = [this](const std::string& a, const std::string& b) {
    return CompareKeys(ucmp_, a, b) == 0;
  };

  std::sort(endpoints.begin(), endpoints.end(), endpoint_less);
  endpoints.erase(std::unique(endpoints.begin(), endpoints.end(), endpoint_equal),
                  endpoints.end());

  num_boundary_events_ = endpoints.size();
  if (endpoints.size() < 2) {
    return;
  }

  // 3. Sweep across elementary intervals [endpoints[i], endpoints[i+1]).
  for (size_t i = 0; i + 1 < endpoints.size(); ++i) {
    const std::string& cur_start = endpoints[i];
    const std::string& cur_end = endpoints[i + 1];

    if (CompareKeys(ucmp_, cur_start, cur_end) == 0) {
      continue;
    }

    // Find all visible tombstones covering this elementary interval.
    SequenceNumber top_seq = 0;
    std::string top_ts;
    bool has_covering = false;

    for (const auto& t : visible_tombstones) {
      // Check if t covers [cur_start, cur_end):
      // t.start_key <= cur_start && cur_end <= t.end_key
      if (CompareKeys(ucmp_, t.start_key_, cur_start) <= 0 &&
          CompareKeys(ucmp_, cur_end, t.end_key_) <= 0) {
        if (!has_covering) {
          has_covering = true;
          top_seq = t.seq_;
          top_ts = t.ts_.ToString();
        } else {
          // Resolve top sequence and timestamp.
          if (t.seq_ > top_seq) {
            top_seq = t.seq_;
            top_ts = t.ts_.ToString();
          } else if (t.seq_ == top_seq && ucmp_->timestamp_size() > 0 && !t.ts_.empty()) {
            if (top_ts.empty() || ucmp_->CompareTimestamp(t.ts_, top_ts) > 0) {
              top_ts = t.ts_.ToString();
            }
          }
        }
      }
    }

    if (!has_covering) {
      // Gap: no visible tombstone covers this interval at this read_seq.
      continue;
    }

    // Format start and end key with max timestamp if user timestamp is enabled.
    std::string frag_start = cur_start;
    std::string frag_end = cur_end;
    if (ucmp_->timestamp_size() > 0) {
      frag_start.clear();
      frag_end.clear();
      AppendUserKeyWithMaxTimestamp(&frag_start, cur_start, ucmp_->timestamp_size());
      AppendUserKeyWithMaxTimestamp(&frag_end, cur_end, ucmp_->timestamp_size());
    }

    // Emit fragment.
    if (coalesce_adjacent && !fragments_.empty()) {
      AMTVFragment& prev = fragments_.back();
      if (CompareKeys(ucmp_, prev.end_key, frag_start) == 0 &&
          prev.seq == top_seq && prev.timestamp == top_ts) {
        // Coalesce contiguous interval with identical covering sequence.
        prev.end_key = frag_end;
        continue;
      }
    }

    fragments_.emplace_back(frag_start, frag_end, top_seq, top_ts);
  }
}

void AMTVMultiRunScanIterator::SeekToTopFirst() {
  current_idx_ = fragments_.empty() ? -1 : 0;
}

void AMTVMultiRunScanIterator::SeekToTopLast() {
  current_idx_ = fragments_.empty() ? -1 : static_cast<int64_t>(fragments_.size()) - 1;
}

void AMTVMultiRunScanIterator::Seek(const Slice& target) {
  if (fragments_.empty()) {
    current_idx_ = -1;
    return;
  }
  // Seeks to the range tombstone that covers target. If no such tombstone exists,
  // seek to the earliest tombstone that ends after target.
  // This corresponds to the first fragment where end_key > target.
  int64_t left = 0;
  int64_t right = static_cast<int64_t>(fragments_.size()) - 1;
  int64_t ans = -1;

  while (left <= right) {
    int64_t mid = left + (right - left) / 2;
    if (CompareKeys(ucmp_, fragments_[mid].end_key, target) > 0) {
      ans = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  current_idx_ = ans;
}

void AMTVMultiRunScanIterator::SeekForPrev(const Slice& target) {
  if (fragments_.empty()) {
    current_idx_ = -1;
    return;
  }
  // Seeks to the range tombstone that covers target. If no such tombstone exists,
  // seek to the latest tombstone that starts before target.
  // This corresponds to the last fragment where start_key <= target.
  int64_t left = 0;
  int64_t right = static_cast<int64_t>(fragments_.size()) - 1;
  int64_t ans = -1;

  while (left <= right) {
    int64_t mid = left + (right - left) / 2;
    if (CompareKeys(ucmp_, fragments_[mid].start_key, target) <= 0) {
      ans = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  current_idx_ = ans;
}

void AMTVMultiRunScanIterator::TopNext() {
  if (!Valid()) {
    return;
  }
  ++current_idx_;
  if (current_idx_ >= static_cast<int64_t>(fragments_.size())) {
    current_idx_ = -1;
  }
}

void AMTVMultiRunScanIterator::TopPrev() {
  if (!Valid()) {
    return;
  }
  --current_idx_;
  if (current_idx_ < 0) {
    current_idx_ = -1;
  }
}

bool AMTVMultiRunScanIterator::Valid() const {
  return current_idx_ >= 0 && current_idx_ < static_cast<int64_t>(fragments_.size());
}

Slice AMTVMultiRunScanIterator::start_key() const {
  assert(Valid());
  return fragments_[current_idx_].start_key;
}

Slice AMTVMultiRunScanIterator::end_key() const {
  assert(Valid());
  return fragments_[current_idx_].end_key;
}

SequenceNumber AMTVMultiRunScanIterator::seq() const {
  assert(Valid());
  return fragments_[current_idx_].seq;
}

Slice AMTVMultiRunScanIterator::timestamp() const {
  assert(Valid());
  return fragments_[current_idx_].timestamp;
}

SequenceNumber AMTVMultiRunScanIterator::MaxCoveringTombstoneSeqnum(
    const Slice& user_key) const {
  if (fragments_.empty()) {
    return 0;
  }
  // Binary search for fragment covering user_key:
  // start_key <= user_key < end_key.
  int64_t left = 0;
  int64_t right = static_cast<int64_t>(fragments_.size()) - 1;
  while (left <= right) {
    int64_t mid = left + (right - left) / 2;
    const auto& f = fragments_[mid];
    if (CompareKeys(ucmp_, f.end_key, user_key) <= 0) {
      left = mid + 1;
    } else if (CompareKeys(ucmp_, f.start_key, user_key) > 0) {
      right = mid - 1;
    } else {
      // Found covering fragment.
      return f.seq;
    }
  }
  return 0;
}

bool AMTVMultiRunScanIterator::ShouldDelete(
    const ParsedInternalKey& parsed) const {
  SequenceNumber covering_seq = MaxCoveringTombstoneSeqnum(parsed.user_key);
  return covering_seq > parsed.sequence;
}

// --------------------------------------------------------------------------
// AMTVScanOracle Static Methods
// --------------------------------------------------------------------------

std::vector<RangeTombstone> AMTVScanOracle::CollectAllTombstones(
    const AMTVScanInput& input) {
  std::vector<RangeTombstone> all;
  for (const auto& t : input.base) {
    all.push_back(t);
  }
  for (const auto& run : input.sealed_runs) {
    for (const auto& t : run) {
      all.push_back(t);
    }
  }
  for (const auto& t : input.open_delta) {
    all.push_back(t);
  }
  return all;
}

std::unique_ptr<FragmentedRangeTombstoneList>
AMTVScanOracle::BuildGroundTruthList(
    const std::vector<RangeTombstone>& tombstones,
    const InternalKeyComparator& icmp) {
  if (tombstones.empty()) {
    return nullptr;
  }
  std::vector<std::string> keys, values;
  keys.reserve(tombstones.size());
  values.reserve(tombstones.size());
  for (const auto& range_del : tombstones) {
    auto key_and_value = range_del.Serialize();
    keys.push_back(key_and_value.first.Encode().ToString());
    values.push_back(key_and_value.second.ToString());
  }
  auto unfrag_iter = std::make_unique<VectorIterator>(
      std::move(keys), std::move(values), &icmp);
  return std::make_unique<FragmentedRangeTombstoneList>(
      std::move(unfrag_iter), icmp);
}

std::vector<AMTVFragment> AMTVScanOracle::ExtractNativeFragments(
    FragmentedRangeTombstoneIterator* iter, const Comparator* ucmp) {
  std::vector<AMTVFragment> result;
  if (!iter) {
    return result;
  }
  iter->SeekToTopFirst();
  while (iter->Valid()) {
    std::string ts = (ucmp && ucmp->timestamp_size() > 0)
                         ? iter->timestamp().ToString()
                         : "";
    result.emplace_back(iter->start_key().ToString(),
                        iter->end_key().ToString(),
                        iter->seq(),
                        std::move(ts));
    iter->TopNext();
  }
  return result;
}

Status AMTVScanOracle::VerifyDifferential(
    const AMTVScanInput& input, const std::vector<std::string>& probe_keys,
    const InternalKeyComparator& icmp) {
  const auto* ucmp = input.ucmp;
  assert(ucmp != nullptr);

  // 1. Build Native Ground Truth.
  std::vector<RangeTombstone> all_tombstones = CollectAllTombstones(input);
  auto gt_list = BuildGroundTruthList(all_tombstones, icmp);
  std::unique_ptr<FragmentedRangeTombstoneIterator> gt_iter;
  if (gt_list) {
    gt_iter = std::make_unique<FragmentedRangeTombstoneIterator>(
        gt_list.get(), icmp, input.read_seq, input.timestamp_upper_bound);
  }
  std::vector<AMTVFragment> gt_fragments =
      ExtractNativeFragments(gt_iter.get(), ucmp);

  // 2. Build AMTV reference iterators: elementary (matching GT 1:1) and coalesced.
  AMTVMultiRunScanIterator amtv_elem(input, /*coalesce_adjacent=*/false);
  AMTVMultiRunScanIterator amtv_coalesced(input, /*coalesce_adjacent=*/true);

  // Property 1: Output fragments strictly key-ascending.
  for (size_t i = 1; i < amtv_elem.fragments().size(); ++i) {
    if (CompareKeys(ucmp, amtv_elem.fragments()[i - 1].start_key,
                    amtv_elem.fragments()[i].start_key) >= 0) {
      return Status::Corruption("AMTV elementary fragments not strictly key-ascending!");
    }
  }
  for (size_t i = 1; i < amtv_coalesced.fragments().size(); ++i) {
    if (CompareKeys(ucmp, amtv_coalesced.fragments()[i - 1].start_key,
                    amtv_coalesced.fragments()[i].start_key) >= 0) {
      return Status::Corruption("AMTV coalesced fragments not strictly key-ascending!");
    }
  }

  // Property 2: Adjacent fragments non-overlapping.
  for (size_t i = 1; i < amtv_elem.fragments().size(); ++i) {
    if (CompareKeys(ucmp, amtv_elem.fragments()[i - 1].end_key,
                    amtv_elem.fragments()[i].start_key) > 0) {
      return Status::Corruption("AMTV elementary fragments overlap!");
    }
  }
  for (size_t i = 1; i < amtv_coalesced.fragments().size(); ++i) {
    if (CompareKeys(ucmp, amtv_coalesced.fragments()[i - 1].end_key,
                    amtv_coalesced.fragments()[i].start_key) > 0) {
      return Status::Corruption("AMTV coalesced fragments overlap!");
    }
  }

  // Elementary fragments must match Ground Truth 1:1.
  if (amtv_elem.fragments().size() != gt_fragments.size()) {
    std::ostringstream oss;
    oss << "Fragment count mismatch! AMTV: " << amtv_elem.fragments().size()
        << ", GT: " << gt_fragments.size();
    return Status::Corruption(oss.str());
  }
  for (size_t i = 0; i < gt_fragments.size(); ++i) {
    if (amtv_elem.fragments()[i] != gt_fragments[i]) {
      std::ostringstream oss;
      oss << "Fragment mismatch at index " << i
          << "! AMTV: [" << amtv_elem.fragments()[i].start_key << ", "
          << amtv_elem.fragments()[i].end_key << ")@" << amtv_elem.fragments()[i].seq
          << ", GT: [" << gt_fragments[i].start_key << ", "
          << gt_fragments[i].end_key << ")@" << gt_fragments[i].seq;
      return Status::Corruption(oss.str());
    }
  }

  // Property 3: Max covering sequence on discrete keys matches Ground Truth.
  for (const auto& key : probe_keys) {
    SequenceNumber gt_seq = gt_iter ? gt_iter->MaxCoveringTombstoneSeqnum(key) : 0;
    SequenceNumber amtv_elem_seq = amtv_elem.MaxCoveringTombstoneSeqnum(key);
    SequenceNumber amtv_coal_seq = amtv_coalesced.MaxCoveringTombstoneSeqnum(key);

    if (amtv_elem_seq != gt_seq) {
      std::ostringstream oss;
      oss << "Elementary max covering seq mismatch on key '" << key
          << "'! Expected: " << gt_seq << ", Got: " << amtv_elem_seq;
      return Status::Corruption(oss.str());
    }
    if (amtv_coal_seq != gt_seq) {
      std::ostringstream oss;
      oss << "Coalesced max covering seq mismatch on key '" << key
          << "'! Expected: " << gt_seq << ", Got: " << amtv_coal_seq;
      return Status::Corruption(oss.str());
    }

    // Property 4: ShouldDelete matches Ground Truth across sequence numbers.
    std::vector<SequenceNumber> check_seqs = {0, gt_seq / 2, gt_seq, gt_seq + 1,
                                             gt_seq + 100, kMaxSequenceNumber};
    for (SequenceNumber s : check_seqs) {
      ParsedInternalKey pik(key, s, kTypeValue);
      bool gt_del = (gt_seq > s);
      bool amtv_del = amtv_coalesced.ShouldDelete(pik);
      if (amtv_del != gt_del) {
        std::ostringstream oss;
        oss << "ShouldDelete mismatch on key '" << key << "' at seq " << s
            << "! Expected: " << gt_del << ", Got: " << amtv_del;
        return Status::Corruption(oss.str());
      }
    }
  }

  // Property 5: Bidirectional Navigation (Seek, SeekForPrev, TopNext, TopPrev).
  // Forward iteration
  std::vector<AMTVFragment> forward_frags;
  amtv_coalesced.SeekToTopFirst();
  while (amtv_coalesced.Valid()) {
    forward_frags.emplace_back(amtv_coalesced.start_key().ToString(),
                               amtv_coalesced.end_key().ToString(),
                               amtv_coalesced.seq(),
                               amtv_coalesced.timestamp().ToString());
    amtv_coalesced.TopNext();
  }
  if (forward_frags != amtv_coalesced.fragments()) {
    return Status::Corruption("Forward TopNext navigation does not match fragment list!");
  }

  // Reverse iteration
  std::vector<AMTVFragment> reverse_frags;
  amtv_coalesced.SeekToTopLast();
  while (amtv_coalesced.Valid()) {
    reverse_frags.emplace_back(amtv_coalesced.start_key().ToString(),
                               amtv_coalesced.end_key().ToString(),
                               amtv_coalesced.seq(),
                               amtv_coalesced.timestamp().ToString());
    amtv_coalesced.TopPrev();
  }
  std::reverse(reverse_frags.begin(), reverse_frags.end());
  if (reverse_frags != amtv_coalesced.fragments()) {
    return Status::Corruption("Reverse TopPrev navigation does not match fragment list!");
  }

  // Point Seeks
  for (const auto& f : amtv_coalesced.fragments()) {
    amtv_coalesced.Seek(f.start_key);
    if (!amtv_coalesced.Valid() || amtv_coalesced.start_key() != f.start_key) {
      return Status::Corruption("Seek(start_key) failed to land on fragment!");
    }
    amtv_coalesced.SeekForPrev(f.start_key);
    if (!amtv_coalesced.Valid() || amtv_coalesced.start_key() != f.start_key) {
      return Status::Corruption("SeekForPrev(start_key) failed to land on fragment!");
    }
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
