/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "core/scan.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace hbase {

Scan::Scan() {}

Scan::~Scan() {}

Scan::Scan(const std::string &start_row) : start_row_(start_row) {
  CheckRow(start_row_);
}

Scan::Scan(const std::string &start_row, const std::string &stop_row)
    : start_row_(start_row), stop_row_(stop_row) {
  CheckRow(start_row_);
  CheckRow(stop_row_);
}

Scan::Scan(const Scan &scan) {
  start_row_ = scan.start_row_;
  stop_row_ = scan.stop_row_;
  max_versions_ = scan.max_versions_;
  caching_ = scan.caching_;
  max_result_size_ = scan.max_result_size_;
  cache_blocks_ = scan.cache_blocks_;
  load_column_families_on_demand_ = scan.load_column_families_on_demand_;
  reversed_ = scan.reversed_;
  small_ = scan.small_;
  allow_partial_results_ = scan.allow_partial_results_;
  consistency_ = scan.consistency_;
  tr_.reset(new TimeRange(scan.tr_->MinTimeStamp(), scan.tr_->MaxTimeStamp()));
  family_map_.insert(scan.family_map_.begin(), scan.family_map_.end());
}

Scan &Scan::operator=(const Scan &scan) {
  start_row_ = scan.start_row_;
  stop_row_ = scan.stop_row_;
  max_versions_ = scan.max_versions_;
  caching_ = scan.caching_;
  max_result_size_ = scan.max_result_size_;
  cache_blocks_ = scan.cache_blocks_;
  load_column_families_on_demand_ = scan.load_column_families_on_demand_;
  reversed_ = scan.reversed_;
  small_ = scan.small_;
  allow_partial_results_ = scan.allow_partial_results_;
  consistency_ = scan.consistency_;
  tr_.reset(new TimeRange(scan.tr_->MinTimeStamp(), scan.tr_->MaxTimeStamp()));
  family_map_.insert(scan.family_map_.begin(), scan.family_map_.end());
  return *this;
}

Scan::Scan(const Get &get) {
  cache_blocks_ = get.CacheBlocks();
  max_versions_ = get.MaxVersions();
  tr_.reset(new TimeRange(get.Timerange().MinTimeStamp(),
                          get.Timerange().MaxTimeStamp()));
  family_map_.insert(get.Family().begin(), get.Family().end());
}

Scan &Scan::AddFamily(const std::string &family) {
  const auto &it = family_map_.find(family);
  /**
   * Check if any qualifiers are already present or not.
   * Remove all existing qualifiers if the given family is already present in
   * the map
   */
  if (family_map_.end() != it) {
    it->second.clear();
  } else {
    family_map_[family];
  }
  return *this;
}

Scan &Scan::AddColumn(const std::string &family, const std::string &qualifier) {
  const auto &it = std::find(family_map_[family].begin(),
                             family_map_[family].end(), qualifier);
  /**
   * Check if any qualifiers are already present or not.
   * Add only if qualifiers for a given family are not present
   */
  if (it == family_map_[family].end()) {
    family_map_[family].push_back(qualifier);
  }
  return *this;
}

void Scan::SetReversed(bool reversed) { reversed_ = reversed; }

bool Scan::IsReversed() const { return reversed_; }

void Scan::SetStartRow(const std::string &start_row) {
  CheckRow(start_row);
  start_row_ = start_row;
}

const std::string &Scan::StartRow() const { return start_row_; }

void Scan::SetStopRow(const std::string &stop_row) {
  CheckRow(stop_row);
  stop_row_ = stop_row;
}

const std::string &Scan::StopRow() const { return stop_row_; }

void Scan::SetSmall(bool small) { small_ = small; }

bool Scan::IsSmall() const { return small_; }

void Scan::SetCaching(int caching) { caching_ = caching; }

int Scan::Caching() const { return caching_; }

Scan &Scan::SetConsistency(const hbase::pb::Consistency consistency) {
  consistency_ = consistency;
  return *this;
}

hbase::pb::Consistency Scan::Consistency() const { return consistency_; }

void Scan::SetCacheBlocks(bool cache_blocks) { cache_blocks_ = cache_blocks; }

bool Scan::CacheBlocks() const { return cache_blocks_; }

void Scan::SetAllowPartialResults(bool allow_partial_results) {
  allow_partial_results_ = allow_partial_results;
}

bool Scan::AllowPartialResults() const { return allow_partial_results_; }

void Scan::SetLoadColumnFamiliesOnDemand(bool load_column_families_on_demand) {
  load_column_families_on_demand_ = load_column_families_on_demand;
}

bool Scan::LoadColumnFamiliesOnDemand() const {
  return load_column_families_on_demand_;
}

Scan &Scan::SetMaxVersions(uint32_t max_versions) {
  max_versions_ = max_versions;
  return *this;
}

int Scan::MaxVersions() const { return max_versions_; }

void Scan::SetMaxResultSize(int64_t max_result_size) {
  max_result_size_ = max_result_size;
}

int64_t Scan::MaxResultSize() const { return max_result_size_; }

Scan &Scan::SetTimeRange(int64_t min_stamp, int64_t max_stamp) {
  tr_.reset(new TimeRange(min_stamp, max_stamp));
  return *this;
}

Scan &Scan::SetTimeStamp(int64_t timestamp) {
  tr_.reset(new TimeRange(timestamp, timestamp + 1));
  return *this;
}

const TimeRange &Scan::Timerange() const { return *tr_; }

void Scan::CheckRow(const std::string &row) {
  const int32_t kMaxRowLength = std::numeric_limits<int16_t>::max();
  int row_length = row.size();
  if (0 == row_length) {
    throw std::runtime_error("Row length can't be 0");
  }
  if (row_length > kMaxRowLength) {
    throw std::runtime_error("Length of " + row +
                             " is greater than max row size: " +
                             std::to_string(kMaxRowLength));
  }
}

bool Scan::HasFamilies() const { return !family_map_.empty(); }

const FamilyMap &Scan::Family() const { return family_map_; }
}  // namespace hbase
