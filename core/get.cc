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

#include "core/get.h"
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace hbase {

Get::~Get() {}

Get::Get(const std::string &row) : row_(row) { CheckRow(row_); }

Get::Get(const Get &get) {
  row_ = get.row_;
  max_versions_ = get.max_versions_;
  cache_blocks_ = get.cache_blocks_;
  check_existence_only_ = get.check_existence_only_;
  consistency_ = get.consistency_;
  tr_.reset(new TimeRange(get.Timerange().MinTimeStamp(), get.Timerange().MaxTimeStamp()));
  family_map_.insert(get.family_map_.begin(), get.family_map_.end());
}

Get &Get::operator=(const Get &get) {
  row_ = get.row_;
  max_versions_ = get.max_versions_;
  cache_blocks_ = get.cache_blocks_;
  check_existence_only_ = get.check_existence_only_;
  consistency_ = get.consistency_;
  tr_.reset(new TimeRange(get.Timerange().MinTimeStamp(), get.Timerange().MaxTimeStamp()));
  family_map_.insert(get.family_map_.begin(), get.family_map_.end());
  return *this;
}

Get &Get::AddFamily(const std::string &family) {
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

Get &Get::AddColumn(const std::string &family, const std::string &qualifier) {
  const auto &it = std::find(family_map_[family].begin(), family_map_[family].end(), qualifier);

  /**
   * Check if any qualifiers are already present or not.
   * Add only if qualifiers for a given family are not present
   */
  if (it == family_map_[family].end()) {
    family_map_[family].push_back(qualifier);
  }
  return *this;
}

const std::string &Get::Row() const { return row_; }

hbase::pb::Consistency Get::Consistency() const { return consistency_; }

Get &Get::SetConsistency(hbase::pb::Consistency consistency) {
  consistency_ = consistency;
  return *this;
}

bool Get::HasFamilies() const { return !family_map_.empty(); }

const FamilyMap &Get::Family() const { return family_map_; }

int Get::MaxVersions() const { return max_versions_; }

Get &Get::SetMaxVersions(int32_t max_versions) {
  if (0 == max_versions) throw std::runtime_error("max_versions must be positive");

  max_versions_ = max_versions;
  return *this;
}

bool Get::CacheBlocks() const { return cache_blocks_; }

Get &Get::SetCacheBlocks(bool cache_blocks) {
  cache_blocks_ = cache_blocks;
  return *this;
}

Get &Get::SetTimeRange(int64_t min_timestamp, int64_t max_timestamp) {
  tr_.reset(new TimeRange(min_timestamp, max_timestamp));
  return *this;
}

Get &Get::SetTimeStamp(int64_t timestamp) {
  tr_.reset(new TimeRange(timestamp, timestamp + 1));
  return *this;
}

const TimeRange &Get::Timerange() const { return *tr_; }

void Get::CheckRow(const std::string &row) {
  const int kMaxRowLength = std::numeric_limits<int16_t>::max();
  int row_length = row.size();
  if (0 == row_length) {
    throw std::runtime_error("Row length can't be 0");
  }
  if (row_length > kMaxRowLength) {
    throw std::runtime_error("Length of " + row + " is greater than max row size: " +
                             std::to_string(kMaxRowLength));
  }
}
}  // namespace hbase
