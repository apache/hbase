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

Get::~Get() {

}

Get::Get(const std::string &row)
    : row_(row),
      max_versions_(1),
      cache_blocks_(true),
      store_limit_(-1),
      store_offset_(0),
      check_existence_only_(false),
      consistency_(hbase::pb::Consistency::STRONG),
      tr_(TimeRange()) {
  Get::CheckRow(&row_);
  family_map_.clear();
}

Get::Get(const Get &cget) {
  this->row_ = cget.row_;
  this->max_versions_ = cget.max_versions_;
  this->cache_blocks_ = cget.cache_blocks_;
  this->store_limit_ = cget.store_limit_;
  this->store_offset_ = cget.store_offset_;
  this->check_existence_only_ = cget.check_existence_only_;
  this->consistency_ = cget.consistency_;
  this->tr_ = cget.tr_;
}

Get& Get::operator=(const Get &cget) {
  this->row_ = cget.row_;
  this->max_versions_ = cget.max_versions_;
  this->cache_blocks_ = cget.cache_blocks_;
  this->store_limit_ = cget.store_limit_;
  this->store_offset_ = cget.store_offset_;
  this->check_existence_only_ = cget.check_existence_only_;
  this->consistency_ = cget.consistency_;
  this->tr_ = cget.tr_;
  return *this;
}

Get& Get::AddFamily(const std::string &family) {
  const auto &it = family_map_.find(family);

  /**
   * Check if any qualifiers are already present or not.
   * Remove all existing qualifiers if the given family is already present in the map
   */
  if (family_map_.end() != it) {
    it->second.clear();
  } else {
    family_map_[family];
  }
  return *this;
}

Get& Get::AddColumn(const std::string &family, const std::string &qualifier) {
  const auto &it = std::find(this->family_map_[family].begin(),
                             this->family_map_[family].end(), qualifier);

  /**
   * Check if any qualifiers are already present or not.
   * Add only if qualifiers for a given family are not present
   */
  if (it == this->family_map_[family].end()) {
    this->family_map_[family].push_back(qualifier);
  }
  return *this;
}

const std::string& Get::Row() const {
  return row_;
}

hbase::pb::Consistency Get::Consistency() const {
  return this->consistency_;
}

Get &Get::SetConsistency(hbase::pb::Consistency consistency) {
  this->consistency_ = consistency;
  return *this;
}

bool Get::HasFamilies() {
  return !this->family_map_.empty();
}

const FAMILY_MAP &Get::FamilyMap() const {
  return this->family_map_;
}

int Get::MaxVersions() const {
  return this->max_versions_;
}

Get& Get::SetMaxVersions(uint32_t max_versions) {
  if (0 == max_versions)
    throw std::runtime_error("max_versions must be positive");

  this->max_versions_ = max_versions;
  return *this;
}

bool Get::CacheBlocks() const {
  return this->cache_blocks_;
}

Get & Get::SetCacheBlocks(bool cache_blocks) {
  this->cache_blocks_ = cache_blocks;
  return *this;
}

int Get::MaxResultsPerColumnFamily() const {
  return this->store_limit_;
}

Get& Get::SetMaxResultsPerColumnFamily(int store_limit) {
  this->store_limit_ = store_limit;
  return *this;
}

Get& Get::SetTimeRange(long min_timestamp, long max_timestamp) {
  this->tr_ = TimeRange(min_timestamp, max_timestamp);
  return *this;
}

Get& Get::SetTimeStamp(long timestamp) {
  this->tr_ = TimeRange(timestamp, timestamp + 1);
  return *this;
}

const TimeRange& Get::Timerange() const {
  return this->tr_;
}

void Get::CheckRow(const std::string *row) {
  int MAX_ROW_LENGTH = std::numeric_limits<short>::max();
  int row_length = row->size();
  if (0 == row_length) {
    throw std::runtime_error("Row length can't be 0");
  }
  if (row_length > MAX_ROW_LENGTH) {
    throw std::runtime_error(
        "Length of " + *row + " is greater than max row size: "
            + std::to_string(MAX_ROW_LENGTH));
  }
}
}
