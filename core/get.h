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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "core/time_range.h"
#include "if/Client.pb.h"

namespace hbase {

/**
 * @brief Map consisting of column families and qualifiers to be used for Get
 * operation
 */
using FamilyMap = std::map<std::string, std::vector<std::string>>;

class Get {
 public:
  /**
   * Constructors
   */
  explicit Get(const std::string& row);
  Get(const Get& cget);
  Get& operator=(const Get& cget);

  ~Get();

  /**
   * @brief Returns the maximum number of values to fetch per CF
   */
  int MaxVersions() const;

  /**
   * @brief Get up to the specified number of versions of each column. default
   * is 1.
   * @param max_versions max_versons to set
   */
  Get& SetMaxVersions(int32_t max_versions = 1);

  /**
   * @brief Returns whether blocks should be cached for this Get operation.
   */
  bool CacheBlocks() const;

  /**
   * @brief Set whether blocks should be cached for this Get operation.
   * @param cache_blocks to set
   */
  Get& SetCacheBlocks(bool cache_blocks);

  /**
   * @brief Returns the Get family map (FamilyMap) for this Get operation. Used
   * for constructing Scan object with an already constructed Get
   */
  const FamilyMap& Family() const;

  /**
   * @brief Returns the timerange for this Get
   */
  const TimeRange& Timerange() const;

  /**
   * @brief Get versions of columns only within the specified timestamp range,
   * [minStamp, maxStamp).
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   */
  Get& SetTimeRange(int64_t min_timestamp, int64_t max_timestamp);

  /**
   * @brief Get versions of columns with the specified timestamp.
   * @param The timestamp to be set
   */
  Get& SetTimeStamp(int64_t timestamp);

  /**
   * @brief Get all columns from the specified family.
   * @param family to be retrieved
   */
  Get& AddFamily(const std::string& family);

  /**
   *  @brief Get the column from the specific family with the specified
   * qualifier.
   *  @param family to be retrieved
   *  @param qualifier to be retrieved
   */
  Get& AddColumn(const std::string& family, const std::string& qualifier);

  /**
   * @brief Returns the row for this Get operation
   */
  const std::string& Row() const;

  /**
   * @brief Returns true if family map (FamilyMap) is non empty false otherwise
   */
  bool HasFamilies() const;

  /**
   * @brief Returns the consistency level for this Get operation
   */
  hbase::pb::Consistency Consistency() const;

  /**
   * @brief Sets the consistency level for this Get operation
   * @param Consistency to be set
   */
  Get& SetConsistency(hbase::pb::Consistency consistency);

 private:
  std::string row_ = "";
  int32_t max_versions_ = 1;
  bool cache_blocks_ = true;
  bool check_existence_only_ = false;
  FamilyMap family_map_;
  hbase::pb::Consistency consistency_ = hbase::pb::Consistency::STRONG;
  std::unique_ptr<TimeRange> tr_ = std::make_unique<TimeRange>();

  /**
   * @brief Checks if the row for this Get operation is proper or not
   * @param row Row to check
   * @throws std::runtime_error if row is empty or greater than
   * MAX_ROW_LENGTH(i.e. std::numeric_limits<short>::max())
   */
  void CheckRow(const std::string& row);
};

}  // namespace hbase
