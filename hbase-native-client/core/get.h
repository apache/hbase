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

#include <map>
#include <string>
#include <vector>
#include <cstdint>
#include "core/time_range.h"
#include "if/Client.pb.h"

namespace hbase {

/**
 * @brief Map consisting of column families and qualifiers to be used for Get operation
 */
using FAMILY_MAP = std::map<std::string, std::vector<std::string>>;

class Get {

 public:

  /**
   * Constructors
   */
  Get(const std::string &row);
  Get(const Get &cget);
  Get& operator=(const Get &cget);

  ~Get();

  /**
   * @brief Returns the maximum number of values to fetch per CF
   */
  int MaxVersions() const;

  /**
   * @brief Get up to the specified number of versions of each column. default is 1.
   * @param max_versions max_versons to set
   */
  Get& SetMaxVersions(uint32_t max_versions = 1);

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
   * @brief Method for retrieving the get's maximum number of values to return per Column Family
   */
  int MaxResultsPerColumnFamily() const;

  /**
   * @brief Set the maximum number of values to return per row per Column Family
   * @param the store_limit to be set
   */
  Get& SetMaxResultsPerColumnFamily(int store_limit);

  /**
   * @brief Returns the Get family map (FAMILY_MAP) for this Get operation.
   */
  const FAMILY_MAP &FamilyMap() const;

  /**
   * @brief Returns the timerange for this Get
   */
  const TimeRange& Timerange() const;

  /**
   * @brief Get versions of columns only within the specified timestamp range, [minStamp, maxStamp).
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   */
  Get& SetTimeRange(long min_timestamp, long max_timestamp);

  /**
   * @brief Get versions of columns with the specified timestamp.
   * @param The timestamp to be set
   */
  Get& SetTimeStamp(long timestamp);

  /**
   * @brief Get all columns from the specified family.
   * @param family to be retrieved
   */
  Get& AddFamily(const std::string &family);

  /**
   *  @brief Get the column from the specific family with the specified qualifier.
   *  @param family to be retrieved
   *  @param qualifier to be retrieved
   */
  Get& AddColumn(const std::string &family, const std::string &qualifier);

  /**
   * @brief Returns the row for this Get operation
   */
  const std::string& Row() const;

  /**
   * @brief Returns true if family map (FAMILY_MAP) is non empty false otherwise
   */
  bool HasFamilies();

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
  std::string row_;
  uint32_t max_versions_;
  bool cache_blocks_;
  int store_limit_;
  int store_offset_;
  bool check_existence_only_;
  FAMILY_MAP family_map_;
  hbase::pb::Consistency consistency_;
  TimeRange tr_;

  /**
   * @brief Checks if the row for this Get operation is proper or not
   * @param row Row to check
   * @throws std::runtime_error if row is empty or greater than MAX_ROW_LENGTH(i.e. std::numeric_limits<short>::max())
   */
  void CheckRow(const std::string *row);
};
}
