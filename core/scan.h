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
#include <memory>
#include <string>
#include <vector>
#include "core/get.h"
#include "core/time_range.h"
#include "if/Client.pb.h"

namespace hbase {

/**
 * @brief Map consisting of column families and qualifiers to be used for Get
 * operation
 */
using FamilyMap = std::map<std::string, std::vector<std::string>>;

class Scan {
 public:
  /**
   * @brief Constructors. Create a Scan operation across all rows.
   */
  Scan();
  Scan(const Scan &scan);
  Scan &operator=(const Scan &scan);

  ~Scan();

  /**
   * @brief Create a Scan operation starting at the specified row. If the
   * specified row does not exist,
   * the Scanner will start from the next closest row after the specified row.
   * @param start_row - row to start scanner at or after
   */
  Scan(const std::string &start_row);

  /**
   * @brief Create a Scan operation for the range of rows specified.
   * @param start_row - row to start scanner at or after (inclusive).
   * @param stop_row - row to stop scanner before (exclusive).
   */
  Scan(const std::string &start_row, const std::string &stop_row);

  /**
   * @brief Builds a scan object with the same specs as get.
   * @param get - get to model scan after
   */
  Scan(const Get &get);

  /**
   * @brief Get all columns from the specified family.Overrides previous calls
   * to AddColumn for this family.
   * @param family - family name
   */
  Scan &AddFamily(const std::string &family);

  /**
   * @brief Get the column from the specified family with the specified
   * qualifier.Overrides previous calls to AddFamily for this family.
   * @param family - family name.
   * @param qualifier - column qualifier.
   */
  Scan &AddColumn(const std::string &family, const std::string &qualifier);

  /**
   * @brief Set whether this scan is a reversed one. This is false by default
   * which means forward(normal) scan.
   * @param reversed - if true, scan will be backward order
   */
  void SetReversed(bool reversed);

  /**
   * @brief Get whether this scan is a reversed one. Returns  true if backward
   * scan, false if forward(default) scan
   */
  bool IsReversed() const;

  /**
   * @brief Set the start row of the scan.If the specified row does not exist,
   * the Scanner will start from the next closest row after the specified row.
   * @param start_row - row to start scanner at or after
   * @throws std::runtime_error if start_row length is 0 or greater than
   * MAX_ROW_LENGTH
   */
  void SetStartRow(std::string &start_row);

  /**
   * @brief returns start_row of the Scan.
   */
  const std::string &StartRow() const;

  /**
   * @brief Set the stop row of the scan. The scan will include rows that are
   * lexicographically less than the provided stop_row.
   * @param stop_row - row to end at (exclusive)
   * @throws std::runtime_error if stop_row length is 0 or greater than
   * MAX_ROW_LENGTH
   */
  void SetStopRow(std::string &stop_row);

  /**
   * @brief returns stop_row of the Scan.
   */
  const std::string &StopRow() const;

  /**
   * @brief Set whether this scan is a small scan.
   */
  void SetSmall(bool small);

  /**
   * @brief Returns if the scan is a small scan.
   */
  bool IsSmall() const;

  /**
   * @brief Set the number of rows for caching that will be passed to scanners.
   * Higher caching values will enable faster scanners but will use more memory.
   * @param caching - the number of rows for caching.
   */
  void SetCaching(int caching);

  /**
   * @brief caching the number of rows fetched when calling next on a scanner.
   */
  int Caching() const;

  /**
   * @brief Sets the consistency level for this operation.
   * @param consistency - the consistency level
   */
  Scan &SetConsistency(const hbase::pb::Consistency consistency);

  /**
   * @brief Returns the consistency level for this operation.
   */
  hbase::pb::Consistency Consistency() const;

  /**
   * @brief Set whether blocks should be cached for this Scan.This is true by
   * default. When true, default settings of the table and family are used (this
   * will never override caching blocks if the block cache is disabled for that
   * family or entirely).
   * @param cache_blocks - if false, default settings are overridden and blocks
   * will not be cached
   */
  void SetCacheBlocks(bool cache_blocks);

  /**
   * @brief Get whether blocks should be cached for this Scan.
   */
  bool CacheBlocks() const;

  /**
   * @brief Setting whether the caller wants to see the partial results that may
   * be returned from the server. By default this value is false and the
   * complete results will be assembled client side before being delivered to
   * the caller.
   * @param allow_partial_results - if true partial results will be returned.
   */
  void SetAllowPartialResults(bool allow_partial_results);

  /**
   * @brief true when the constructor of this scan understands that the results
   * they will see may only represent a partial portion of a row. The entire row
   * would be retrieved by subsequent calls to ResultScanner.next()
   */
  bool AllowPartialResults() const;

  /**
   * @brief Set the value indicating whether loading CFs on demand should be
   * allowed (cluster default is false). On-demand CF loading doesn't load
   * column families until necessary.
   * @param load_column_families_on_demand
   */
  void SetLoadColumnFamiliesOnDemand(bool load_column_families_on_demand);

  /**
   * @brief Get the raw loadColumnFamiliesOnDemand setting.
   */
  bool LoadColumnFamiliesOnDemand() const;

  /**
   * @brief Get up to the specified number of versions of each column if
   * specified else get default i.e. one.
   * @param max_versions - maximum versions for each column.
   */
  Scan &SetMaxVersions(uint32_t max_versions = 1);

  /**
   * @brief the max number of versions to fetch
   */
  int MaxVersions() const;

  /**
   * @brief Set the maximum result size. The default is -1; this means that no
   * specific maximum result size will be set for this scan, and the global
   * configured value will be used instead. (Defaults to unlimited).
   * @param The maximum result size in bytes.
   */
  void SetMaxResultSize(long max_result_size);

  /**
   * @brief the maximum result size in bytes.
   */
  long MaxResultSize() const;

  /**
   * @brief Get versions of columns only within the specified timestamp range,
   * [min_stamp, max_stamp). Note, default maximum versions to return is 1. If
   * your time range spans more than one version and you want all versions
   * returned, up the number of versions beyond the default.
   * @param min_stamp - minimum timestamp value, inclusive.
   * @param max_stamp - maximum timestamp value, exclusive.
   */
  Scan &SetTimeRange(long min_stamp, long max_stamp);

  /**
   * @brief Get versions of columns with the specified timestamp. Note, default
   * maximum versions to return is 1. If your time range spans more than one
   * version and you want all versions returned, up the number of versions
   * beyond the defaut.
   * @param timestamp - version timestamp
   */
  Scan &SetTimeStamp(long timestamp);

  /**
   * @brief Return Timerange
   */
  const TimeRange &Timerange() const;

  /**
   * @brief Returns true if family map (FamilyMap) is non empty false otherwise
   */
  bool HasFamilies() const;

  /**
   * @brief Returns the Scan family map (FamilyMap) for this Scan operation.
   */
  const FamilyMap &Family() const;

 private:
  std::string start_row_ = "";
  std::string stop_row_ = "";
  uint32_t max_versions_ = 1;
  int caching_ = -1;
  long max_result_size_ = -1;
  bool cache_blocks_ = true;
  bool load_column_families_on_demand_ = false;
  bool reversed_ = false;
  bool small_ = false;
  bool allow_partial_results_ = false;
  hbase::pb::Consistency consistency_ = hbase::pb::Consistency::STRONG;
  std::unique_ptr<TimeRange> tr_ = std::make_unique<TimeRange>();
  FamilyMap family_map_;

  /**
   * @brief Checks for row length validity, throws if length check fails,
   * returns null otherwise.
   * @param row - row whose validity needs to be checked
   * @throws std::runtime_error if row length equals 0 or greater than
   * std::numeric_limits<short>::max();
   */
  void CheckRow(const std::string &row);
};
} /* namespace hbase */
