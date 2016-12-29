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

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "core/cell.h"

namespace hbase {

/**
 * @brief Map of families to all versions of its qualifiers and values
 * We need to have a reverse ordered map, when storing TS -> value, so that the
 * most recent value is stored first
 */
using ResultMap = std::map<
    std::string, std::map<std::string, std::map<int64_t, std::string,
                                                std::greater<int64_t> > > >;

/**
 * @brief Map of qualifiers to values.
 */
using ResultFamilyMap = std::map<std::string, std::string>;

class Result {
 public:
  /**
   * Constructors
   */
  Result(const std::vector<std::shared_ptr<Cell> > &cells, bool exists,
         bool stale, bool partial);
  Result(const Result &result);
  ~Result();

  /**
   * @brief Return the vector of Cells backing this Result instance. This vector
   * will be ordered in the same manner
   * as the one which was passed while creation of the Result instance.
   */
  const std::vector<std::shared_ptr<Cell> > &Cells() const;

  /**
   * @brief  Return a vector of Cells for the family and qualifier or empty list
   * if the column
   * did not exist in the result.
   * @param family - column family
   * @param qualifier - column qualifier
   */
  std::vector<std::shared_ptr<Cell> > ColumnCells(
      const std::string &family, const std::string &qualifier) const;

  /**
   * @brief Returns the Cell for the most recent timestamp for a given family
   * and qualifier.
   * Returns map of qualifiers to values, only includes latest values
   * @param family - column family.
   * @param qualifier - column qualifier
   */
  const std::shared_ptr<Cell> ColumnLatestCell(
      const std::string &family, const std::string &qualifier) const;

  /**
   * @brief Get the latest version of the specified family and qualifier.
   * @param family - column family
   * @param qualifier - column qualifier
   */
  std::shared_ptr<std::string> Value(const std::string &family,
                                     const std::string &qualifier) const;

  /**
   * @brief Returns if the underlying Cell vector is empty or not
   */
  bool IsEmpty() const;

  /**
   * @brief Retrieves the row key that corresponds to the row from which this
   * Result was created.
   */
  const std::string &Row() const;

  /**
   * @brief Returns the size of the underlying Cell vector
   */
  const int Size() const;

  /**
   * @brief Map of families to all versions of its qualifiers and values.
   * Returns a three level Map of the form:
   * Map<family,Map<qualifier,Map<timestamp,value>>>>
   * All other map returning methods make use of this map internally
   * The Map is created when the Result instance is created
   */
  const ResultMap &Map() const;

  /**
   * @brief Map of qualifiers to values.
   * Returns a Map of the form: Map<qualifier,value>
   * @param family - column family to get
   */
  const ResultFamilyMap FamilyMap(const std::string &family) const;

 private:
  bool exists_ = false;
  bool stale_ = false;
  bool partial_ = false;
  std::string row_ = "";
  std::vector<std::shared_ptr<Cell> > cells_;
  ResultMap result_map_;
};
} /* namespace hbase */
