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
#include "core/cell.h"
#include "core/mutation.h"

namespace hbase {

class Delete : public Mutation {
 public:
  /**
   * Constructors
   */
  /*
   * If no further operations are done, this will delete everything
   * associated with the specified row (all versions of all columns in all
   * families), with timestamp from current point in time to the past.
   * Cells defining timestamp for a future point in time
   * (timestamp > current time) will not be deleted.
   */
  explicit Delete(const std::string& row) : Mutation(row) {}

  Delete(const std::string& row, int64_t timestamp) : Mutation(row, timestamp) {}
  Delete(const Delete& cdelete) : Mutation(cdelete) {}
  Delete& operator=(const Delete& cdelete) {
    Mutation::operator=(cdelete);
    return *this;
  }

  ~Delete() = default;

  /**
   *  @brief Add the specified column to this Delete operation.
   *  @param family family name
   *  @param qualifier column qualifier
   */
  Delete& AddColumn(const std::string& family, const std::string& qualifier);

  /**
   *  @brief Add the specified column to this Delete operation.
   *  @param family family name
   *  @param qualifier column qualifier
   *  @param timestamp version timestamp
   */
  Delete& AddColumn(const std::string& family, const std::string& qualifier, int64_t timestamp);

  /**
   *  @brief Deletes all versions of the specified column
   *  @param family family name
   *  @param qualifier column qualifier
   */
  Delete& AddColumns(const std::string& family, const std::string& qualifier);
  /**
   *  @brief Deletes all versions of the specified column with a timestamp less than
   * or equal to the specified timestamp
   *  @param family family name
   *  @param qualifier column qualifier
   *  @param timestamp version timestamp
   */
  Delete& AddColumns(const std::string& family, const std::string& qualifier, int64_t timestamp);
  /**
   *  @brief Add the specified family to this Delete operation.
   *  @param family family name
   */
  Delete& AddFamily(const std::string& family);

  /**
   *  @brief Deletes all columns of the specified family with a timestamp less than
   * or equal to the specified timestamp
   *  @param family family name
   *  @param timestamp version timestamp
   */
  Delete& AddFamily(const std::string& family, int64_t timestamp);
  /**
   *  @brief Deletes all columns of the specified family with a timestamp
   *   equal to the specified timestamp
   *  @param family family name
   *  @param timestamp version timestamp
   */
  Delete& AddFamilyVersion(const std::string& family, int64_t timestamp);
  /**
   * Advanced use only.
   * Add an existing delete marker to this Delete object.
   */
  Delete& Add(std::unique_ptr<Cell> cell);
};

}  // namespace hbase
