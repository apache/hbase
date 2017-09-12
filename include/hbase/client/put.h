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
#include "hbase/client/cell.h"
#include "hbase/client/mutation.h"

namespace hbase {

class Put : public Mutation {
 public:
  /**
   * Constructors
   */
  explicit Put(const std::string& row) : Mutation(row) {}
  Put(const std::string& row, int64_t timestamp) : Mutation(row, timestamp) {}
  Put(const Put& cput) : Mutation(cput) {}
  Put& operator=(const Put& cput) {
    Mutation::operator=(cput);
    return *this;
  }

  ~Put() = default;

  /**
   *  @brief Add the specified column and value to this Put operation.
   *  @param family family name
   *  @param qualifier column qualifier
   *  @param value column value
   */
  Put& AddColumn(const std::string& family, const std::string& qualifier, const std::string& value);

  /**
   *  @brief Add the specified column and value to this Put operation.
   *  @param family family name
   *  @param qualifier column qualifier
   *  @param timestamp version timestamp
   *  @param value column value
   */
  Put& AddColumn(const std::string& family, const std::string& qualifier, int64_t timestamp,
                 const std::string& value);

  Put& Add(std::unique_ptr<Cell> cell);
};

}  // namespace hbase
