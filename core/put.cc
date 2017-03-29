

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

#include "core/put.h"
#include <folly/Conv.h>
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace hbase {

/**
 *  @brief Add the specified column and value to this Put operation.
 *  @param family family name
 *  @param qualifier column qualifier
 *  @param value column value
 */
Put& Put::AddColumn(const std::string& family, const std::string& qualifier,
                    const std::string& value) {
  return AddColumn(family, qualifier, kLatestTimestamp, value);
}

/**
 *  @brief Add the specified column and value to this Put operation.
 *  @param family family name
 *  @param qualifier column qualifier
 *  @param timestamp version timestamp
 *  @param value column value
 */
Put& Put::AddColumn(const std::string& family, const std::string& qualifier, int64_t timestamp,
                    const std::string& value) {
  if (timestamp < 0) {
    throw std::runtime_error("Timestamp cannot be negative. ts=" +
                             folly::to<std::string>(timestamp));
  }

  return Add(CreateCell(family, qualifier, timestamp, value));
}

Put& Put::Add(std::unique_ptr<Cell> cell) {
  if (cell->Row() != row_) {
    throw std::runtime_error("The row in" + cell->DebugString() +
                             " doesn't match the original one " + row_);
  }

  family_map_[cell->Family()].push_back(std::move(cell));
  return *this;
}
}  // namespace hbase
