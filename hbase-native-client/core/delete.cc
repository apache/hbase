

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

#include "core/delete.h"
#include <folly/Conv.h>
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace hbase {

/**
 * @brief Add the specified column to this Delete operation.
 * This is an expensive call in that on the server-side, it first does a
 * get to find the latest versions timestamp.  Then it adds a delete using
 * the fetched cells timestamp.
 *  @param family family name
 *  @param qualifier column qualifier
 */
Delete& Delete::AddColumn(const std::string& family, const std::string& qualifier) {
  return AddColumn(family, qualifier, timestamp_);
}

/**
 *  @brief Add the specified column to this Delete operation.
 *  @param family family name
 *  @param qualifier column qualifier
 *  @param timestamp version timestamp
 */
Delete& Delete::AddColumn(const std::string& family, const std::string& qualifier,
                          int64_t timestamp) {
  if (timestamp < 0) {
    throw std::runtime_error("Timestamp cannot be negative. ts=" +
                             folly::to<std::string>(timestamp));
  }

  return Add(
      std::make_unique<Cell>(row_, family, qualifier, timestamp, "", hbase::CellType::DELETE));
}
/**
 * Delete all versions of the specified column.
 * @param family family name
 * @param qualifier column qualifier
 */
Delete& Delete::AddColumns(const std::string& family, const std::string& qualifier) {
  return AddColumns(family, qualifier, timestamp_);
}
/**
 * Delete all versions of the specified column with a timestamp less than
 * or equal to the specified timestamp.
 * @param family family name
 * @param qualifier column qualifier
 * @param timestamp maximum version timestamp
 */
Delete& Delete::AddColumns(const std::string& family, const std::string& qualifier,
                           int64_t timestamp) {
  if (timestamp < 0) {
    throw std::runtime_error("Timestamp cannot be negative. ts=" +
                             folly::to<std::string>(timestamp));
  }

  return Add(std::make_unique<Cell>(row_, family, qualifier, timestamp, "",
                                    hbase::CellType::DELETE_COLUMN));
}
/**
 * Delete all versions of all columns of the specified family.
 * <p>
 * Overrides previous calls to deleteColumn and deleteColumns for the
 * specified family.
 * @param family family name
 */
Delete& Delete::AddFamily(const std::string& family) { return AddFamily(family, timestamp_); }

/**
 * Delete all columns of the specified family with a timestamp less than
 * or equal to the specified timestamp.
 * <p>
 * Overrides previous calls to deleteColumn and deleteColumns for the
 * specified family.
 * @param family family name
 * @param timestamp maximum version timestamp
 */
Delete& Delete::AddFamily(const std::string& family, int64_t timestamp) {
  const auto& it = family_map_.find(family);
  if (family_map_.end() != it) {
    it->second.clear();
  } else {
    family_map_[family];
  }
  return Add(
      std::make_unique<Cell>(row_, family, "", timestamp, "", hbase::CellType::DELETE_FAMILY));
}
/**
 * Delete all columns of the specified family with a timestamp equal to
 * the specified timestamp.
 * @param family family name
 * @param timestamp version timestamp
 */
Delete& Delete::AddFamilyVersion(const std::string& family, int64_t timestamp) {
  return Add(std::make_unique<Cell>(row_, family, "", timestamp, "",
                                    hbase::CellType::DELETE_FAMILY_VERSION));
}
Delete& Delete::Add(std::unique_ptr<Cell> cell) {
  if (cell->Row() != row_) {
    throw std::runtime_error("The row in " + cell->DebugString() +
                             " doesn't match the original one " + row_);
  }

  family_map_[cell->Family()].push_back(std::move(cell));
  return *this;
}
}  // namespace hbase
