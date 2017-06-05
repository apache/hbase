

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

#include "core/increment.h"
#include <folly/Conv.h>
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "utils/bytes-util.h"

namespace hbase {

/**
 *  @brief Increment the column from the specific family with the specified qualifier
 * by the specified amount.
 *  @param family family name
 *  @param qualifier column qualifier
 *  @param amount amount to increment by
 */
Increment& Increment::AddColumn(const std::string& family, const std::string& qualifier,
        int64_t amount) {
  family_map_[family].push_back(std::move(
            std::make_unique<Cell>(row_, family, qualifier, timestamp_, BytesUtil::ToString(amount),
            hbase::CellType::PUT)));
  return *this;
}
Increment& Increment::Add(std::unique_ptr<Cell> cell) {
  if (cell->Row() != row_) {
    throw std::runtime_error("The row in " + cell->DebugString() +
                             " doesn't match the original one " + row_);
  }

  family_map_[cell->Family()].push_back(std::move(cell));
  return *this;
}

}  // namespace hbase
