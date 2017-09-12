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

#include <limits>
#include <stdexcept>
#include <string>

#pragma once

namespace hbase {

class Row {
 public:
  Row() {}
  explicit Row(const std::string &row) : row_(row) { CheckRow(row_); }

  /**
   * @brief Returns the row for the Row interface.
   */
  const std::string &row() const { return row_; }
  virtual ~Row() {}

 private:
  /**
   * @brief Checks if the row for this Get operation is proper or not
   * @param row Row to check
   * @throws std::runtime_error if row is empty or greater than
   * MAX_ROW_LENGTH(i.e. std::numeric_limits<short>::max())
   */
  void CheckRow(const std::string &row) {
    const int16_t kMaxRowLength = std::numeric_limits<int16_t>::max();
    size_t row_length = row.size();
    if (0 == row_length) {
      throw std::runtime_error("Row length can't be 0");
    }
    if (row_length > kMaxRowLength) {
      throw std::runtime_error("Length of " + row + " is greater than max row size: " +
                               std::to_string(kMaxRowLength));
    }
  }

 protected:
  std::string row_ = "";
};

} /* namespace hbase */
