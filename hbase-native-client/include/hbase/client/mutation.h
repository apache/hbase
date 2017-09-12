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
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "hbase/client/cell.h"
#include "hbase/client/row.h"
#include "hbase/if/Client.pb.h"

namespace hbase {

class Mutation : public Row {
 public:
  /**
   * Constructors
   */
  explicit Mutation(const std::string& row);
  Mutation(const std::string& row, int64_t timestamp);
  Mutation(const Mutation& cmutation);
  Mutation& operator=(const Mutation& cmutation);

  virtual ~Mutation() = default;

  /**
   * @brief Returns the Mutation family map for this operation.
   */
  const std::map<std::string, std::vector<std::unique_ptr<Cell>>>& FamilyMap() const {
    return family_map_;
  }

  /**
   * @brief Returns the timerange for this Get
   */
  int64_t TimeStamp() const { return timestamp_; }

  /**
   * @brief Get versions of columns with the specified timestamp.
   * @param The timestamp to be set
   */
  Mutation& SetTimeStamp(int64_t timestamp) {
    timestamp_ = timestamp;
    return *this;
  }

  /**
   * @brief Returns true if family map is non empty false otherwise
   */
  bool HasFamilies() const;

  /**
   * @brief Returns the durability level for this Mutation operation
   */
  pb::MutationProto_Durability Durability() const;

  /**
   * @brief Sets the durability level for this Mutation operation
   * @param durability  the durability to be set
   */
  Mutation& SetDurability(pb::MutationProto_Durability durability);

 public:
  static const constexpr int64_t kLatestTimestamp = std::numeric_limits<int64_t>::max();

 protected:
  std::map<std::string, std::vector<std::unique_ptr<Cell>>> family_map_;
  pb::MutationProto_Durability durability_ =
      hbase::pb::MutationProto_Durability::MutationProto_Durability_USE_DEFAULT;
  int64_t timestamp_ = kLatestTimestamp;

  std::unique_ptr<Cell> CreateCell(const std::string& family, const std::string& qualifier,
                                   int64_t timestamp, const std::string& value);
};

}  // namespace hbase
