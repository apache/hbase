

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

#include "core/mutation.h"
#include <algorithm>
#include <limits>
#include <stdexcept>

namespace hbase {

Mutation::Mutation(const std::string &row) : Row(row) {}
Mutation::Mutation(const std::string &row, int64_t timestamp) : Row(row), timestamp_(timestamp) {}

Mutation::Mutation(const Mutation &mutation) {
  row_ = mutation.row_;
  durability_ = mutation.durability_;
  timestamp_ = mutation.timestamp_;
  for (auto const &e : mutation.family_map_) {
    for (auto const &c : e.second) {
      family_map_[e.first].push_back(std::make_unique<Cell>(*c));
    }
  }
}

Mutation &Mutation::operator=(const Mutation &mutation) {
  row_ = mutation.row_;
  durability_ = mutation.durability_;
  timestamp_ = mutation.timestamp_;
  for (auto const &e : mutation.family_map_) {
    for (auto const &c : e.second) {
      family_map_[e.first].push_back(std::make_unique<Cell>(*c));
    }
  }
  return *this;
}

pb::MutationProto_Durability Mutation::Durability() const { return durability_; }

Mutation &Mutation::SetDurability(pb::MutationProto_Durability durability) {
  durability_ = durability;
  return *this;
}

bool Mutation::HasFamilies() const { return !family_map_.empty(); }

std::unique_ptr<Cell> Mutation::CreateCell(const std::string &family, const std::string &qualifier,
                                           int64_t timestamp, const std::string &value) {
  return std::make_unique<Cell>(row_, family, qualifier, timestamp, value, hbase::CellType::PUT);
}

}  // namespace hbase
