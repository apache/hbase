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

class Append : public Mutation {
 public:
  /**
   * Constructors
   */
  explicit Append(const std::string& row) : Mutation(row) {}
  Append(const Append& cappend) : Mutation(cappend) {}
  Append& operator=(const Append& cappend) {
    Mutation::operator=(cappend);
    return *this;
  }

  ~Append() = default;

  /**
   *  @brief Add the specified column and value to this Append operation.
   *  @param family family name
   *  @param qualifier column qualifier
   *  @param value value to append
   */
  Append& Add(const std::string& family, const std::string& qualifier, const std::string& value);
  Append& Add(std::unique_ptr<Cell> cell);
};

}  // namespace hbase
