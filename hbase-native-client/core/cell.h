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
#include <string>

namespace hbase {

enum CellType {
  MINIMUM = 0,
  PUT = 4,
  DELETE = 8,
  DELETEFAMILYVERSION = 10,
  DELETE_COLUMN = 12,
  DELETE_FAMILY = 14,
  MAXIMUM = 255
};

class Cell {
 public:
  Cell(const std::string &row, const std::string &family, const std::string &qualifier,
       const int64_t timestamp, const std::string &value, const hbase::CellType &cell_type);
  virtual ~Cell();
  const std::string &Row() const;
  const std::string &Family() const;
  const std::string &Qualifier() const;
  int64_t Timestamp() const;
  const std::string &Value() const;
  CellType Type() const;
  int64_t SequenceId() const;

 private:
  std::string row_;
  std::string family_;
  std::string qualifier_;
  // Since java does not have unsigned, we are also using signed numerics here
  // so that we won't have surprises when large uint64's are treated as
  // negative values in the java server side
  int64_t timestamp_;
  hbase::CellType cell_type_;
  std::string value_;
  int64_t sequence_id_;
};

}  // namespace hbase
