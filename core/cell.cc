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

#include "core/cell.h"
#include <stdexcept>

namespace hbase {

Cell::Cell(const std::string &row, const std::string &family,
           const std::string &qualifier, const int64_t timestamp,
           const std::string &value, const hbase::CellType &cell_type)
    : row_(row),
      family_(family),
      qualifier_(qualifier),
      timestamp_(timestamp),
      cell_type_(cell_type),
      value_(value),
      sequence_id_(0) {
  if (0 == row.size())
    throw std::runtime_error("Row size should be greater than 0");

  if (0 == family.size())
    throw std::runtime_error("Column family size should be greater than 0");

  if (0 >= timestamp)
    throw std::runtime_error("Timestamp should be greater than 0");
}

Cell::~Cell() {}

const std::string &Cell::Row() const { return row_; }

const std::string &Cell::Family() const { return family_; }

const std::string &Cell::Qualifier() const { return qualifier_; }

int64_t Cell::Timestamp() const { return timestamp_; }

const std::string &Cell::Value() const { return value_; }

hbase::CellType Cell::Type() const { return cell_type_; }

int64_t Cell::SequenceId() const { return sequence_id_; }

} /* namespace hbase */
