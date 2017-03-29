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
#include <climits>
#include <limits>
#include <stdexcept>

#include "folly/Conv.h"
#include "utils/bytes-util.h"

namespace hbase {

Cell::Cell(const std::string &row, const std::string &family, const std::string &qualifier,
           const int64_t timestamp, const std::string &value, const hbase::CellType &cell_type)
    : row_(row),
      family_(family),
      qualifier_(qualifier),
      timestamp_(timestamp),
      cell_type_(cell_type),
      value_(value),
      sequence_id_(0) {
  if (0 == row.size()) throw std::runtime_error("Row size should be greater than 0");

  if (0 >= timestamp) throw std::runtime_error("Timestamp should be greater than 0");
}

Cell::Cell(const Cell &cell)
    : row_(cell.row_),
      family_(cell.family_),
      qualifier_(cell.qualifier_),
      timestamp_(cell.timestamp_),
      cell_type_(cell.cell_type_),
      value_(cell.value_),
      sequence_id_(cell.sequence_id_) {}

Cell &Cell::operator=(const Cell &cell) {
  row_ = cell.row_;
  family_ = cell.family_;
  qualifier_ = cell.qualifier_;
  timestamp_ = cell.timestamp_;
  cell_type_ = cell.cell_type_;
  value_ = cell.value_;
  sequence_id_ = cell.sequence_id_;

  return *this;
}

Cell::~Cell() {}

const std::string &Cell::Row() const { return row_; }

const std::string &Cell::Family() const { return family_; }

const std::string &Cell::Qualifier() const { return qualifier_; }

int64_t Cell::Timestamp() const { return timestamp_; }

const std::string &Cell::Value() const { return value_; }

hbase::CellType Cell::Type() const { return cell_type_; }

int64_t Cell::SequenceId() const { return sequence_id_; }

std::string Cell::DebugString() const {
  std::string timestamp_str;
  if (timestamp_ == std::numeric_limits<int64_t>::max()) {
    timestamp_str = "LATEST_TIMESTAMP";
  } else {
    timestamp_str = folly::to<std::string>(timestamp_);
  }

  return BytesUtil::ToStringBinary(row_) + "/" + BytesUtil::ToStringBinary(family_) +
         (family_.empty() ? "" : ":") + BytesUtil::ToStringBinary(qualifier_) + "/" +
         timestamp_str + "/" + TypeToString(cell_type_) + "/vlen=" +
         folly::to<std::string>(value_.size()) + "/seqid=" + folly::to<std::string>(sequence_id_);
}

const char *Cell::TypeToString(CellType type) {
  switch (type) {
    case MINIMUM:
      return "MINIMUM";
    case PUT:
      return "PUT";
    case DELETE:
      return "DELETE";
    case DELETE_COLUMN:
      return "DELETE_COLUMN";
    case DELETE_FAMILY:
      return "DELETE_FAMILY";
    case MAXIMUM:
      return "MAXIMUM";
    default:
      return "UNKNOWN";
  }
}

} /* namespace hbase */
