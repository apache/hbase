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

#include "core/result.h"

namespace hbase {

Result::~Result() {}

Result::Result(const std::vector<std::shared_ptr<Cell> > &cells, bool exists, bool stale,
               bool partial)
    : exists_(exists), stale_(stale), partial_(partial), cells_(cells) {
  row_ = (cells_.size() == 0 ? "" : cells_[0]->Row());
}

Result::Result(const Result &result) {
  exists_ = result.exists_;
  stale_ = result.stale_;
  partial_ = result.partial_;
  row_ = result.row_;
  if (!result.cells_.empty()) {
    for (const auto &cell : result.cells_) {
      cells_.push_back(cell);
    }
  }
}

const std::vector<std::shared_ptr<Cell> > &Result::Cells() const { return cells_; }

std::vector<std::shared_ptr<Cell> > Result::ColumnCells(const std::string &family,
                                                        const std::string &qualifier) const {
  std::vector<std::shared_ptr<Cell> > column_cells;
  // TODO implement a BinarySearch here ?
  for (const auto &cell : cells_) {
    if (cell->Family() == family && cell->Qualifier() == qualifier) {
      column_cells.push_back(cell);
    }
  }
  return column_cells;
}

const std::shared_ptr<Cell> Result::ColumnLatestCell(const std::string &family,
                                                     const std::string &qualifier) const {
  // TODO implement a BinarySearch here ?
  for (const auto &cell : cells_) {
    // We find the latest(first) occurrence of the Cell for a given column and
    // qualifier and break
    if (cell->Family() == family && cell->Qualifier() == qualifier) {
      return cell;
    }
  }
  return nullptr;
}

optional<std::string> Result::Value(const std::string &family, const std::string &qualifier) const {
  std::shared_ptr<Cell> latest_cell(ColumnLatestCell(family, qualifier));
  if (latest_cell.get()) {
    return optional<std::string>(latest_cell->Value());
  }
  return optional<std::string>();
}

bool Result::IsEmpty() const { return cells_.empty(); }

const std::string &Result::Row() const { return row_; }

int Result::Size() const { return cells_.size(); }

ResultMap Result::Map() const {
  ResultMap result_map;
  for (const auto &cell : cells_) {
    result_map[cell->Family()][cell->Qualifier()][cell->Timestamp()] = cell->Value();
  }
  return result_map;
}

std::map<std::string, std::string> Result::FamilyMap(const std::string &family) const {
  std::map<std::string, std::string> family_map;
  if (!IsEmpty()) {
    auto result_map = Map();
    auto itr = result_map.find(family);
    if (itr == result_map.end()) {
      return family_map;
    }

    for (auto qitr = itr->second.begin(); qitr != itr->second.end(); ++qitr) {
      for (auto vitr = qitr->second.begin(); vitr != qitr->second.end(); ++vitr) {
        // We break after inserting the first value. Result.java takes only
        // the first value
        family_map[qitr->first] = vitr->second;
        break;
      }
    }
  }

  return family_map;
}

std::string Result::DebugString() const {
  std::string ret{"keyvalues="};
  if (IsEmpty()) {
    ret += "NONE";
    return ret;
  }
  ret += "{";
  bool is_first = true;
  for (const auto &cell : cells_) {
    if (is_first) {
      is_first = false;
    } else {
      ret += ", ";
    }
    ret += cell->DebugString();
  }
  ret += "}";

  return ret;
}

size_t Result::EstimatedSize() const {
  size_t s = sizeof(Result);
  s += row_.capacity();
  for (const auto c : cells_) {
    s += sizeof(std::shared_ptr<Cell>);
    s + c->EstimatedSize();
  }
  return s;
}

} /* namespace hbase */
