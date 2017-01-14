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
    : exists_(exists), stale_(stale), partial_(partial) {
  for (const auto &cell : cells) {
    cells_.push_back(cell);
    // We create the map when cells are added. unlike java where map is created
    // when result.getMap() is called
    result_map_[cell->Family()][cell->Qualifier()][cell->Timestamp()] = cell->Value();
  }
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
      result_map_[cell->Family()][cell->Qualifier()][cell->Timestamp()] = cell->Value();
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

std::shared_ptr<std::string> Result::Value(const std::string &family,
                                           const std::string &qualifier) const {
  std::shared_ptr<Cell> latest_cell(ColumnLatestCell(family, qualifier));
  if (latest_cell.get()) {
    return std::make_shared<std::string>(latest_cell->Value());
  }
  return nullptr;
}

bool Result::IsEmpty() const { return cells_.empty(); }

const std::string &Result::Row() const { return row_; }

const int Result::Size() const { return cells_.size(); }

const ResultMap &Result::Map() const { return result_map_; }

const ResultFamilyMap Result::FamilyMap(const std::string &family) const {
  ResultFamilyMap family_map;
  if (!IsEmpty()) {
    for (auto itr = result_map_.begin(); itr != result_map_.end(); ++itr) {
      if (family == itr->first) {
        for (auto qitr = itr->second.begin(); qitr != itr->second.end(); ++qitr) {
          for (auto vitr = qitr->second.begin(); vitr != qitr->second.end(); ++vitr) {
            // We break after inserting the first value. Result.java takes only
            // the first value
            family_map[qitr->first] = vitr->second;
            break;
          }
        }
      }
    }
  }
  return family_map;
}
} /* namespace hbase */
