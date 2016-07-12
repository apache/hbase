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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>

using namespace hbase;
TEST(CellTest, CellFailureTest) {
  CellType cell_type = CellType::PUT;
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  std::string value = "value";
  long timestamp = std::numeric_limits<long>::max();
  std::string tags = "";
  std::unique_ptr<Cell> cell(
      new Cell(row, family, column, timestamp, value, cell_type));
  if (cell.get()) {
    EXPECT_NE("row-value", cell.get()->Row());
    EXPECT_NE("family-value", cell.get()->Family());
    EXPECT_NE("column-value", cell.get()->Qualifier());
    EXPECT_NE("value-value", cell.get()->Value());
    EXPECT_NE(8975431260, cell.get()->Timestamp());
    EXPECT_NE(CellType::MAXIMUM, cell.get()->Type());
  }
}

TEST(CellTest, CellSuceessTest) {
  std::string row = "row-value";
  std::string family = "family-value";
  std::string column = "column-value";
  std::string value = "value-value";
  long timestamp = std::numeric_limits<long>::max();
  CellType cell_type = CellType::PUT;
  const std::unique_ptr<Cell> cell(
      new Cell(row, family, column, timestamp, value, cell_type));
  if (cell.get()) {
    EXPECT_EQ(row, cell.get()->Row());
    EXPECT_EQ(family, cell.get()->Family());
    EXPECT_EQ(column, cell.get()->Qualifier());
    EXPECT_EQ(value, cell.get()->Value());
    EXPECT_EQ(timestamp, cell.get()->Timestamp());
    EXPECT_EQ(cell_type, cell.get()->Type());
  }
}

TEST(CellTest, MultipleCellsTest) {
  std::vector<const Cell *> cells;
  for (int i = 0; i < 5; i++) {
    std::string row = "row-value";
    std::string family = "family-value";
    std::string column = "column-value";
    std::string value = "value-value";
    long timestamp = std::numeric_limits<long>::max();
    row += std::to_string(i);
    value += std::to_string(i);
    CellType cell_type = CellType::PUT;
    const Cell *cell =
        new Cell(row, family, column, timestamp, value, cell_type);
    cells.push_back(cell);
  }
  int i = 0;
  for (const auto cell : cells) {
    std::string row = "row-value";
    std::string value = "value-value";
    row += std::to_string(i);
    value += std::to_string(i);
    EXPECT_EQ(row, cell->Row());
    EXPECT_EQ("family-value", cell->Family());
    EXPECT_EQ("column-value", cell->Qualifier());
    EXPECT_EQ(value, cell->Value());
    EXPECT_EQ(std::numeric_limits<long>::max(), cell->Timestamp());
    EXPECT_EQ(CellType::PUT, cell->Type());
    i += 1;
  }
  for (const auto cell : cells) {
    delete cell;
  }
  cells.clear();
}

TEST(CellTest, CellRowTest) {
  std::string row = "only-row";
  std::string family = "D";
  std::string column = "";
  std::string value = "";
  long timestamp = std::numeric_limits<long>::max();
  CellType cell_type = CellType::PUT;
  std::unique_ptr<Cell> cell(
      new Cell(row, family, column, timestamp, value, cell_type));
  if (cell.get()) {
    EXPECT_EQ(row, cell.get()->Row());
    EXPECT_EQ(family, cell.get()->Family());
    EXPECT_EQ(column, cell.get()->Qualifier());
    EXPECT_EQ(value, cell.get()->Value());
    EXPECT_EQ(timestamp, cell.get()->Timestamp());
    EXPECT_EQ(cell_type, cell.get()->Type());
  }
}

TEST(CellTest, CellRowFamilyTest) {
  std::string row = "only-row";
  std::string family = "only-family";
  std::string column = "";
  std::string value = "";
  long timestamp = std::numeric_limits<long>::max();
  CellType cell_type = CellType::PUT;
  const std::unique_ptr<Cell> cell(
      new Cell(row, family, column, timestamp, value, cell_type));
  if (cell.get()) {
    EXPECT_EQ(row, cell.get()->Row());
    EXPECT_EQ(family, cell.get()->Family());
    EXPECT_EQ(column, cell.get()->Qualifier());
    EXPECT_EQ(value, cell.get()->Value());
    EXPECT_EQ(timestamp, cell.get()->Timestamp());
    EXPECT_EQ(cell_type, cell.get()->Type());
  }
}

TEST(CellTest, CellRowFamilyValueTest) {
  std::string row = "only-row";
  std::string family = "only-family";
  std::string column = "";
  std::string value = "only-value";
  long timestamp = std::numeric_limits<long>::max();
  CellType cell_type = CellType::PUT;
  const std::unique_ptr<Cell> cell(
      new Cell(row, family, column, timestamp, value, cell_type));
  if (cell.get()) {
    EXPECT_EQ(row, cell.get()->Row());
    EXPECT_EQ(family, cell.get()->Family());
    EXPECT_EQ(column, cell.get()->Qualifier());
    EXPECT_EQ(value, cell.get()->Value());
    EXPECT_EQ(timestamp, cell.get()->Timestamp());
    EXPECT_EQ(cell_type, cell.get()->Type());
  }
}

TEST(CellTest, CellRowFamilyColumnValueTest) {
  std::string row = "only-row";
  std::string family = "only-family";
  std::string column = "only-column";
  std::string value = "only-value";
  long timestamp = std::numeric_limits<long>::max();
  CellType cell_type = CellType::PUT;
  std::unique_ptr<Cell> cell(
      new Cell(row, family, column, timestamp, value, cell_type));
  if (cell.get()) {
    EXPECT_EQ(row, cell.get()->Row());
    EXPECT_EQ(family, cell.get()->Family());
    EXPECT_EQ(column, cell.get()->Qualifier());
    EXPECT_EQ(value, cell.get()->Value());
    EXPECT_EQ(timestamp, cell.get()->Timestamp());
    EXPECT_EQ(cell_type, cell.get()->Type());
  }
}
