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

using hbase::Cell;
using hbase::CellType;

TEST(CellTest, Constructor) {
  std::string row = "row-value";
  std::string family = "family-value";
  std::string column = "column-value";
  std::string value = "value-value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  CellType cell_type = CellType::PUT;

  Cell cell{row, family, column, timestamp, value, cell_type};

  EXPECT_EQ(row, cell.Row());
  EXPECT_EQ(family, cell.Family());
  EXPECT_EQ(column, cell.Qualifier());
  EXPECT_EQ(value, cell.Value());
  EXPECT_EQ(timestamp, cell.Timestamp());
  EXPECT_EQ(cell_type, cell.Type());
}

TEST(CellTest, CopyConstructor) {
  std::string row = "row-value";
  std::string family = "family-value";
  std::string column = "column-value";
  std::string value = "value-value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  CellType cell_type = CellType::PUT;

  auto cell = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);
  Cell cell2{*cell};
  cell = nullptr;

  EXPECT_EQ(row, cell2.Row());
  EXPECT_EQ(family, cell2.Family());
  EXPECT_EQ(column, cell2.Qualifier());
  EXPECT_EQ(value, cell2.Value());
  EXPECT_EQ(timestamp, cell2.Timestamp());
  EXPECT_EQ(cell_type, cell2.Type());
}

TEST(CellTest, CopyAssignment) {
  std::string row = "row-value";
  std::string family = "family-value";
  std::string column = "column-value";
  std::string value = "value-value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  CellType cell_type = CellType::PUT;

  auto cell = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);
  Cell cell2 = *cell;
  cell = nullptr;

  EXPECT_EQ(row, cell2.Row());
  EXPECT_EQ(family, cell2.Family());
  EXPECT_EQ(column, cell2.Qualifier());
  EXPECT_EQ(value, cell2.Value());
  EXPECT_EQ(timestamp, cell2.Timestamp());
  EXPECT_EQ(cell_type, cell2.Type());
}

TEST(CellTest, CellRowTest) {
  std::string row = "only-row";
  std::string family = "D";
  std::string column = "";
  std::string value = "";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  CellType cell_type = CellType::PUT;
  Cell cell{row, family, column, timestamp, value, cell_type};

  EXPECT_EQ(row, cell.Row());
  EXPECT_EQ(family, cell.Family());
  EXPECT_EQ(column, cell.Qualifier());
  EXPECT_EQ(value, cell.Value());
  EXPECT_EQ(timestamp, cell.Timestamp());
  EXPECT_EQ(cell_type, cell.Type());
}

TEST(CellTest, CellRowFamilyTest) {
  std::string row = "only-row";
  std::string family = "only-family";
  std::string column = "";
  std::string value = "";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  CellType cell_type = CellType::PUT;
  Cell cell{row, family, column, timestamp, value, cell_type};

  EXPECT_EQ(row, cell.Row());
  EXPECT_EQ(family, cell.Family());
  EXPECT_EQ(column, cell.Qualifier());
  EXPECT_EQ(value, cell.Value());
  EXPECT_EQ(timestamp, cell.Timestamp());
  EXPECT_EQ(cell_type, cell.Type());
}

TEST(CellTest, CellRowFamilyValueTest) {
  std::string row = "only-row";
  std::string family = "only-family";
  std::string column = "";
  std::string value = "only-value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  CellType cell_type = CellType::PUT;

  Cell cell{row, family, column, timestamp, value, cell_type};

  EXPECT_EQ(row, cell.Row());
  EXPECT_EQ(family, cell.Family());
  EXPECT_EQ(column, cell.Qualifier());
  EXPECT_EQ(value, cell.Value());
  EXPECT_EQ(timestamp, cell.Timestamp());
  EXPECT_EQ(cell_type, cell.Type());
}

TEST(CellTest, CellRowFamilyColumnValueTest) {
  std::string row = "only-row";
  std::string family = "only-family";
  std::string column = "only-column";
  std::string value = "only-value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  CellType cell_type = CellType::PUT;
  Cell cell{row, family, column, timestamp, value, cell_type};

  EXPECT_EQ(row, cell.Row());
  EXPECT_EQ(family, cell.Family());
  EXPECT_EQ(column, cell.Qualifier());
  EXPECT_EQ(value, cell.Value());
  EXPECT_EQ(timestamp, cell.Timestamp());
  EXPECT_EQ(cell_type, cell.Type());
}

TEST(CellTest, CellDebugString) {
  CellType cell_type = CellType::PUT;
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  std::string value = "value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();

  Cell cell{row, family, column, timestamp, value, cell_type};
  LOG(INFO) << cell.DebugString();
  EXPECT_EQ("row/family:column/LATEST_TIMESTAMP/PUT/vlen=5/seqid=0", cell.DebugString());

  Cell cell2{row, "", column, 42, value, CellType::DELETE};
  LOG(INFO) << cell2.DebugString();
  EXPECT_EQ("row/column/42/DELETE/vlen=5/seqid=0", cell2.DebugString());
}

TEST(CellTest, CellEstimatedSize) {
  CellType cell_type = CellType::PUT;
  int64_t timestamp = std::numeric_limits<int64_t>::max();

  Cell empty{"a", "a", "", timestamp, "", cell_type};
  Cell cell1{"aa", "a", "", timestamp, "", cell_type};
  Cell cell2{"a", "aa", "", timestamp, "", cell_type};
  Cell cell3{"a", "a", "a", timestamp, "", cell_type};
  Cell cell4{"a", "a", "", timestamp, "a", cell_type};
  Cell cell5{"a", "a", "", timestamp, "a", CellType::DELETE};
  Cell cell6{"aaaaaa", "a", "", timestamp, "a", cell_type};

  LOG(INFO) << empty.EstimatedSize();
  LOG(INFO) << cell1.EstimatedSize();

  EXPECT_TRUE(empty.EstimatedSize() > sizeof(Cell));
  EXPECT_TRUE(cell1.EstimatedSize() > empty.EstimatedSize());
  EXPECT_EQ(cell1.EstimatedSize(), cell2.EstimatedSize());
  EXPECT_EQ(cell2.EstimatedSize(), cell3.EstimatedSize());
  EXPECT_EQ(cell3.EstimatedSize(), cell4.EstimatedSize());
  EXPECT_EQ(cell4.EstimatedSize(), cell5.EstimatedSize());
  EXPECT_TRUE(cell6.EstimatedSize() > cell1.EstimatedSize());
}
