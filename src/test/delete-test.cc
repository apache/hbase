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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "hbase/client/delete.h"
#include "hbase/client/mutation.h"
#include "hbase/utils/time-util.h"

using hbase::Delete;
using hbase::Cell;
using hbase::CellType;
using hbase::Mutation;
using hbase::TimeUtil;

const constexpr int64_t Mutation::kLatestTimestamp;

TEST(Delete, Row) {
  Delete del{"foo"};
  EXPECT_EQ("foo", del.row());
}

TEST(Delete, Timestamp) {
  Delete del{"row"};

  // test default timestamp
  EXPECT_EQ(Mutation::kLatestTimestamp, del.TimeStamp());

  // set custom timestamp
  auto ts = TimeUtil::ToMillis(TimeUtil::GetNowNanos());
  del.SetTimeStamp(ts);
  EXPECT_EQ(ts, del.TimeStamp());

  // Add a column with custom timestamp
  del.AddColumn("f", "q");
  auto &cell = del.FamilyMap().at("f")[0];
  EXPECT_EQ(ts, cell->Timestamp());
}

TEST(Delete, HasFamilies) {
  Delete del{"row"};

  EXPECT_EQ(false, del.HasFamilies());

  del.AddColumn("f", "q");
  EXPECT_EQ(true, del.HasFamilies());
}

TEST(Delete, Add) {
  CellType cell_type = CellType::DELETE;
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  auto cell = std::make_unique<Cell>(row, family, column, timestamp, "", cell_type);

  // add first cell
  Delete del{"row"};
  del.Add(std::move(cell));
  EXPECT_EQ(1, del.FamilyMap().size());
  EXPECT_EQ(1, del.FamilyMap().at(family).size());

  // add a non-matching row
  auto cell2 = std::make_unique<Cell>(row, family, column, timestamp, "", cell_type);
  Delete del2{"foo"};
  ASSERT_THROW(del2.Add(std::move(cell2)), std::runtime_error);  // rows don't match

  // add a second cell with same family
  auto cell3 = std::make_unique<Cell>(row, family, "column-2", timestamp, "", cell_type);
  del.Add(std::move(cell3));
  EXPECT_EQ(1, del.FamilyMap().size());
  EXPECT_EQ(2, del.FamilyMap().at(family).size());

  // add a cell to a different family
  auto cell4 = std::make_unique<Cell>(row, "family-2", "column-2", timestamp, "", cell_type);
  del.Add(std::move(cell4));
  EXPECT_EQ(2, del.FamilyMap().size());
  EXPECT_EQ(1, del.FamilyMap().at("family-2").size());
}

TEST(Delete, AddColumn) {
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";

  Delete del{"row"};
  del.AddColumn(family, column);
  EXPECT_EQ(1, del.FamilyMap().size());
  EXPECT_EQ(1, del.FamilyMap().at(family).size());

  // add a second cell with same family
  del.AddColumn(family, "column-2");
  EXPECT_EQ(1, del.FamilyMap().size());
  EXPECT_EQ(2, del.FamilyMap().at(family).size());

  // add a cell to a different family
  del.AddColumn("family-2", column);
  EXPECT_EQ(2, del.FamilyMap().size());
  EXPECT_EQ(1, del.FamilyMap().at("family-2").size());

  // use the AddColumn overload
  auto ts = TimeUtil::ToMillis(TimeUtil::GetNowNanos());
  del.AddColumn(family, column, ts);
  EXPECT_EQ(2, del.FamilyMap().size());
  EXPECT_EQ(3, del.FamilyMap().at(family).size());
  auto &cell = del.FamilyMap().at(family)[2];
  EXPECT_EQ(ts, cell->Timestamp());
}
