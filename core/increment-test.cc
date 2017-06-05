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

#include "core/mutation.h"
#include "core/increment.h"
#include "core/put.h"
#include "utils/time-util.h"

using hbase::Increment;
using hbase::Increment;
using hbase::Cell;
using hbase::CellType;
using hbase::Mutation;
using hbase::TimeUtil;

const constexpr int64_t Mutation::kLatestTimestamp;

TEST(Increment, Row) {
  Increment incr{"foo"};
  EXPECT_EQ("foo", incr.row());
}

TEST(Increment, Durability) {
  Increment incr{"row"};
  EXPECT_EQ(hbase::pb::MutationProto_Durability_USE_DEFAULT, incr.Durability());

  auto skipWal = hbase::pb::MutationProto_Durability_SKIP_WAL;
  incr.SetDurability(skipWal);
  EXPECT_EQ(skipWal, incr.Durability());
}

TEST(Increment, Timestamp) {
  Increment incr{"row"};

  // test default timestamp
  EXPECT_EQ(Mutation::kLatestTimestamp, incr.TimeStamp());

  // set custom timestamp
  auto ts = TimeUtil::ToMillis(TimeUtil::GetNowNanos());
  incr.SetTimeStamp(ts);
  EXPECT_EQ(ts, incr.TimeStamp());

  // Add a column with custom timestamp
  incr.AddColumn("f", "q", 5l);
  auto &cell = incr.FamilyMap().at("f")[0];
  EXPECT_EQ(ts, cell->Timestamp());
}

TEST(Increment, HasFamilies) {
  Increment incr{"row"};

  EXPECT_EQ(false, incr.HasFamilies());

  incr.AddColumn("f", "q", 5l);
  EXPECT_EQ(true, incr.HasFamilies());
}

TEST(Increment, Add) {
  CellType cell_type = CellType::PUT;
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  std::string value = "value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  auto cell = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);

  // add first cell
  Increment incr{"row"};
  incr.Add(std::move(cell));
  EXPECT_EQ(1, incr.FamilyMap().size());
  EXPECT_EQ(1, incr.FamilyMap().at(family).size());

  // add a non-matching row
  auto cell2 = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);
  Increment incr2{"foo"};
  ASSERT_THROW(incr2.Add(std::move(cell2)), std::runtime_error);  // rows don't match

  // add a second cell with same family
  auto cell3 = std::make_unique<Cell>(row, family, "column-2", timestamp, value, cell_type);
  incr.Add(std::move(cell3));
  EXPECT_EQ(1, incr.FamilyMap().size());
  EXPECT_EQ(2, incr.FamilyMap().at(family).size());

  // add a cell to a different family
  auto cell4 = std::make_unique<Cell>(row, "family-2", "column-2", timestamp, value, cell_type);
  incr.Add(std::move(cell4));
  EXPECT_EQ(2, incr.FamilyMap().size());
  EXPECT_EQ(1, incr.FamilyMap().at("family-2").size());
}

TEST(Increment, AddColumn) {
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  std::string value = "value";

  Increment incr{"row"};
  incr.AddColumn(family, column, 5l);
  EXPECT_EQ(1, incr.FamilyMap().size());
  EXPECT_EQ(1, incr.FamilyMap().at(family).size());

  // add a second cell with same family
  incr.AddColumn(family, "column-2", 6l);
  EXPECT_EQ(1, incr.FamilyMap().size());
  EXPECT_EQ(2, incr.FamilyMap().at(family).size());

  // add a cell to a different family
  incr.AddColumn("family-2", column, 7l);
  EXPECT_EQ(2, incr.FamilyMap().size());
  EXPECT_EQ(1, incr.FamilyMap().at("family-2").size());
}
