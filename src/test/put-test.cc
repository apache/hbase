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

#include "hbase/client/mutation.h"
#include "hbase/client/put.h"
#include "hbase/utils/time-util.h"

using hbase::Put;
using hbase::Cell;
using hbase::CellType;
using hbase::Mutation;
using hbase::TimeUtil;

const constexpr int64_t Mutation::kLatestTimestamp;

TEST(Put, Row) {
  Put put{"foo"};
  EXPECT_EQ("foo", put.row());
}

TEST(Put, Durability) {
  Put put{"row"};
  EXPECT_EQ(hbase::pb::MutationProto_Durability_USE_DEFAULT, put.Durability());

  auto skipWal = hbase::pb::MutationProto_Durability_SKIP_WAL;
  put.SetDurability(skipWal);
  EXPECT_EQ(skipWal, put.Durability());
}

TEST(Put, Timestamp) {
  Put put{"row"};

  // test default timestamp
  EXPECT_EQ(Mutation::kLatestTimestamp, put.TimeStamp());

  // set custom timestamp
  auto ts = TimeUtil::ToMillis(TimeUtil::GetNowNanos());
  put.SetTimeStamp(ts);
  EXPECT_EQ(ts, put.TimeStamp());

  // Add a column with custom timestamp
  put.AddColumn("f", "q", "v");
  auto &cell = put.FamilyMap().at("f")[0];
  EXPECT_EQ(ts, cell->Timestamp());
}

TEST(Put, HasFamilies) {
  Put put{"row"};

  EXPECT_EQ(false, put.HasFamilies());

  put.AddColumn("f", "q", "v");
  EXPECT_EQ(true, put.HasFamilies());
}

TEST(Put, Add) {
  CellType cell_type = CellType::PUT;
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  std::string value = "value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  auto cell = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);

  // add first cell
  Put put{"row"};
  put.Add(std::move(cell));
  EXPECT_EQ(1, put.FamilyMap().size());
  EXPECT_EQ(1, put.FamilyMap().at(family).size());

  // add a non-matching row
  auto cell2 = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);
  Put put2{"foo"};
  ASSERT_THROW(put2.Add(std::move(cell2)), std::runtime_error);  // rows don't match

  // add a second cell with same family
  auto cell3 = std::make_unique<Cell>(row, family, "column-2", timestamp, value, cell_type);
  put.Add(std::move(cell3));
  EXPECT_EQ(1, put.FamilyMap().size());
  EXPECT_EQ(2, put.FamilyMap().at(family).size());

  // add a cell to a different family
  auto cell4 = std::make_unique<Cell>(row, "family-2", "column-2", timestamp, value, cell_type);
  put.Add(std::move(cell4));
  EXPECT_EQ(2, put.FamilyMap().size());
  EXPECT_EQ(1, put.FamilyMap().at("family-2").size());
}

TEST(Put, AddColumn) {
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  std::string value = "value";

  Put put{"row"};
  put.AddColumn(family, column, value);
  EXPECT_EQ(1, put.FamilyMap().size());
  EXPECT_EQ(1, put.FamilyMap().at(family).size());

  // add a second cell with same family
  put.AddColumn(family, "column-2", value);
  EXPECT_EQ(1, put.FamilyMap().size());
  EXPECT_EQ(2, put.FamilyMap().at(family).size());

  // add a cell to a different family
  put.AddColumn("family-2", column, value);
  EXPECT_EQ(2, put.FamilyMap().size());
  EXPECT_EQ(1, put.FamilyMap().at("family-2").size());

  // use the AddColumn overload
  auto ts = TimeUtil::ToMillis(TimeUtil::GetNowNanos());
  put.AddColumn(family, column, ts, value);
  EXPECT_EQ(2, put.FamilyMap().size());
  EXPECT_EQ(3, put.FamilyMap().at(family).size());
  auto &cell = put.FamilyMap().at(family)[2];
  EXPECT_EQ(ts, cell->Timestamp());
}
