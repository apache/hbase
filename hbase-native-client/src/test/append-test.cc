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

#include "hbase/client/append.h"
#include "hbase/client/mutation.h"
#include "hbase/utils/time-util.h"

using hbase::Append;
using hbase::Cell;
using hbase::CellType;
using hbase::Mutation;
using hbase::TimeUtil;

const constexpr int64_t Mutation::kLatestTimestamp;

TEST(Append, Row) {
  Append append{"foo"};
  EXPECT_EQ("foo", append.row());
}

TEST(Append, Durability) {
  Append append{"row"};
  EXPECT_EQ(hbase::pb::MutationProto_Durability_USE_DEFAULT, append.Durability());

  auto skipWal = hbase::pb::MutationProto_Durability_SKIP_WAL;
  append.SetDurability(skipWal);
  EXPECT_EQ(skipWal, append.Durability());
}

TEST(Append, Timestamp) {
  Append append{"row"};

  // test default timestamp
  EXPECT_EQ(Mutation::kLatestTimestamp, append.TimeStamp());

  // set custom timestamp
  auto ts = TimeUtil::ToMillis(TimeUtil::GetNowNanos());
  append.SetTimeStamp(ts);
  EXPECT_EQ(ts, append.TimeStamp());

  // Add a column with custom timestamp
  append.Add("f", "q", "v");
  auto &cell = append.FamilyMap().at("f")[0];
  EXPECT_EQ(ts, cell->Timestamp());
}

TEST(Append, HasFamilies) {
  Append append{"row"};

  EXPECT_EQ(false, append.HasFamilies());

  append.Add("f", "q", "v");
  EXPECT_EQ(true, append.HasFamilies());
}

TEST(Append, Add) {
  CellType cell_type = CellType::PUT;
  std::string row = "row";
  std::string family = "family";
  std::string column = "column";
  std::string value = "value";
  int64_t timestamp = std::numeric_limits<int64_t>::max();
  auto cell = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);

  // add first cell
  Append append{"row"};
  append.Add(std::move(cell));
  EXPECT_EQ(1, append.FamilyMap().size());
  EXPECT_EQ(1, append.FamilyMap().at(family).size());

  // add a non-matching row
  auto cell2 = std::make_unique<Cell>(row, family, column, timestamp, value, cell_type);
  Append append2{"foo"};
  ASSERT_THROW(append2.Add(std::move(cell2)), std::runtime_error);  // rows don't match

  // add a second cell with same family
  auto cell3 = std::make_unique<Cell>(row, family, "column-2", timestamp, value, cell_type);
  append.Add(std::move(cell3));
  EXPECT_EQ(1, append.FamilyMap().size());
  EXPECT_EQ(2, append.FamilyMap().at(family).size());

  // add a cell to a different family
  auto cell4 = std::make_unique<Cell>(row, "family-2", "column-2", timestamp, value, cell_type);
  append.Add(std::move(cell4));
  EXPECT_EQ(2, append.FamilyMap().size());
  EXPECT_EQ(1, append.FamilyMap().at("family-2").size());
}
