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

#include "core/get.h"

#include <gtest/gtest.h>
#include <glog/logging.h>

using namespace hbase;
const int NUMBER_OF_GETS = 5;

TEST (Get, SingleGet) {

  std::string row_str = "row-test";
  ASSERT_NO_THROW(Get tmp = Get(row_str));
  Get get = Get(row_str);

  get.SetCacheBlocks(true);
  get.SetConsistency(hbase::pb::Consistency::STRONG);
  get.SetMaxResultsPerColumnFamily(1);

  ASSERT_THROW(get.SetMaxVersions(0), std::runtime_error);
  ASSERT_NO_THROW(get.SetMaxVersions(-10));
  ASSERT_THROW(get.SetMaxVersions(std::numeric_limits<unsigned int>::max() + 1),
               std::runtime_error);

  ASSERT_THROW(get.SetTimeRange(-100, 2000), std::runtime_error);
  ASSERT_THROW(get.SetTimeRange(100, -2000), std::runtime_error);
  ASSERT_THROW(get.SetTimeRange(1000, 200), std::runtime_error);

  ASSERT_NO_THROW(get.SetMaxVersions());
  ASSERT_NO_THROW(get.SetMaxVersions(2));
  ASSERT_NO_THROW(get.SetTimeRange(0, std::numeric_limits<long>::max()));

  EXPECT_EQ(true, get.CacheBlocks());
  EXPECT_EQ(hbase::pb::Consistency::STRONG, get.Consistency());
  EXPECT_EQ(1, get.MaxResultsPerColumnFamily());
  EXPECT_EQ(2, get.MaxVersions());

  TimeRange tr = get.Timerange();
  EXPECT_EQ(0, tr.MinTimeStamp());
  EXPECT_EQ(std::numeric_limits<long>::max(), tr.MaxTimeStamp());

  EXPECT_EQ("row-test", get.Row());

  EXPECT_EQ(false, get.HasFamilies());

  get.AddFamily("family-1");
  EXPECT_EQ(true, get.HasFamilies());

  get.AddColumn("family-1", "column-1");
  get.AddColumn("family-1", "column-2");
  get.AddColumn("family-1", "");
  get.AddColumn("family-1", "column-3");
  get.AddFamily("family-1");
  get.AddFamily("family-2");
  get.AddFamily("family-3");

}

TEST (Get, MultiGet) {

  std::vector<Get *> gets;
  for (int i = 0; i < NUMBER_OF_GETS; i++) {
    std::string row_str = "row-test";
    row_str += std::to_string(i);
    ASSERT_NO_THROW(Get tmp = Get(row_str));
    Get *get = new Get(row_str);

    get->SetCacheBlocks(true);
    get->SetConsistency(hbase::pb::Consistency::STRONG);
    get->SetMaxResultsPerColumnFamily(1);

    ASSERT_THROW(get->SetMaxVersions(0), std::runtime_error);
    ASSERT_NO_THROW(get->SetMaxVersions(-10));
    ASSERT_THROW(
        get->SetMaxVersions(std::numeric_limits<unsigned int>::max() + 1),
        std::runtime_error);

    ASSERT_THROW(get->SetTimeRange(-100, 2000), std::runtime_error);
    ASSERT_THROW(get->SetTimeRange(100, -2000), std::runtime_error);
    ASSERT_THROW(get->SetTimeRange(1000, 200), std::runtime_error);

    get->SetMaxVersions();
    get->SetMaxVersions(2);
    get->SetTimeRange(0, std::numeric_limits<long>::max());

    EXPECT_EQ(true, get->CacheBlocks());
    EXPECT_EQ(hbase::pb::Consistency::STRONG, get->Consistency());
    EXPECT_EQ(1, get->MaxResultsPerColumnFamily());
    EXPECT_EQ(2, get->MaxVersions());

    TimeRange tr = get->Timerange();
    EXPECT_EQ(0, tr.MinTimeStamp());
    EXPECT_EQ(std::numeric_limits<long>::max(), tr.MaxTimeStamp());

    EXPECT_EQ(false, get->HasFamilies());

    get->AddFamily("family-1");
    EXPECT_EQ(true, get->HasFamilies());

    get->AddColumn("family-1", "column-1");
    get->AddColumn("family-1", "column-2");
    get->AddColumn("family-1", "");
    get->AddColumn("family-1", "column-3");
    get->AddFamily("family-1");
    get->AddFamily("family-2");
    get->AddFamily("family-3");

    gets.push_back(get);
  }
  EXPECT_EQ(NUMBER_OF_GETS, gets.size());

  int i = 0;
  for (const auto &get : gets) {
    std::string row_str = "row-test";
    row_str += std::to_string(i);
    EXPECT_EQ(row_str, get->Row());
    i++;
  }

  for (const auto &get : gets) {
    delete get;
  }
  gets.clear();

}

TEST (Get, Exception) {

  std::string row(std::numeric_limits<short>::max() + 1, 'X');
  ASSERT_THROW(Get tmp = Get(row), std::runtime_error);
  ASSERT_THROW(Get tmp = Get(""), std::runtime_error);
}
