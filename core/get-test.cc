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

#include <glog/logging.h>
#include <gtest/gtest.h>
using namespace hbase;
const int NUMBER_OF_GETS = 5;

void CheckFamilies(Get &get) {
  EXPECT_EQ(false, get.HasFamilies());
  get.AddFamily("family-1");
  EXPECT_EQ(true, get.HasFamilies());
  EXPECT_EQ(1, get.Family().size());
  for (const auto &family : get.Family()) {
    EXPECT_STREQ("family-1", family.first.c_str());
    EXPECT_EQ(0, family.second.size());
  }
  // Not allowed to add the same CF.
  get.AddFamily("family-1");
  EXPECT_EQ(1, get.Family().size());
  get.AddFamily("family-2");
  EXPECT_EQ(2, get.Family().size());
  get.AddFamily("family-3");
  EXPECT_EQ(3, get.Family().size());
  int i = 1;
  for (const auto &family : get.Family()) {
    std::string family_name = "family-" + std::to_string(i);
    EXPECT_STREQ(family_name.c_str(), family.first.c_str());
    EXPECT_EQ(0, family.second.size());
    i += 1;
  }

  get.AddColumn("family-1", "column-1");
  get.AddColumn("family-1", "column-2");
  get.AddColumn("family-1", "");
  get.AddColumn("family-1", "column-3");
  get.AddColumn("family-2", "column-X");

  EXPECT_EQ(3, get.Family().size());
  auto it = get.Family().begin();
  EXPECT_STREQ("family-1", it->first.c_str());
  EXPECT_EQ(4, it->second.size());
  EXPECT_STREQ("column-1", it->second[0].c_str());
  EXPECT_STREQ("column-2", it->second[1].c_str());
  EXPECT_STREQ("", it->second[2].c_str());
  EXPECT_STREQ("column-3", it->second[3].c_str());
  ++it;
  EXPECT_STREQ("family-2", it->first.c_str());
  EXPECT_EQ(1, it->second.size());
  EXPECT_STREQ("column-X", it->second[0].c_str());
  ++it;
  EXPECT_STREQ("family-3", it->first.c_str());
  EXPECT_EQ(0, it->second.size());
  ++it;
  EXPECT_EQ(it, get.Family().end());
}

void CheckFamiliesAfterCopy(Get &get) {
  EXPECT_EQ(true, get.HasFamilies());
  EXPECT_EQ(3, get.Family().size());
  int i = 1;
  for (const auto &family : get.Family()) {
    std::string family_name = "family-" + std::to_string(i);
    EXPECT_STREQ(family_name.c_str(), family.first.c_str());
    i += 1;
  }
  // Check if the alreaday added CF's and CQ's are as expected
  auto it = get.Family().begin();
  EXPECT_STREQ("family-1", it->first.c_str());
  EXPECT_EQ(4, it->second.size());
  EXPECT_STREQ("column-1", it->second[0].c_str());
  EXPECT_STREQ("column-2", it->second[1].c_str());
  EXPECT_STREQ("", it->second[2].c_str());
  EXPECT_STREQ("column-3", it->second[3].c_str());
  ++it;
  EXPECT_STREQ("family-2", it->first.c_str());
  EXPECT_EQ(1, it->second.size());
  EXPECT_STREQ("column-X", it->second[0].c_str());
  ++it;
  EXPECT_STREQ("family-3", it->first.c_str());
  EXPECT_EQ(0, it->second.size());
  ++it;
  EXPECT_EQ(it, get.Family().end());
}

void GetMethods(Get &get, const std::string &row) {
  EXPECT_EQ(row, get.Row());

  CheckFamilies(get);
  EXPECT_EQ(true, get.CacheBlocks());
  get.SetCacheBlocks(false);
  EXPECT_EQ(false, get.CacheBlocks());

  EXPECT_EQ(hbase::pb::Consistency::STRONG, get.Consistency());
  get.SetConsistency(hbase::pb::Consistency::TIMELINE);
  EXPECT_EQ(hbase::pb::Consistency::TIMELINE, get.Consistency());

  EXPECT_EQ(1, get.MaxVersions());
  get.SetMaxVersions(2);
  EXPECT_EQ(2, get.MaxVersions());
  get.SetMaxVersions();
  EXPECT_EQ(1, get.MaxVersions());

  // Test initial values
  EXPECT_EQ(0, get.Timerange().MinTimeStamp());
  EXPECT_EQ(std::numeric_limits<int64_t>::max(),
            get.Timerange().MaxTimeStamp());

  // Set & Test new values using TimeRange and TimeStamp
  get.SetTimeRange(1000, 2000);
  EXPECT_EQ(1000, get.Timerange().MinTimeStamp());
  EXPECT_EQ(2000, get.Timerange().MaxTimeStamp());
  get.SetTimeStamp(0);
  EXPECT_EQ(0, get.Timerange().MinTimeStamp());
  EXPECT_EQ(1, get.Timerange().MaxTimeStamp());

  // Test some exceptions
  ASSERT_THROW(get.SetTimeRange(-1000, 2000), std::runtime_error);
  ASSERT_THROW(get.SetTimeRange(1000, -2000), std::runtime_error);
  ASSERT_THROW(get.SetTimeRange(1000, 200), std::runtime_error);
  ASSERT_THROW(get.SetTimeStamp(std::numeric_limits<int64_t>::max()),
               std::runtime_error);

  // Test some exceptions
  ASSERT_THROW(get.SetMaxVersions(0), std::runtime_error);
  ASSERT_THROW(get.SetMaxVersions(std::numeric_limits<uint32_t>::max() + 1),
               std::runtime_error);
}

TEST(Get, SingleGet) {
  std::string row_str = "row-test";
  Get get(row_str);
  GetMethods(get, row_str);

  Get get_tmp(row_str);
  Get getcp(get_tmp);
  GetMethods(getcp, row_str);

  Get geteq("test");
  geteq = get_tmp;
  GetMethods(geteq, row_str);

  // Adding the below tests as there were some concerns raised that the same
  // vector of qualifiers in FamilyMap is being shared between copied objects
  // Verify the source object's family map size before using it to copy.
  EXPECT_EQ(3, get.Family().size());

  Get getcp_fam(get);
  // address of family maps should be different.
  EXPECT_NE(&(get.Family()), &(getcp_fam.Family()));

  // Add family to the source object
  get.AddColumn("family-4", "column-A");
  get.AddColumn("family-4", "column-B");
  // Verify the source object's family map size
  EXPECT_EQ(4, get.Family().size());
  // Verify the source object's family elements
  auto it = get.Family().begin();
  EXPECT_STREQ("family-1", it->first.c_str());
  EXPECT_EQ(4, it->second.size());
  EXPECT_STREQ("column-1", it->second[0].c_str());
  EXPECT_STREQ("column-2", it->second[1].c_str());
  EXPECT_STREQ("", it->second[2].c_str());
  EXPECT_STREQ("column-3", it->second[3].c_str());
  ++it;
  EXPECT_STREQ("family-2", it->first.c_str());
  EXPECT_EQ(1, it->second.size());
  EXPECT_STREQ("column-X", it->second[0].c_str());
  ++it;
  EXPECT_STREQ("family-3", it->first.c_str());
  EXPECT_EQ(0, it->second.size());
  ++it;
  EXPECT_STREQ("family-4", it->first.c_str());
  EXPECT_EQ(2, it->second.size());
  EXPECT_STREQ("column-A", it->second[0].c_str());
  EXPECT_STREQ("column-B", it->second[1].c_str());
  ++it;
  EXPECT_EQ(it, get.Family().end());

  // Verifying the copied object's families. It will remain unchanged and below
  // tests should pass
  CheckFamiliesAfterCopy(getcp_fam);
}

TEST(Get, MultiGet) {
  std::vector<Get *> gets;
  for (int i = 0; i < NUMBER_OF_GETS; i++) {
    std::string row_str = "row-test";
    row_str += std::to_string(i);
    Get *get = new Get(row_str);

    GetMethods(*get, row_str);
    gets.push_back(get);
  }
  EXPECT_EQ(NUMBER_OF_GETS, gets.size());

  for (const auto &get : gets) {
    delete get;
  }
  gets.clear();
}

TEST(Get, Exception) {
  std::string row(std::numeric_limits<int16_t>::max() + 1, 'X');
  ASSERT_THROW(Get tmp = Get(row), std::runtime_error);
  ASSERT_THROW(Get tmp = Get(""), std::runtime_error);
}
