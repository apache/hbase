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

#include "core/scan.h"

#include <limits>

#include <gtest/gtest.h>

using namespace hbase;

void CheckFamilies(Scan &scan) {
  EXPECT_EQ(false, scan.HasFamilies());
  scan.AddFamily("family-1");
  EXPECT_EQ(true, scan.HasFamilies());
  EXPECT_EQ(1, scan.Family().size());
  for (const auto &family : scan.Family()) {
    EXPECT_STREQ("family-1", family.first.c_str());
    EXPECT_EQ(0, family.second.size());
  }
  // Not allowed to add the same CF.
  scan.AddFamily("family-1");
  EXPECT_EQ(1, scan.Family().size());
  scan.AddFamily("family-2");
  EXPECT_EQ(2, scan.Family().size());
  scan.AddFamily("family-3");
  EXPECT_EQ(3, scan.Family().size());
  int i = 1;
  for (const auto &family : scan.Family()) {
    std::string family_name = "family-" + std::to_string(i);
    EXPECT_STREQ(family_name.c_str(), family.first.c_str());
    EXPECT_EQ(0, family.second.size());
    i += 1;
  }

  scan.AddColumn("family-1", "column-1");
  scan.AddColumn("family-1", "column-2");
  scan.AddColumn("family-1", "");
  scan.AddColumn("family-1", "column-3");
  scan.AddColumn("family-2", "column-X");

  EXPECT_EQ(3, scan.Family().size());
  auto it = scan.Family().begin();
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
  EXPECT_EQ(it, scan.Family().end());
}

void CheckFamiliesAfterCopy(Scan &scan) {
  EXPECT_EQ(true, scan.HasFamilies());
  EXPECT_EQ(3, scan.Family().size());
  int i = 1;
  for (const auto &family : scan.Family()) {
    std::string family_name = "family-" + std::to_string(i);
    EXPECT_STREQ(family_name.c_str(), family.first.c_str());
    i += 1;
  }
  // Check if the alreaday added CF's and CQ's are as expected
  auto it = scan.Family().begin();
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
  EXPECT_EQ(it, scan.Family().end());
}

void ScanMethods(Scan &scan) {
  scan.SetReversed(true);
  EXPECT_EQ(true, scan.IsReversed());
  scan.SetReversed(false);
  EXPECT_EQ(false, scan.IsReversed());

  std::string start_row("start-row");
  std::string stop_row("stop-row");
  scan.SetStartRow(start_row);
  EXPECT_EQ(start_row, scan.StartRow());

  scan.SetStopRow(stop_row);
  EXPECT_EQ(stop_row, scan.StopRow());

  scan.SetSmall(true);
  EXPECT_EQ(true, scan.IsSmall());
  scan.SetSmall(false);
  EXPECT_EQ(false, scan.IsSmall());

  scan.SetCaching(3);
  EXPECT_EQ(3, scan.Caching());

  scan.SetConsistency(hbase::pb::Consistency::STRONG);
  EXPECT_EQ(hbase::pb::Consistency::STRONG, scan.Consistency());
  scan.SetConsistency(hbase::pb::Consistency::TIMELINE);
  EXPECT_EQ(hbase::pb::Consistency::TIMELINE, scan.Consistency());

  scan.SetCacheBlocks(true);
  EXPECT_EQ(true, scan.CacheBlocks());
  scan.SetCacheBlocks(false);
  EXPECT_EQ(false, scan.CacheBlocks());

  scan.SetAllowPartialResults(true);
  EXPECT_EQ(true, scan.AllowPartialResults());
  scan.SetAllowPartialResults(false);
  EXPECT_EQ(false, scan.AllowPartialResults());

  scan.SetLoadColumnFamiliesOnDemand(true);
  EXPECT_EQ(true, scan.LoadColumnFamiliesOnDemand());
  scan.SetLoadColumnFamiliesOnDemand(false);
  EXPECT_EQ(false, scan.LoadColumnFamiliesOnDemand());

  scan.SetMaxVersions();
  EXPECT_EQ(1, scan.MaxVersions());
  scan.SetMaxVersions(20);
  EXPECT_EQ(20, scan.MaxVersions());

  scan.SetMaxResultSize(1024);
  EXPECT_EQ(1024, scan.MaxResultSize());

  // Test initial values
  EXPECT_EQ(0, scan.Timerange().MinTimeStamp());
  EXPECT_EQ(std::numeric_limits<int64_t>::max(),
            scan.Timerange().MaxTimeStamp());

  // Set & Test new values using TimeRange and TimeStamp
  scan.SetTimeRange(1000, 2000);
  EXPECT_EQ(1000, scan.Timerange().MinTimeStamp());
  EXPECT_EQ(2000, scan.Timerange().MaxTimeStamp());
  scan.SetTimeStamp(0);
  EXPECT_EQ(0, scan.Timerange().MinTimeStamp());
  EXPECT_EQ(1, scan.Timerange().MaxTimeStamp());

  // Test some exceptions
  ASSERT_THROW(scan.SetTimeRange(-1000, 2000), std::runtime_error);
  ASSERT_THROW(scan.SetTimeRange(1000, -2000), std::runtime_error);
  ASSERT_THROW(scan.SetTimeRange(1000, 200), std::runtime_error);
  ASSERT_THROW(scan.SetTimeStamp(std::numeric_limits<int64_t>::max()),
               std::runtime_error);
}

TEST(Scan, Object) {
  Scan scan;
  ScanMethods(scan);
  CheckFamilies(scan);

  // Resetting TimeRange values so that the copy construction and assignment
  // operator tests pass.
  scan.SetTimeRange(0, std::numeric_limits<int64_t>::max());
  Scan scancp(scan);
  ScanMethods(scancp);
  CheckFamiliesAfterCopy(scancp);

  Scan scaneq;
  scaneq = scan;
  ScanMethods(scaneq);
  CheckFamiliesAfterCopy(scaneq);
}

TEST(Scan, WithStartRow) {
  Scan("row-test");
  Scan scan("row-test");
  ScanMethods(scan);
  CheckFamilies(scan);
}

TEST(Scan, WithStartAndStopRow) {
  Scan("start-row", "stop-row");
  Scan scan("start-row", "stop-row");
  ScanMethods(scan);
  CheckFamilies(scan);
}

TEST(Scan, FromGet) {
  std::string row_str = "row-test";
  Get get = Get(row_str);

  get.SetCacheBlocks(true);
  get.SetMaxVersions(5);
  get.AddFamily("family-1");
  get.AddFamily("family-1");
  get.AddFamily("family-2");
  get.AddFamily("family-3");
  get.AddColumn("family-1", "column-1");
  get.AddColumn("family-1", "column-2");
  get.AddColumn("family-1", "");
  get.AddColumn("family-1", "column-3");
  get.AddColumn("family-2", "column-X");

  EXPECT_EQ(3, get.Family().size());

  Scan scan(get);
  ScanMethods(scan);
  CheckFamiliesAfterCopy(scan);
}

TEST(Scan, Exception) {
  std::string row(std::numeric_limits<int16_t>::max() + 1, 'X');
  ASSERT_THROW(Scan tmp(row), std::runtime_error);
  ASSERT_THROW(Scan tmp(""), std::runtime_error);
}
