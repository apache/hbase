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

#include "core/request_converter.h"

#include <gtest/gtest.h>
#include <limits>
#include "connection/request.h"
#include "core/get.h"
#include "core/scan.h"

using hbase::Get;
using hbase::Scan;

using hbase::pb::GetRequest;
using hbase::pb::RegionSpecifier;
using hbase::pb::RegionSpecifier_RegionSpecifierType;
using hbase::pb::ScanRequest;

TEST(RequestConverter, ToGet) {
  std::string row_str = "row-test";
  Get get(row_str);
  get.AddFamily("family-1");
  get.AddFamily("family-2");
  get.AddFamily("family-3");
  get.AddColumn("family-2", "qualifier-1");
  get.AddColumn("family-2", "qualifier-2");
  get.AddColumn("family-2", "qualifier-3");
  get.SetCacheBlocks(false);
  get.SetConsistency(hbase::pb::Consistency::TIMELINE);
  get.SetMaxVersions(2);
  get.SetTimeRange(10000, 20000);
  std::string region_name("RegionName");

  auto req = hbase::RequestConverter::ToGetRequest(get, region_name);
  auto msg = std::static_pointer_cast<GetRequest>(req->req_msg());

  // Tests whether the PB object is properly set or not.
  ASSERT_TRUE(msg->has_region());
  ASSERT_TRUE(msg->region().has_value());
  EXPECT_EQ(msg->region().value(), region_name);

  ASSERT_TRUE(msg->has_get());
  EXPECT_EQ(msg->get().row(), row_str);
  EXPECT_FALSE(msg->get().cache_blocks());
  EXPECT_EQ(msg->get().consistency(), hbase::pb::Consistency::TIMELINE);
  EXPECT_EQ(msg->get().max_versions(), 2);
  EXPECT_EQ(msg->get().column_size(), 3);
  for (int i = 0; i < msg->get().column_size(); ++i) {
    EXPECT_EQ(msg->get().column(i).family(), "family-" + std::to_string(i + 1));
    for (int j = 0; j < msg->get().column(i).qualifier_size(); ++j) {
      EXPECT_EQ(msg->get().column(i).qualifier(j), "qualifier-" + std::to_string(j + 1));
    }
  }
}

TEST(RequestConverter, ToScan) {
  std::string start_row("start-row");
  std::string stop_row("stop-row");
  hbase::Scan scan;
  scan.AddFamily("family-1");
  scan.AddFamily("family-2");
  scan.AddFamily("family-3");
  scan.AddColumn("family-2", "qualifier-1");
  scan.AddColumn("family-2", "qualifier-2");
  scan.AddColumn("family-2", "qualifier-3");
  scan.SetReversed(true);
  scan.SetStartRow(start_row);
  scan.SetStopRow(stop_row);
  scan.SetSmall(true);
  scan.SetCaching(3);
  scan.SetConsistency(hbase::pb::Consistency::TIMELINE);
  scan.SetCacheBlocks(true);
  scan.SetAllowPartialResults(true);
  scan.SetLoadColumnFamiliesOnDemand(true);
  scan.SetMaxVersions(5);
  scan.SetTimeRange(10000, 20000);
  std::string region_name("RegionName");

  auto req = hbase::RequestConverter::ToScanRequest(scan, region_name);
  auto msg = std::static_pointer_cast<ScanRequest>(req->req_msg());

  // Tests whether the PB object is properly set or not.
  ASSERT_TRUE(msg->has_region());
  ASSERT_TRUE(msg->region().has_value());
  EXPECT_EQ(msg->region().value(), region_name);

  ASSERT_TRUE(msg->has_scan());
  EXPECT_TRUE(msg->scan().reversed());
  EXPECT_EQ(msg->scan().start_row(), start_row);
  EXPECT_EQ(msg->scan().stop_row(), stop_row);
  EXPECT_TRUE(msg->scan().small());
  EXPECT_EQ(msg->scan().caching(), 3);
  EXPECT_EQ(msg->scan().consistency(), hbase::pb::Consistency::TIMELINE);
  EXPECT_TRUE(msg->scan().cache_blocks());
  EXPECT_TRUE(msg->scan().allow_partial_results());
  EXPECT_TRUE(msg->scan().load_column_families_on_demand());
  EXPECT_EQ(msg->scan().max_versions(), 5);
  EXPECT_EQ(msg->scan().max_result_size(), std::numeric_limits<uint64_t>::max());

  EXPECT_EQ(msg->scan().column_size(), 3);
  for (int i = 0; i < msg->scan().column_size(); ++i) {
    EXPECT_EQ(msg->scan().column(i).family(), "family-" + std::to_string(i + 1));
    for (int j = 0; j < msg->scan().column(i).qualifier_size(); ++j) {
      EXPECT_EQ(msg->scan().column(i).qualifier(j), "qualifier-" + std::to_string(j + 1));
    }
  }
  ASSERT_FALSE(msg->client_handles_partials());
  ASSERT_FALSE(msg->client_handles_heartbeats());
  ASSERT_FALSE(msg->track_scan_metrics());
}
