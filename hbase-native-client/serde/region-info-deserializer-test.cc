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

#include "serde/region-info.h"

#include <gtest/gtest.h>

#include <string>

#include "if/HBase.pb.h"
#include "serde/table-name.h"

using std::string;
using hbase::pb::RegionInfo;
using hbase::pb::TableName;

TEST(TestRegionInfoDesializer, TestDeserialize) {
  string ns{"test_ns"};
  string tn{"table_name"};
  string start_row{"AAAAAA"};
  string stop_row{"BBBBBBBBBBBB"};
  uint64_t region_id = 2345678;

  RegionInfo ri_out;
  ri_out.set_region_id(region_id);
  ri_out.mutable_table_name()->set_namespace_(ns);
  ri_out.mutable_table_name()->set_qualifier(tn);
  ri_out.set_start_key(start_row);
  ri_out.set_end_key(stop_row);

  string header{"PBUF"};
  string ser = header + ri_out.SerializeAsString();

  auto out = folly::to<RegionInfo>(ser);

  EXPECT_EQ(region_id, out.region_id());
}
