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

#include <gtest/gtest.h>

#include "core/append.h"
#include "core/cell.h"
#include "core/client.h"
#include "core/configuration.h"
#include "core/delete.h"
#include "core/get.h"
#include "core/hbase-configuration-loader.h"
#include "core/increment.h"
#include "core/meta-utils.h"
#include "core/put.h"
#include "core/result.h"
#include "core/table.h"
#include "exceptions/exception.h"
#include "serde/table-name.h"
#include "test-util/test-util.h"
#include "utils/bytes-util.h"

using hbase::Cell;
using hbase::Configuration;
using hbase::Get;
using hbase::MetaUtil;
using hbase::RetriesExhaustedException;
using hbase::Put;
using hbase::Table;
using hbase::TestUtil;

using std::chrono_literals::operator""s;

class LocationCacheRetryTest : public ::testing::Test {
 public:
  static std::unique_ptr<hbase::TestUtil> test_util;
  static void SetUpTestCase() {
    google::InstallFailureSignalHandler();
    test_util = std::make_unique<hbase::TestUtil>();
    test_util->StartMiniCluster(2);
    test_util->conf()->SetInt("hbase.client.retries.number", 5);
  }
};

std::unique_ptr<hbase::TestUtil> LocationCacheRetryTest::test_util = nullptr;

TEST_F(LocationCacheRetryTest, GetFromMetaTable) {
  auto tn = folly::to<hbase::pb::TableName>("hbase:meta");
  auto row = "test1";

  hbase::Client client(*LocationCacheRetryTest::test_util->conf());

  // do a get against the other table, but not the actual table "t".
  auto table = client.Table(tn);
  hbase::Get get(row);
  auto result = table->Get(get);

  LocationCacheRetryTest::test_util->MoveRegion(MetaUtil::kMetaRegion, "");

  std::this_thread::sleep_for(3s);  // sleep 3 sec

  result = table->Get(get);
}

TEST_F(LocationCacheRetryTest, PutGet) {
  LocationCacheRetryTest::test_util->CreateTable("t", "d");
  LocationCacheRetryTest::test_util->CreateTable("t2", "d");

  auto tn = folly::to<hbase::pb::TableName>("t");
  auto tn2 = folly::to<hbase::pb::TableName>("t2");
  auto row = "test1";

  hbase::Client client(*LocationCacheRetryTest::test_util->conf());

  // do a get against the other table, but not the actual table "t".
  auto table = client.Table(tn);
  auto table2 = client.Table(tn2);
  hbase::Get get(row);
  auto result = table2->Get(get);

  // we should have already cached the location of meta right now. Now
  // move the meta region to the other server so that we will get a NotServingRegionException
  // when we do the actual location lookup request. If there is no invalidation
  // of the meta's own location, then following put/get will result in retries exhausted.
  LocationCacheRetryTest::test_util->MoveRegion(MetaUtil::kMetaRegion, "");

  std::this_thread::sleep_for(3s);  // sleep 3 sec

  table->Put(Put{row}.AddColumn("d", "1", "value1"));

  result = table->Get(get);

  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ("test1", result->Row());
  EXPECT_EQ("value1", *(result->Value("d", "1")));
}
