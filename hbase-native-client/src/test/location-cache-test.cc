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
#include "hbase/client/location-cache.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include <chrono>

#include "hbase/client/keyvalue-codec.h"
#include "hbase/if/HBase.pb.h"
#include "hbase/serde/table-name.h"
#include "hbase/test-util/test-util.h"

using hbase::Cell;
using hbase::Configuration;
using hbase::ConnectionPool;
using hbase::MetaUtil;
using hbase::LocationCache;
using hbase::TestUtil;
using hbase::KeyValueCodec;
using std::chrono::milliseconds;

class LocationCacheTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    google::InstallFailureSignalHandler();
    test_util_ = std::make_unique<TestUtil>();
    test_util_->StartMiniCluster(2);
  }
  static void TearDownTestCase() { test_util_.release(); }

  virtual void SetUp() {}
  virtual void TearDown() {}

 public:
  static std::unique_ptr<TestUtil> test_util_;
};

std::unique_ptr<TestUtil> LocationCacheTest::test_util_ = nullptr;

TEST_F(LocationCacheTest, TestGetMetaNodeContents) {
  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  auto codec = std::make_shared<KeyValueCodec>();
  auto cp = std::make_shared<ConnectionPool>(io, cpu, codec, LocationCacheTest::test_util_->conf());
  LocationCache cache{LocationCacheTest::test_util_->conf(), io, cpu, cp};
  auto f = cache.LocateMeta();
  auto result = f.get();
  ASSERT_FALSE(f.hasException());
  ASSERT_TRUE(result.has_port());
  ASSERT_TRUE(result.has_host_name());
  cpu->stop();
  io->stop();
}

TEST_F(LocationCacheTest, TestGetRegionLocation) {
  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  auto codec = std::make_shared<KeyValueCodec>();
  auto cp = std::make_shared<ConnectionPool>(io, cpu, codec, LocationCacheTest::test_util_->conf());
  LocationCache cache{LocationCacheTest::test_util_->conf(), io, cpu, cp};

  // If there is no table this should throw an exception
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test";
  ASSERT_ANY_THROW(cache.LocateFromMeta(tn, row).get(milliseconds(1000)));
  LocationCacheTest::test_util_->CreateTable("t", "d");
  auto loc = cache.LocateFromMeta(tn, row).get(milliseconds(1000));
  ASSERT_TRUE(loc != nullptr);
  cpu->stop();
  io->stop();
}

TEST_F(LocationCacheTest, TestCaching) {
  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  auto codec = std::make_shared<KeyValueCodec>();
  auto cp = std::make_shared<ConnectionPool>(io, cpu, codec, LocationCacheTest::test_util_->conf());
  LocationCache cache{LocationCacheTest::test_util_->conf(), io, cpu, cp};

  auto tn_1 = folly::to<hbase::pb::TableName>("t1");
  auto tn_2 = folly::to<hbase::pb::TableName>("t2");
  auto tn_3 = folly::to<hbase::pb::TableName>("t3");
  auto row_a = "a";

  // test location pulled from meta gets cached
  ASSERT_ANY_THROW(cache.LocateRegion(tn_1, row_a).get(milliseconds(1000)));
  ASSERT_ANY_THROW(cache.LocateFromMeta(tn_1, row_a).get(milliseconds(1000)));
  LocationCacheTest::test_util_->CreateTable("t1", "d");

  ASSERT_FALSE(cache.IsLocationCached(tn_1, row_a));
  auto loc = cache.LocateRegion(tn_1, row_a).get(milliseconds(1000));
  ASSERT_TRUE(cache.IsLocationCached(tn_1, row_a));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_1, row_a));

  // test with two regions
  std::vector<std::string> keys;
  keys.push_back("b");
  LocationCacheTest::test_util_->CreateTable("t2", "d", keys);

  ASSERT_FALSE(cache.IsLocationCached(tn_2, "a"));
  loc = cache.LocateRegion(tn_2, "a").get(milliseconds(1000));
  ASSERT_TRUE(cache.IsLocationCached(tn_2, "a"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_2, "a"));

  ASSERT_FALSE(cache.IsLocationCached(tn_2, "b"));
  loc = cache.LocateRegion(tn_2, "b").get(milliseconds(1000));
  ASSERT_TRUE(cache.IsLocationCached(tn_2, "b"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_2, "b"));
  ASSERT_TRUE(cache.IsLocationCached(tn_2, "ba"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_2, "ba"));

  // test with three regions
  keys.clear();
  keys.push_back("b");
  keys.push_back("c");
  LocationCacheTest::test_util_->CreateTable("t3", "d", keys);

  ASSERT_FALSE(cache.IsLocationCached(tn_3, "c"));
  ASSERT_FALSE(cache.IsLocationCached(tn_3, "ca"));
  loc = cache.LocateRegion(tn_3, "ca").get(milliseconds(1000));
  ASSERT_TRUE(cache.IsLocationCached(tn_3, "c"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_3, "c"));
  ASSERT_TRUE(cache.IsLocationCached(tn_3, "ca"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_3, "ca"));

  ASSERT_FALSE(cache.IsLocationCached(tn_3, "b"));
  loc = cache.LocateRegion(tn_3, "b").get(milliseconds(1000));
  ASSERT_TRUE(cache.IsLocationCached(tn_3, "b"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_3, "b"));
  ASSERT_TRUE(cache.IsLocationCached(tn_3, "ba"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_3, "ba"));

  // clear second region
  cache.ClearCachedLocation(tn_3, "b");
  ASSERT_FALSE(cache.IsLocationCached(tn_3, "b"));

  ASSERT_FALSE(cache.IsLocationCached(tn_3, "a"));
  loc = cache.LocateRegion(tn_3, "a").get(milliseconds(1000));
  ASSERT_TRUE(cache.IsLocationCached(tn_3, "a"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_3, "a"));
  ASSERT_TRUE(cache.IsLocationCached(tn_3, "abc"));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_3, "abc"));

  cpu->stop();
  io->stop();
}
