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
#include "core/location-cache.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include <chrono>

#include "core/keyvalue-codec.h"
#include "if/HBase.pb.h"
#include "serde/table-name.h"
#include "test-util/test-util.h"
using namespace hbase;
using namespace std::chrono;

TEST(LocationCacheTest, TestGetMetaNodeContents) {
  TestUtil test_util{};
  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  auto codec = std::make_shared<KeyValueCodec>();
  auto cp = std::make_shared<ConnectionPool>(io, codec);
  LocationCache cache{test_util.conf(), cpu, cp};
  auto f = cache.LocateMeta();
  auto result = f.get();
  ASSERT_FALSE(f.hasException());
  ASSERT_TRUE(result.has_port());
  ASSERT_TRUE(result.has_host_name());
  cpu->stop();
  io->stop();
  cp->Close();
}

TEST(LocationCacheTest, TestGetRegionLocation) {
  TestUtil test_util{};
  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  auto codec = std::make_shared<KeyValueCodec>();
  auto cp = std::make_shared<ConnectionPool>(io, codec);
  LocationCache cache{test_util.conf(), cpu, cp};

  // If there is no table this should throw an exception
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test";
  ASSERT_ANY_THROW(cache.LocateFromMeta(tn, row).get(milliseconds(1000)));
  test_util.RunShellCmd("create 't', 'd'");
  auto loc = cache.LocateFromMeta(tn, row).get(milliseconds(1000));
  ASSERT_TRUE(loc != nullptr);
  cpu->stop();
  io->stop();
  cp->Close();
}

TEST(LocationCacheTest, TestCaching) {
  TestUtil test_util{};
  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  auto codec = std::make_shared<KeyValueCodec>();
  auto cp = std::make_shared<ConnectionPool>(io, codec);
  LocationCache cache{test_util.conf(), cpu, cp};

  auto tn_1 = folly::to<hbase::pb::TableName>("t1");
  auto tn_2 = folly::to<hbase::pb::TableName>("t2");
  auto tn_3 = folly::to<hbase::pb::TableName>("t3");
  auto row_a = "a";

  // test location pulled from meta gets cached
  ASSERT_ANY_THROW(cache.LocateRegion(tn_1, row_a).get(milliseconds(1000)));
  ASSERT_ANY_THROW(cache.LocateFromMeta(tn_1, row_a).get(milliseconds(1000)));

  test_util.RunShellCmd("create 't1', 'd'");

  ASSERT_FALSE(cache.IsLocationCached(tn_1, row_a));
  auto loc = cache.LocateRegion(tn_1, row_a).get(milliseconds(1000));
  ASSERT_TRUE(cache.IsLocationCached(tn_1, row_a));
  ASSERT_EQ(loc, cache.GetCachedLocation(tn_1, row_a));

  // test with two regions
  test_util.RunShellCmd("create 't2', 'd', SPLITS => ['b']");

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
  test_util.RunShellCmd("create 't3', 'd', SPLITS => ['b', 'c']");

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
  cp->Close();
}
