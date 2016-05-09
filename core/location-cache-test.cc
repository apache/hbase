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

#include "if/HBase.pb.h"
#include "serde/table-name.h"
#include "test-util/test-util.h"
using namespace hbase;
using namespace std::chrono;

TEST(LocationCacheTest, TestGetMetaNodeContents) {
  TestUtil test_util{};

  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  LocationCache cache{"localhost:2181", cpu, io};
  auto f = cache.LocateMeta();
  auto result = f.get();
  ASSERT_FALSE(f.hasException());
  ASSERT_TRUE(result.has_port());
  ASSERT_TRUE(result.has_host_name());
}

TEST(LocationCacheTest, TestGetRegionLocation) {
  TestUtil test_util{};
  auto cpu = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  LocationCache cache{"localhost:2181", cpu, io};

  // If there is no table this should throw an exception
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test";
  ASSERT_ANY_THROW(cache.LocateFromMeta(tn, row).get(milliseconds(1000)));
  test_util.RunShellCmd("create 't', 'd'");
  auto loc = cache.LocateFromMeta(tn, row).get(milliseconds(1000));
  ASSERT_TRUE(loc != nullptr);
  cpu->stop();
  io->stop();
}
