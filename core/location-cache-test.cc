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
#include <folly/Memory.h>
#include <gtest/gtest.h>
#include <wangle/concurrent/GlobalExecutor.h>

#include "location-cache.h"
using namespace hbase;

TEST(LocationCacheTest, TestGetMetaNodeContents) {
  // TODO(elliott): need to make a test utility for this.
  LocationCache cache{"localhost:2181", wangle::getCPUExecutor()};
  auto f = cache.LocateMeta();
  auto result = f.get();
  ASSERT_FALSE(f.hasException());
  ASSERT_TRUE(result.has_port());
}
