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

#include "serde/server-name.h"

#include <gtest/gtest.h>
#include <string>

using hbase::pb::ServerName;

TEST(TestServerName, TestMakeServerName) {
  auto sn = folly::to<ServerName>("test:123");

  ASSERT_EQ("test", sn.host_name());
  ASSERT_EQ(123, sn.port());
}

TEST(TestServerName, TestIps) {
  auto sn = folly::to<ServerName>("127.0.0.1:999");
  ASSERT_EQ("127.0.0.1", sn.host_name());
  ASSERT_EQ(999, sn.port());
}

TEST(TestServerName, TestThrow) {
  ASSERT_ANY_THROW(folly::to<ServerName>("Ther's no colon here"));
}

TEST(TestServerName, TestIPV6) {
  auto sn = folly::to<ServerName>("[::::1]:123");

  ASSERT_EQ("[::::1]", sn.host_name());
  ASSERT_EQ(123, sn.port());
}
