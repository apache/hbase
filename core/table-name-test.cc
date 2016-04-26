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

#include <folly/Conv.h>
#include <gtest/gtest.h>

#include <string>

#include "core/table-name.h"

using namespace hbase;

TEST(TestTableName, TestToStringNoDefault) {
  TableName tn{"TestTableName"};
  std::string result = folly::to<std::string>(tn);
  ASSERT_EQ(result.find("default"), std::string::npos);
  ASSERT_EQ("TestTableName", result);
}
TEST(TestTableName, TestToStringIncludeNS) {
  TableName tn{"hbase", "acl"};
  std::string result = folly::to<std::string>(tn);
  ASSERT_EQ(result.find("hbase"), 0);
  ASSERT_EQ("hbase:acl", result);
}
TEST(TestTableName, TestIsDefault) {
  TableName default_t1{"in_default"};
  ASSERT_TRUE(default_t1.is_default_name_space());

  TableName default_t2{"default", "in_also"};
  ASSERT_TRUE(default_t2.is_default_name_space());

  TableName non_default{"testing", "hmm"};
  ASSERT_FALSE(non_default.is_default_name_space());
}
