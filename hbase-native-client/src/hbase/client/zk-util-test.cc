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

#include "hbase/client/zk-util.h"

using hbase::Configuration;
using hbase::ZKUtil;

TEST(ZKUtilTest, ParseZooKeeperQuorum) {
  Configuration conf{};
  conf.Set(ZKUtil::kHBaseZookeeperQuorum_, "s1");
  conf.SetInt(ZKUtil::kHBaseZookeeperClientPort_, 100);

  ASSERT_EQ("s1:100", ZKUtil::ParseZooKeeperQuorum(conf));

  conf.Set(ZKUtil::kHBaseZookeeperQuorum_, "s1:42");

  ASSERT_EQ("s1:42", ZKUtil::ParseZooKeeperQuorum(conf));

  conf.Set(ZKUtil::kHBaseZookeeperQuorum_, "s1,s2,s3");
  ASSERT_EQ("s1:100,s2:100,s3:100", ZKUtil::ParseZooKeeperQuorum(conf));

  conf.Set(ZKUtil::kHBaseZookeeperQuorum_, "s1:42,s2:42,s3:42");
  ASSERT_EQ("s1:42,s2:42,s3:42", ZKUtil::ParseZooKeeperQuorum(conf));
}

TEST(ZKUtilTest, MetaZNode) {
  Configuration conf{};
  ASSERT_EQ("/hbase/meta-region-server", ZKUtil::MetaZNode(conf));

  conf.Set(ZKUtil::kHBaseZnodeParent_, "/hbase-secure");
  ASSERT_EQ("/hbase-secure/meta-region-server", ZKUtil::MetaZNode(conf));
}
