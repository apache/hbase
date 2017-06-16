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
#pragma once

#include <cstdlib>
#include <string>
#include "core/configuration.h"

namespace hbase {

class ZKUtil {
 public:
  static constexpr const char* kHBaseZookeeperQuorum_ = "hbase.zookeeper.quorum";
  static constexpr const char* kDefHBaseZookeeperQuorum_ = "localhost:2181";
  static constexpr const char* kHBaseZookeeperClientPort_ = "hbase.zookeeper.property.clientPort";
  static constexpr const int32_t kDefHBaseZookeeperClientPort_ = 2181;
  static constexpr const char* kHBaseZnodeParent_ = "zookeeper.znode.parent";
  static constexpr const char* kDefHBaseZnodeParent_ = "/hbase";
  static constexpr const char* kHBaseMetaRegionServer_ = "meta-region-server";

  static constexpr const char* kHBaseZookeeperSessionTimeout_ = "zookeeper.session.timeout";
  static constexpr const int32_t kDefHBaseZookeeperSessionTimeout_ = 90000;

  static std::string ParseZooKeeperQuorum(const hbase::Configuration& conf);

  static std::string MetaZNode(const hbase::Configuration& conf);

  static int32_t SessionTimeout(const hbase::Configuration& conf);
};
}  // namespace hbase
