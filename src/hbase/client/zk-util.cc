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

#include "hbase/client/zk-util.h"

#include <folly/Conv.h>
#include <boost/algorithm/string.hpp>

#include <vector>

namespace hbase {

/**
 * Returns a "proper" zookeeper quorum string, from hbase's broken quorum string formats. In
 * hbase.zookeeper.quorum, the ports are not listed explicitly per server (eg. s1,s2,s3),
 * however ZooKeeper expects the string of the format s1:2181,s2:2181,s3:2181. This code
 * appends the "clientPort" to each node in the quorum string if not there.
 */
std::string ZKUtil::ParseZooKeeperQuorum(const hbase::Configuration& conf) {
  auto zk_quorum = conf.Get(kHBaseZookeeperQuorum_, kDefHBaseZookeeperQuorum_);
  auto zk_port = conf.GetInt(kHBaseZookeeperClientPort_, kDefHBaseZookeeperClientPort_);

  std::vector<std::string> zk_quorum_parts;
  boost::split(zk_quorum_parts, zk_quorum, boost::is_any_of(","), boost::token_compress_on);
  std::vector<std::string> servers;
  for (auto server : zk_quorum_parts) {
    if (boost::contains(server, ":")) {
      servers.push_back(server);
    } else {
      servers.push_back(server + ":" + folly::to<std::string>(zk_port));
    }
  }
  return boost::join(servers, ",");
}

std::string ZKUtil::MetaZNode(const hbase::Configuration& conf) {
  std::string zk_node = conf.Get(kHBaseZnodeParent_, kDefHBaseZnodeParent_) + "/";
  zk_node += kHBaseMetaRegionServer_;
  return zk_node;
}

int32_t ZKUtil::SessionTimeout(const hbase::Configuration& conf) {
  return conf.GetInt(kHBaseZookeeperSessionTimeout_, kDefHBaseZookeeperSessionTimeout_);
}

}  // namespace hbase
