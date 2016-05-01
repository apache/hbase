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

#include <folly/Logging.h>
#include <folly/Random.h>
#include <gflags/gflags.h>
#include <wangle/concurrent/GlobalExecutor.h>

#include <chrono>
#include <iostream>

#include "connection/connection-pool.h"
#include "core/client.h"
#include "core/table-name.h"
#include "if/Client.pb.h"
#include "if/ZooKeeper.pb.h"

using namespace folly;
using namespace std;
using namespace std::chrono;
using hbase::Response;
using hbase::Request;
using hbase::HBaseService;
using hbase::LocationCache;
using hbase::ConnectionPool;
using hbase::TableNameUtil;
using hbase::pb::ServerName;
using hbase::pb::RegionSpecifier_RegionSpecifierType;
using hbase::pb::GetRequest;
using hbase::pb::GetResponse;

// TODO(eclark): remove the need for this.
DEFINE_string(table, "t", "What region to send a get");
DEFINE_string(row, "test", "What row to get");
DEFINE_string(zookeeper, "localhost:2181", "What zk quorum to talk to");

int main(int argc, char *argv[]) {
  google::SetUsageMessage(
      "Simple client to get a single row from HBase on the comamnd line");
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Create a connection factory
  ConnectionPool cp;
  auto cpu_ex = wangle::getCPUExecutor();
  LocationCache cache{FLAGS_zookeeper, cpu_ex};
  auto result =
      cache.locateFromMeta(TableNameUtil::create(FLAGS_table), FLAGS_row)
          .get(milliseconds(5000));

  return 0;
}
