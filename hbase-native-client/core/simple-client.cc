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
#include <glog/logging.h>
#include <wangle/concurrent/GlobalExecutor.h>

#include <iostream>

#include "core/client.h"
#include "core/connection-factory.h"
#include "if/ZooKeeper.pb.h"

using namespace folly;
using namespace std;
using namespace hbase;
using namespace hbase::pb;

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Create a connection factory
  ConnectionFactory cf;

  LocationCache cache{"localhost:2181", wangle::getCPUExecutor()};

  auto result = cache.LocateMeta().get();
  cout << "ServerName = " << result.host_name() << ":" << result.port() << endl;

  // Create a connection to the local host
  auto conn = cf.make_connection(result.host_name(), result.port()).get();

  // Send the request
  Request r;
  conn(r).get();

  return 0;
}
