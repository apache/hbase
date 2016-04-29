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
using hbase::pb::ServerName;
using hbase::pb::RegionSpecifier_RegionSpecifierType;
using hbase::pb::GetRequest;
using hbase::pb::GetResponse;

// TODO(eclark): remove the need for this.
DEFINE_string(region, "1588230740", "What region to send a get to");
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
      cache.LocateMeta()
          .then([&cp = cp](ServerName sn) { return cp.get(sn); })
          .then([](shared_ptr<HBaseService> con) {
            // Send the request
            auto r = Request::get();
            // This is a get request so make that
            auto req_msg = static_pointer_cast<GetRequest>(r->req_msg());
            // Set what region
            req_msg->mutable_region()->set_value(FLAGS_region);
            // It's always this.
            req_msg->mutable_region()->set_type(
                RegionSpecifier_RegionSpecifierType::
                    RegionSpecifier_RegionSpecifierType_ENCODED_REGION_NAME);

            // What row.
            req_msg->mutable_get()->set_row(FLAGS_row);

            return (*con)(std::move(r));
          })
          .then([](Response resp) {
            return static_pointer_cast<GetResponse>(resp.response());
          })
          .via(cpu_ex.get())
          .then([](shared_ptr<GetResponse> get_resp) {
            cout << "GetResponse has_result = " << get_resp->has_result()
                 << '\n';
            if (get_resp->has_result()) {
              auto &r = get_resp->result();
              cout << "Result cell_size = " << r.cell_size() << endl;
              for (auto &cell : r.cell()) {
                cout << "\trow = " << cell.row()
                     << " family = " << cell.family()
                     << " qualifier = " << cell.qualifier()
                     << " timestamp = " << cell.timestamp()
                     << " value = " << cell.value() << endl;
              }
              return 0;
            }

            return 1;
          })
          .get(milliseconds(5000));

  return result;
}
