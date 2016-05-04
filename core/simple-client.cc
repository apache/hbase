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
#include <folly/futures/Future.h>
#include <gflags/gflags.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/GlobalExecutor.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "connection/connection-pool.h"
#include "core/client.h"
#include "if/Client.pb.h"
#include "if/ZooKeeper.pb.h"
#include "serde/server-name.h"
#include "serde/table-name.h"

using namespace folly;
using namespace std;
using namespace std::chrono;
using hbase::Response;
using hbase::Request;
using hbase::HBaseService;
using hbase::LocationCache;
using hbase::ConnectionPool;
using hbase::ConnectionFactory;
using hbase::pb::TableName;
using hbase::pb::ServerName;
using hbase::pb::RegionSpecifier_RegionSpecifierType;
using hbase::pb::MutateRequest;
using hbase::pb::MutationProto_MutationType;

// TODO(eclark): remove the need for this.
DEFINE_string(table, "t", "What region to send a get");
DEFINE_string(row, "test", "What row to get");
DEFINE_string(zookeeper, "localhost:2181", "What zk quorum to talk to");
DEFINE_uint64(columns, 10000, "How many columns to write");
DEFINE_int32(threads, 6, "How many cpu threads");

std::unique_ptr<Request> MakeRequest(uint64_t col, std::string region_name) {
  auto req = Request::mutate();
  auto msg = std::static_pointer_cast<MutateRequest>(req->req_msg());
  auto region = msg->mutable_region();
  auto suf = folly::to<std::string>(col);

  region->set_value(region_name);
  region->set_type(RegionSpecifier_RegionSpecifierType::
                       RegionSpecifier_RegionSpecifierType_REGION_NAME);
  auto mutation = msg->mutable_mutation();
  mutation->set_row(FLAGS_row + suf);
  mutation->set_mutate_type(
      MutationProto_MutationType::MutationProto_MutationType_PUT);
  auto column = mutation->add_column_value();
  column->set_family("d");
  auto qual = column->add_qualifier_value();
  qual->set_qualifier(suf);
  qual->set_value(".");

  return std::move(req);
}

int main(int argc, char *argv[]) {
  google::SetUsageMessage(
      "Simple client to get a single row from HBase on the comamnd line");
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Set up thread pools.
  auto cpu_pool =
      std::make_shared<wangle::CPUThreadPoolExecutor>(FLAGS_threads);
  wangle::setCPUExecutor(cpu_pool);
  auto io_pool = std::make_shared<wangle::IOThreadPoolExecutor>(5);
  wangle::setIOExecutor(io_pool);

  // Create the cache.
  LocationCache cache{FLAGS_zookeeper, cpu_pool};

  auto row = FLAGS_row;
  auto tn = folly::to<TableName>(FLAGS_table);

  auto loc = cache.LocateFromMeta(tn, row).get(milliseconds(5000));
  auto connection = loc->service();

  auto num_puts = FLAGS_columns;

  auto results = std::vector<Future<Response>>{};
  uint64_t col{0};
  for (; col < num_puts; col++) {
    results.push_back(folly::makeFuture(col)
                          .via(cpu_pool.get())
                          .then([loc](uint64_t col) {
                            return MakeRequest(col, loc->region_name());
                          })
                          .then([connection](std::unique_ptr<Request> req) {
                            return (*connection)(std::move(req));
                          }));
  }
  auto allf = folly::collect(results).get();

  LOG(ERROR) << "Successfully sent  " << allf.size() << " requests.";

  io_pool->stop();

  return 0;
}
