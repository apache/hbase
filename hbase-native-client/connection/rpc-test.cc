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

#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/channel/Handler.h>

#include <folly/Logging.h>
#include <folly/SocketAddress.h>
#include <folly/String.h>
#include <folly/experimental/TestUtil.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <boost/thread.hpp>

#include "connection/rpc-client.h"
#include "if/test.pb.h"
#include "rpc-test-server.h"
#include "security/user.h"
#include "serde/rpc-serde.h"

using namespace wangle;
using namespace folly;
using namespace hbase;

DEFINE_int32(port, 0, "test server port");

TEST(RpcTestServer, echo) {
  /* create conf */
  auto conf = std::make_shared<Configuration>();
  conf->Set(RpcSerde::HBASE_CLIENT_RPC_TEST_MODE, "true");

  /* create rpc test server */
  auto server = std::make_shared<ServerBootstrap<RpcTestServerSerializePipeline>>();
  server->childPipeline(std::make_shared<RpcTestServerPipelineFactory>());
  server->bind(FLAGS_port);
  folly::SocketAddress server_addr;
  server->getSockets()[0]->getAddress(&server_addr);

  /* create RpcClient */
  auto io_executor = std::make_shared<wangle::IOThreadPoolExecutor>(1);

  auto rpc_client = std::make_shared<RpcClient>(io_executor, nullptr, conf);

  /**
   * test echo
   */
  try {
    std::string greetings = "hello, hbase server!";
    auto request = std::make_unique<Request>(std::make_shared<EchoRequestProto>(),
                                             std::make_shared<EchoResponseProto>(), "echo");
    auto pb_msg = std::static_pointer_cast<EchoRequestProto>(request->req_msg());
    pb_msg->set_message(greetings);

    /* sending out request */
    rpc_client
        ->AsyncCall(server_addr.getAddressStr(), server_addr.getPort(), std::move(request),
                    hbase::security::User::defaultUser())
        .then([=](std::unique_ptr<Response> response) {
          auto pb_resp = std::static_pointer_cast<EchoResponseProto>(response->resp_msg());
          VLOG(1) << "message returned: " + pb_resp->message();
          EXPECT_EQ(greetings, pb_resp->message());
        });
  } catch (const std::exception& e) {
    throw e;
  }

  server->stop();
  server->join();
}
