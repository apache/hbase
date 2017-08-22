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

#include <folly/Format.h>
#include <folly/Logging.h>
#include <folly/SocketAddress.h>
#include <folly/String.h>
#include <folly/experimental/TestUtil.h>
#include <folly/io/async/AsyncSocketException.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <boost/thread.hpp>
#include <chrono>

#include "connection/rpc-client.h"
#include "exceptions/exception.h"
#include "if/test.pb.h"
#include "rpc-test-server.h"
#include "security/user.h"
#include "serde/rpc-serde.h"

using namespace wangle;
using namespace folly;
using namespace hbase;
using namespace std::chrono;

DEFINE_int32(port, 0, "test server port");
DEFINE_string(result_format, "RPC {} returned: {}.", "output format of RPC result");
DEFINE_string(fail_ex_format, "Shouldn't get here, exception is expected for RPC {}.",
              "output format of enforcing fail with exception");
DEFINE_string(fail_no_ex_format, "Shouldn't get here, exception is not expected for RPC {}.",
              "output format of enforcing fail without exception");
typedef ServerBootstrap<RpcTestServerSerializePipeline> ServerTestBootstrap;
typedef std::shared_ptr<ServerTestBootstrap> ServerPtr;

class RpcTest : public ::testing::Test {
 public:
  static void SetUpTestCase() { google::InstallFailureSignalHandler(); }
};

std::shared_ptr<Configuration> CreateConf() {
  auto conf = std::make_shared<Configuration>();
  conf->Set(RpcSerde::HBASE_CLIENT_RPC_TEST_MODE, "true");
  return conf;
}

ServerPtr CreateRpcServer() {
  /* create rpc test server */
  auto server = std::make_shared<ServerTestBootstrap>();
  server->childPipeline(std::make_shared<RpcTestServerPipelineFactory>());
  server->bind(FLAGS_port);
  return server;
}

std::shared_ptr<folly::SocketAddress> GetRpcServerAddress(ServerPtr server) {
  auto addr = std::make_shared<folly::SocketAddress>();
  server->getSockets()[0]->getAddress(addr.get());
  return addr;
}

std::shared_ptr<RpcClient> CreateRpcClient(std::shared_ptr<Configuration> conf) {
  auto io_executor = std::make_shared<wangle::IOThreadPoolExecutor>(1);
  auto client = std::make_shared<RpcClient>(io_executor, nullptr, conf);
  return client;
}

std::shared_ptr<RpcClient> CreateRpcClient(std::shared_ptr<Configuration> conf,
                                           std::chrono::nanoseconds connect_timeout) {
  auto io_executor = std::make_shared<wangle::IOThreadPoolExecutor>(1);
  auto client = std::make_shared<RpcClient>(io_executor, nullptr, conf, connect_timeout);
  return client;
}

/**
* test ping
*/
TEST_F(RpcTest, Ping) {
  auto conf = CreateConf();
  auto server = CreateRpcServer();
  auto server_addr = GetRpcServerAddress(server);
  auto client = CreateRpcClient(conf);

  auto method = "ping";
  auto request = std::make_unique<Request>(std::make_shared<EmptyRequestProto>(),
                                           std::make_shared<EmptyResponseProto>(), method);

  /* sending out request */
  client
      ->AsyncCall(server_addr->getAddressStr(), server_addr->getPort(), std::move(request),
                  hbase::security::User::defaultUser())
      .then([&](std::unique_ptr<Response> response) {
        auto pb_resp = std::static_pointer_cast<EmptyResponseProto>(response->resp_msg());
        EXPECT_TRUE(pb_resp != nullptr);
        VLOG(1) << folly::sformat(FLAGS_result_format, method, "");
      })
      .onError([&](const folly::exception_wrapper& ew) {
        FAIL() << folly::sformat(FLAGS_fail_no_ex_format, method);
      }).get();

  server->stop();
  server->join();
}

/**
 * test echo
 */
TEST_F(RpcTest, Echo) {
  auto conf = CreateConf();
  auto server = CreateRpcServer();
  auto server_addr = GetRpcServerAddress(server);
  auto client = CreateRpcClient(conf);

  auto method = "echo";
  auto greetings = "hello, hbase server!";
  auto request = std::make_unique<Request>(std::make_shared<EchoRequestProto>(),
                                           std::make_shared<EchoResponseProto>(), method);
  auto pb_msg = std::static_pointer_cast<EchoRequestProto>(request->req_msg());
  pb_msg->set_message(greetings);

  /* sending out request */
  client
      ->AsyncCall(server_addr->getAddressStr(), server_addr->getPort(), std::move(request),
                  hbase::security::User::defaultUser())
      .then([&](std::unique_ptr<Response> response) {
        auto pb_resp = std::static_pointer_cast<EchoResponseProto>(response->resp_msg());
        EXPECT_TRUE(pb_resp != nullptr);
        VLOG(1) << folly::sformat(FLAGS_result_format, method, pb_resp->message());
        EXPECT_EQ(greetings, pb_resp->message());
      })
      .onError([&](const folly::exception_wrapper& ew) {
        FAIL() << folly::sformat(FLAGS_fail_no_ex_format, method);
      }).get();

  server->stop();
  server->join();
}

/**
 * test error
 */
TEST_F(RpcTest, Error) {
  auto conf = CreateConf();
  auto server = CreateRpcServer();
  auto server_addr = GetRpcServerAddress(server);
  auto client = CreateRpcClient(conf);

  auto method = "error";
  auto request = std::make_unique<Request>(std::make_shared<EmptyRequestProto>(),
                                           std::make_shared<EmptyResponseProto>(), method);
  /* sending out request */
  client
      ->AsyncCall(server_addr->getAddressStr(), server_addr->getPort(), std::move(request),
                  hbase::security::User::defaultUser())
      .then([&](std::unique_ptr<Response> response) {
        FAIL() << folly::sformat(FLAGS_fail_ex_format, method);
      })
      .onError([&](const folly::exception_wrapper& ew) {
        VLOG(1) << folly::sformat(FLAGS_result_format, method, ew.what());
        std::string kRemoteException = demangle(typeid(hbase::RemoteException)).toStdString();
        std::string kRpcTestException = demangle(typeid(hbase::RpcTestException)).toStdString();

        /* verify exception_wrapper */
        EXPECT_TRUE(bool(ew));
        EXPECT_EQ(kRemoteException, ew.class_name());

        /* verify exception */
        EXPECT_TRUE(ew.with_exception([&](const hbase::RemoteException& e) {
          EXPECT_EQ(kRpcTestException, e.exception_class_name());
          EXPECT_EQ(kRpcTestException + ": server error!", e.stack_trace());
        }));
      }).get();

  server->stop();
  server->join();
}

TEST_F(RpcTest, SocketNotOpen) {
  auto conf = CreateConf();
  auto server = CreateRpcServer();
  auto server_addr = GetRpcServerAddress(server);
  auto client = CreateRpcClient(conf);

  auto method = "socketNotOpen";
  auto request = std::make_unique<Request>(std::make_shared<EmptyRequestProto>(),
                                           std::make_shared<EmptyResponseProto>(), method);

  server->stop();
  server->join();

  /* sending out request */
  client
      ->AsyncCall(server_addr->getAddressStr(), server_addr->getPort(), std::move(request),
                  hbase::security::User::defaultUser())
      .then([&](std::unique_ptr<Response> response) {
        FAIL() << folly::sformat(FLAGS_fail_ex_format, method);
      })
      .onError([&](const folly::exception_wrapper& ew) {
        VLOG(1) << folly::sformat(FLAGS_result_format, method, ew.what());
        std::string kConnectionException =
            demangle(typeid(hbase::ConnectionException)).toStdString();
        std::string kAsyncSocketException =
            demangle(typeid(folly::AsyncSocketException)).toStdString();

        /* verify exception_wrapper */
        EXPECT_TRUE(bool(ew));
        EXPECT_EQ(kConnectionException, ew.class_name());

        /* verify exception */
        EXPECT_TRUE(ew.with_exception([&](const hbase::ConnectionException& e) {
          EXPECT_TRUE(bool(e.cause()));
          EXPECT_EQ(kAsyncSocketException, e.cause().class_name());
          VLOG(1) << folly::sformat(FLAGS_result_format, method, e.cause().what());
          e.cause().with_exception([&](const folly::AsyncSocketException& ase) {
            EXPECT_EQ(AsyncSocketException::AsyncSocketExceptionType::NOT_OPEN, ase.getType());
            EXPECT_EQ(111 /*ECONNREFUSED*/, ase.getErrno());
          });
        }));
      }).get();
}

/**
 * test pause
 */
TEST_F(RpcTest, Pause) {
  int ms = 500;

  auto conf = CreateConf();
  auto server = CreateRpcServer();
  auto server_addr = GetRpcServerAddress(server);
  auto client =
      CreateRpcClient(conf, std::chrono::duration_cast<nanoseconds>(milliseconds(2 * ms)));

  auto method = "pause";
  auto request = std::make_unique<Request>(std::make_shared<PauseRequestProto>(),
                                           std::make_shared<EmptyResponseProto>(), method);
  auto pb_msg = std::static_pointer_cast<PauseRequestProto>(request->req_msg());

  pb_msg->set_ms(ms);

  /* sending out request */
  client
      ->AsyncCall(server_addr->getAddressStr(), server_addr->getPort(), std::move(request),
                  hbase::security::User::defaultUser())
      .then([&](std::unique_ptr<Response> response) {
        auto pb_resp = std::static_pointer_cast<EmptyResponseProto>(response->resp_msg());
        EXPECT_TRUE(pb_resp != nullptr);
        VLOG(1) << folly::sformat(FLAGS_result_format, method, "");
      })
      .onError([&](const folly::exception_wrapper& ew) {
        VLOG(1) << folly::sformat(FLAGS_result_format, method, ew.what());
        FAIL() << folly::sformat(FLAGS_fail_no_ex_format, method);
      }).get();

  server->stop();
  server->join();
}
