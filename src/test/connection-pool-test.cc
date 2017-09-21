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
#include <gmock/gmock.h>

#include "hbase/connection/connection-factory.h"
#include "hbase/connection/connection-id.h"
#include "hbase/connection/connection-pool.h"
#include "hbase/if/HBase.pb.h"
#include "hbase/serde/server-name.h"

using hbase::pb::ServerName;
using ::testing::Return;
using ::testing::_;
using hbase::ConnectionFactory;
using hbase::ConnectionPool;
using hbase::ConnectionId;
using hbase::HBaseService;
using hbase::Request;
using hbase::Response;
using hbase::RpcConnection;
using hbase::SerializePipeline;

class MockConnectionFactory : public ConnectionFactory {
 public:
  MockConnectionFactory() : ConnectionFactory(nullptr, nullptr, nullptr, nullptr) {}
  MOCK_METHOD0(MakeBootstrap, std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>>());
  MOCK_METHOD4(Connect, std::shared_ptr<HBaseService>(
                            std::shared_ptr<RpcConnection> rpc_connection,
                            std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>>,
                            const std::string &hostname, uint16_t port));
};

class MockBootstrap : public wangle::ClientBootstrap<SerializePipeline> {};

class MockService : public HBaseService {
 public:
  folly::Future<std::unique_ptr<Response>> operator()(std::unique_ptr<Request> req) override {
    return folly::makeFuture<std::unique_ptr<Response>>(
        std::make_unique<Response>(do_operation(req.get())));
  }
  MOCK_METHOD1(do_operation, Response(Request *));
};

TEST(TestConnectionPool, TestOnlyCreateOnce) {
  auto hostname = std::string{"hostname"};
  auto mock_boot = std::make_shared<MockBootstrap>();
  auto mock_service = std::make_shared<MockService>();
  auto mock_cf = std::make_shared<MockConnectionFactory>();
  uint32_t port{999};

  EXPECT_CALL((*mock_cf), Connect(_, _, _, _)).Times(1).WillRepeatedly(Return(mock_service));
  EXPECT_CALL((*mock_cf), MakeBootstrap()).Times(1).WillRepeatedly(Return(mock_boot));
  EXPECT_CALL((*mock_service), do_operation(_)).Times(1).WillRepeatedly(Return(Response{}));
  ConnectionPool cp{mock_cf};

  auto remote_id = std::make_shared<ConnectionId>(hostname, port);
  auto result = cp.GetConnection(remote_id);
  ASSERT_TRUE(result != nullptr);
  result = cp.GetConnection(remote_id);
  result->SendRequest(nullptr);
}

TEST(TestConnectionPool, TestOnlyCreateMultipleDispose) {
  std::string hostname_one{"hostname"};
  std::string hostname_two{"hostname_two"};
  uint32_t port{999};

  auto mock_boot = std::make_shared<MockBootstrap>();
  auto mock_service = std::make_shared<MockService>();
  auto mock_cf = std::make_shared<MockConnectionFactory>();

  EXPECT_CALL((*mock_cf), Connect(_, _, _, _)).Times(2).WillRepeatedly(Return(mock_service));
  EXPECT_CALL((*mock_cf), MakeBootstrap()).Times(2).WillRepeatedly(Return(mock_boot));
  EXPECT_CALL((*mock_service), do_operation(_)).Times(4).WillRepeatedly(Return(Response{}));
  ConnectionPool cp{mock_cf};

  {
    auto remote_id = std::make_shared<ConnectionId>(hostname_one, port);
    auto result_one = cp.GetConnection(remote_id);
    result_one->SendRequest(nullptr);
    auto remote_id2 = std::make_shared<ConnectionId>(hostname_two, port);
    auto result_two = cp.GetConnection(remote_id2);
    result_two->SendRequest(nullptr);
  }
  auto remote_id = std::make_shared<ConnectionId>(hostname_one, port);
  auto result_one = cp.GetConnection(remote_id);
  result_one->SendRequest(nullptr);
  auto remote_id2 = std::make_shared<ConnectionId>(hostname_two, port);
  auto result_two = cp.GetConnection(remote_id2);
  result_two->SendRequest(nullptr);
}

TEST(TestConnectionPool, TestCreateOneConnectionForOneService) {
  std::string hostname{"hostname"};
  uint32_t port{999};
  std::string service1{"service1"};
  std::string service2{"service2"};

  auto mock_boot = std::make_shared<MockBootstrap>();
  auto mock_service = std::make_shared<MockService>();
  auto mock_cf = std::make_shared<MockConnectionFactory>();

  EXPECT_CALL((*mock_cf), Connect(_, _, _, _)).Times(2).WillRepeatedly(Return(mock_service));
  EXPECT_CALL((*mock_cf), MakeBootstrap()).Times(2).WillRepeatedly(Return(mock_boot));
  EXPECT_CALL((*mock_service), do_operation(_)).Times(4).WillRepeatedly(Return(Response{}));
  ConnectionPool cp{mock_cf};

  {
    auto remote_id = std::make_shared<ConnectionId>(hostname, port, service1);
    auto result_one = cp.GetConnection(remote_id);
    result_one->SendRequest(nullptr);
    auto remote_id2 = std::make_shared<ConnectionId>(hostname, port, service2);
    auto result_two = cp.GetConnection(remote_id2);
    result_two->SendRequest(nullptr);
  }
  auto remote_id = std::make_shared<ConnectionId>(hostname, port, service1);
  auto result_one = cp.GetConnection(remote_id);
  result_one->SendRequest(nullptr);
  auto remote_id2 = std::make_shared<ConnectionId>(hostname, port, service2);
  auto result_two = cp.GetConnection(remote_id2);
  result_two->SendRequest(nullptr);
}
