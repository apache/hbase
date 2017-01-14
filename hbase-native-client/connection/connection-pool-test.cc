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

#include "connection/connection-pool.h"
#include "connection/connection-factory.h"
#include "connection/connection-id.h"

#include "if/HBase.pb.h"
#include "serde/server-name.h"

#include <folly/Logging.h>
#include <gmock/gmock.h>

using namespace hbase;

using hbase::pb::ServerName;
using ::testing::Return;
using ::testing::_;
using hbase::ConnectionId;

class MockConnectionFactory : public ConnectionFactory {
 public:
  MockConnectionFactory() : ConnectionFactory(nullptr) {}
  MOCK_METHOD0(MakeBootstrap, std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>>());
  MOCK_METHOD3(Connect, std::shared_ptr<HBaseService>(
                            std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>>,
                            const std::string &hostname, int port));
};

class MockBootstrap : public wangle::ClientBootstrap<SerializePipeline> {};

class MockServiceBase : public HBaseService {
 public:
  folly::Future<Response> operator()(std::unique_ptr<Request> req) override {
    return do_operation(req.get());
  }
  virtual folly::Future<Response> do_operation(Request *req) {
    return folly::makeFuture<Response>(Response{});
  }
};

class MockService : public MockServiceBase {
 public:
  MOCK_METHOD1(do_operation, folly::Future<Response>(Request *));
};

TEST(TestConnectionPool, TestOnlyCreateOnce) {
  auto hostname = std::string{"hostname"};
  auto mock_boot = std::make_shared<MockBootstrap>();
  auto mock_service = std::make_shared<MockService>();
  auto mock_cf = std::make_shared<MockConnectionFactory>();
  uint32_t port{999};

  EXPECT_CALL((*mock_cf), Connect(_, _, _)).Times(1).WillRepeatedly(Return(mock_service));
  EXPECT_CALL((*mock_cf), MakeBootstrap()).Times(1).WillRepeatedly(Return(mock_boot));
  ConnectionPool cp{mock_cf};

  auto remote_id = std::make_shared<ConnectionId>(hostname, port);
  auto result = cp.GetConnection(remote_id);
  ASSERT_TRUE(result != nullptr);
  result = cp.GetConnection(remote_id);
}

TEST(TestConnectionPool, TestOnlyCreateMultipleDispose) {
  std::string hostname_one{"hostname"};
  std::string hostname_two{"hostname_two"};
  uint32_t port{999};

  auto mock_boot = std::make_shared<MockBootstrap>();
  auto mock_service = std::make_shared<MockService>();
  auto mock_cf = std::make_shared<MockConnectionFactory>();

  EXPECT_CALL((*mock_cf), Connect(_, _, _)).Times(2).WillRepeatedly(Return(mock_service));
  EXPECT_CALL((*mock_cf), MakeBootstrap()).Times(2).WillRepeatedly(Return(mock_boot));
  ConnectionPool cp{mock_cf};

  {
    auto remote_id = std::make_shared<ConnectionId>(hostname_one, port);
    auto result_one = cp.GetConnection(remote_id);
    auto remote_id2 = std::make_shared<ConnectionId>(hostname_two, port);
    auto result_two = cp.GetConnection(remote_id2);
  }
  auto remote_id = std::make_shared<ConnectionId>(hostname_one, port);
  auto result_one = cp.GetConnection(remote_id);
  auto remote_id2 = std::make_shared<ConnectionId>(hostname_two, port);
  auto result_two = cp.GetConnection(remote_id2);
}
