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
#include <folly/Memory.h>
#include <folly/futures/Future.h>
#include <gmock/gmock.h>
#include <google/protobuf/stubs/callback.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>

#include <functional>
#include <string>

#include "connection/request.h"
#include "connection/response.h"
#include "connection/rpc-client.h"
#include "core/async-rpc-retrying-caller-factory.h"
#include "core/async-rpc-retrying-caller.h"
#include "core/client.h"
#include "core/hbase-rpc-controller.h"
#include "core/keyvalue-codec.h"
#include "core/region-location.h"
#include "core/request_converter.h"
#include "core/response_converter.h"
#include "core/result.h"
#include "exceptions/exception.h"
#include "if/Client.pb.h"
#include "if/HBase.pb.h"
#include "test-util/test-util.h"

using namespace google::protobuf;
using namespace hbase;
using namespace hbase::pb;
using namespace std::placeholders;
using namespace testing;
using ::testing::Return;
using ::testing::_;
using std::chrono::nanoseconds;

class MockRpcControllerFactory {
 public:
  MOCK_METHOD0(NewController, std::shared_ptr<HBaseRpcController>());
};

class MockAsyncConnectionConfiguration {
 public:
  MOCK_METHOD0(GetPauseNs, nanoseconds());
  MOCK_METHOD0(GetMaxRetries, int32_t());
  MOCK_METHOD0(GetStartLogErrorsCount, int32_t());
  MOCK_METHOD0(GetReadRpcTimeoutNs, nanoseconds());
  MOCK_METHOD0(GetOperationTimeoutNs, nanoseconds());
};

class AsyncRegionLocator {
 public:
  explicit AsyncRegionLocator(std::shared_ptr<RegionLocation> region_location)
      : region_location_(region_location) {}
  ~AsyncRegionLocator() = default;

  folly::Future<RegionLocation> GetRegionLocation(std::shared_ptr<hbase::pb::TableName>,
                                                  const std::string&, RegionLocateType, int64_t) {
    folly::Promise<RegionLocation> promise;
    promise.setValue(*region_location_);
    return promise.getFuture();
  }

  void UpdateCachedLocation(const RegionLocation&, const std::exception&) {}

 private:
  std::shared_ptr<RegionLocation> region_location_;
};

class MockAsyncConnection {
 public:
  MOCK_METHOD0(get_conn_conf, std::shared_ptr<MockAsyncConnectionConfiguration>());
  MOCK_METHOD0(get_rpc_controller_factory, std::shared_ptr<MockRpcControllerFactory>());
  MOCK_METHOD0(get_locator, std::shared_ptr<AsyncRegionLocator>());
  MOCK_METHOD0(GetRpcClient, std::shared_ptr<hbase::RpcClient>());
};

template <typename CONN>
class MockRawAsyncTableImpl {
 public:
  explicit MockRawAsyncTableImpl(std::shared_ptr<CONN> conn)
      : conn_(conn), promise_(std::make_shared<folly::Promise<hbase::Result>>()) {}
  virtual ~MockRawAsyncTableImpl() = default;

  /* implement this in real RawAsyncTableImpl. */

  /* in real RawAsyncTableImpl, this should be private. */
  folly::Future<hbase::Result> GetCall(std::shared_ptr<hbase::RpcClient> rpc_client,
                                       std::shared_ptr<HBaseRpcController> controller,
                                       std::shared_ptr<RegionLocation> loc, const hbase::Get& get) {
    hbase::RpcCall<hbase::Request, hbase::Response, hbase::RpcClient> rpc_call = [](
        std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<RegionLocation> loc,
        std::shared_ptr<HBaseRpcController> controller,
        std::unique_ptr<hbase::Request> preq) -> folly::Future<std::unique_ptr<hbase::Response>> {
      return rpc_client->AsyncCall(loc->server_name().host_name(), loc->server_name().port(),
                                   std::move(preq), User::defaultUser(), "ClientService");
    };

    return Call<hbase::Get, hbase::Request, hbase::Response, hbase::Result>(
        rpc_client, controller, loc, get, &hbase::RequestConverter::ToGetRequest, rpc_call,
        &hbase::ResponseConverter::FromGetResponse);
  }

  /* in real RawAsyncTableImpl, this should be private. */
  template <typename REQ, typename PREQ, typename PRESP, typename RESP>
  folly::Future<RESP> Call(
      std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<HBaseRpcController> controller,
      std::shared_ptr<RegionLocation> loc, const REQ& req,
      const ReqConverter<std::unique_ptr<PREQ>, REQ, std::string>& req_converter,
      const hbase::RpcCall<PREQ, PRESP, hbase::RpcClient>& rpc_call,
      const RespConverter<std::unique_ptr<RESP>, PRESP>& resp_converter) {
    rpc_call(rpc_client, loc, controller, std::move(req_converter(req, loc->region_name())))
        .then([&, this](std::unique_ptr<PRESP> presp) {
          std::unique_ptr<hbase::Result> result = hbase::ResponseConverter::FromGetResponse(*presp);
          promise_->setValue(std::move(*result));
        })
        .onError([this](const std::exception& e) { promise_->setException(e); });
    return promise_->getFuture();
  }

 private:
  std::shared_ptr<CONN> conn_;
  std::shared_ptr<folly::Promise<hbase::Result>> promise_;
};

TEST(AsyncRpcRetryTest, TestGetBasic) {
  // Remove already configured env if present.
  unsetenv("HBASE_CONF");

  // Using TestUtil to populate test data
  hbase::TestUtil* test_util = new hbase::TestUtil();
  test_util->RunShellCmd("create 't', 'd'");
  test_util->RunShellCmd("put 't', 'test2', 'd:2', 'value2'");
  test_util->RunShellCmd("put 't', 'test2', 'd:extra', 'value for extra'");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test2";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create Configuration
  hbase::Configuration conf;

  // Create a client
  Client client(conf);

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  /* init region location and rpc channel */
  auto region_location = table->GetRegionLocation(row);

  auto io_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(1);
  auto codec = std::make_shared<hbase::KeyValueCodec>();
  auto rpc_client = std::make_shared<RpcClient>(io_executor_, codec);

  /* init rpc controller */
  auto controller = std::make_shared<HBaseRpcController>();

  /* init rpc controller factory */
  auto controller_factory = std::make_shared<MockRpcControllerFactory>();
  EXPECT_CALL((*controller_factory), NewController()).Times(1).WillRepeatedly(Return(controller));

  /* init connection configuration */
  auto connection_conf = std::make_shared<MockAsyncConnectionConfiguration>();
  EXPECT_CALL((*connection_conf), GetPauseNs())
      .Times(1)
      .WillRepeatedly(Return(nanoseconds(100000000)));
  EXPECT_CALL((*connection_conf), GetMaxRetries()).Times(1).WillRepeatedly(Return(31));
  EXPECT_CALL((*connection_conf), GetStartLogErrorsCount()).Times(1).WillRepeatedly(Return(9));
  EXPECT_CALL((*connection_conf), GetReadRpcTimeoutNs())
      .Times(1)
      .WillRepeatedly(Return(nanoseconds(60000000000)));
  EXPECT_CALL((*connection_conf), GetOperationTimeoutNs())
      .Times(1)
      .WillRepeatedly(Return(nanoseconds(1200000000000)));

  /* init region locator */
  auto region_locator = std::make_shared<AsyncRegionLocator>(region_location);

  /* init hbase client connection */
  auto conn = std::make_shared<MockAsyncConnection>();
  EXPECT_CALL((*conn), get_conn_conf()).Times(AtLeast(1)).WillRepeatedly(Return(connection_conf));
  EXPECT_CALL((*conn), get_rpc_controller_factory())
      .Times(AtLeast(1))
      .WillRepeatedly(Return(controller_factory));
  EXPECT_CALL((*conn), get_locator()).Times(AtLeast(1)).WillRepeatedly(Return(region_locator));
  EXPECT_CALL((*conn), GetRpcClient()).Times(AtLeast(1)).WillRepeatedly(Return(rpc_client));

  /* init retry caller factory */
  auto tableImpl = std::make_shared<MockRawAsyncTableImpl<MockAsyncConnection>>(conn);
  AsyncRpcRetryingCallerFactory<MockAsyncConnection> caller_factory(conn);

  /* init request caller builder */
  auto builder = caller_factory.Single<hbase::Result>();

  /* call with retry to get result */
  try {
    auto async_caller =
        builder->table(std::make_shared<TableName>(tn))
            ->row(row)
            ->rpc_timeout(conn->get_conn_conf()->GetReadRpcTimeoutNs())
            ->operation_timeout(conn->get_conn_conf()->GetOperationTimeoutNs())
            ->action(
                [=, &get](
                    std::shared_ptr<hbase::HBaseRpcController> controller,
                    std::shared_ptr<hbase::RegionLocation> loc,
                    std::shared_ptr<hbase::RpcClient> rpc_client) -> folly::Future<hbase::Result> {
                  return tableImpl->GetCall(rpc_client, controller, loc, get);
                })
            ->Build();

    hbase::Result result = async_caller->Call().get();

    /*Stopping the connection as we are getting segfault due to some folly issue
     The connection stays open and we don't want that.
     So we are stopping the connection.
     We can remove this once we have fixed the folly part */
    delete test_util;

    // Test the values, should be same as in put executed on hbase shell
    ASSERT_TRUE(!result.IsEmpty()) << "Result shouldn't be empty.";
    EXPECT_EQ("test2", result.Row());
    EXPECT_EQ("value2", *(result.Value("d", "2")));
    EXPECT_EQ("value for extra", *(result.Value("d", "extra")));
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
    throw e;
  }

  table->Close();
  client.Close();
}
