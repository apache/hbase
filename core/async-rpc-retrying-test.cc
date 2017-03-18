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
#include "core/async-connection.h"
#include "core/async-rpc-retrying-caller-factory.h"
#include "core/async-rpc-retrying-caller.h"
#include "core/client.h"
#include "core/connection-configuration.h"
#include "core/hbase-rpc-controller.h"
#include "core/keyvalue-codec.h"
#include "core/region-location.h"
#include "core/request-converter.h"
#include "core/response-converter.h"
#include "core/result.h"
#include "exceptions/exception.h"
#include "if/Client.pb.h"
#include "if/HBase.pb.h"
#include "test-util/test-util.h"
#include "utils/time-util.h"

using namespace google::protobuf;
using namespace hbase;
using namespace hbase::pb;
using namespace std::placeholders;
using namespace testing;
using ::testing::Return;
using ::testing::_;
using std::chrono::nanoseconds;

class MockAsyncRegionLocator : public AsyncRegionLocator {
 public:
  explicit MockAsyncRegionLocator(std::shared_ptr<RegionLocation> region_location)
      : region_location_(region_location) {}
  ~MockAsyncRegionLocator() = default;

  folly::Future<std::shared_ptr<RegionLocation>> LocateRegion(const hbase::pb::TableName&,
                                                              const std::string&,
                                                              const RegionLocateType,
                                                              const int64_t) override {
    folly::Promise<std::shared_ptr<RegionLocation>> promise;
    promise.setValue(region_location_);
    return promise.getFuture();
  }

  void UpdateCachedLocation(const RegionLocation&, const std::exception&) override {}

 private:
  std::shared_ptr<RegionLocation> region_location_;
};

class MockAsyncConnection : public AsyncConnection,
                            public std::enable_shared_from_this<MockAsyncConnection> {
 public:
  MockAsyncConnection(std::shared_ptr<ConnectionConfiguration> conn_conf,
                      std::shared_ptr<RpcClient> rpc_client,
                      std::shared_ptr<AsyncRegionLocator> region_locator)
      : conn_conf_(conn_conf), rpc_client_(rpc_client), region_locator_(region_locator) {}
  ~MockAsyncConnection() {}
  void Init() {
    caller_factory_ = std::make_shared<AsyncRpcRetryingCallerFactory>(shared_from_this());
  }

  std::shared_ptr<Configuration> conf() override { return nullptr; }
  std::shared_ptr<ConnectionConfiguration> connection_conf() override { return conn_conf_; }
  std::shared_ptr<AsyncRpcRetryingCallerFactory> caller_factory() override {
    return caller_factory_;
  }
  std::shared_ptr<RpcClient> rpc_client() override { return rpc_client_; }
  std::shared_ptr<AsyncRegionLocator> region_locator() override { return region_locator_; }

  void Close() override {}
  std::shared_ptr<HBaseRpcController> CreateRpcController() override {
    return std::make_shared<HBaseRpcController>();
  }

 private:
  std::shared_ptr<ConnectionConfiguration> conn_conf_;
  std::shared_ptr<AsyncRpcRetryingCallerFactory> caller_factory_;
  std::shared_ptr<RpcClient> rpc_client_;
  std::shared_ptr<AsyncRegionLocator> region_locator_;
};

template <typename CONN>
class MockRawAsyncTableImpl {
 public:
  explicit MockRawAsyncTableImpl(std::shared_ptr<CONN> conn)
      : conn_(conn), promise_(std::make_shared<folly::Promise<std::shared_ptr<hbase::Result>>>()) {}
  virtual ~MockRawAsyncTableImpl() = default;

  /* implement this in real RawAsyncTableImpl. */

  /* in real RawAsyncTableImpl, this should be private. */
  folly::Future<std::shared_ptr<hbase::Result>> GetCall(
      std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<HBaseRpcController> controller,
      std::shared_ptr<RegionLocation> loc, const hbase::Get& get) {
    hbase::RpcCall<hbase::Request, hbase::Response> rpc_call = [](
        std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<RegionLocation> loc,
        std::shared_ptr<HBaseRpcController> controller,
        std::unique_ptr<hbase::Request> preq) -> folly::Future<std::unique_ptr<hbase::Response>> {
      return rpc_client->AsyncCall(loc->server_name().host_name(), loc->server_name().port(),
                                   std::move(preq), User::defaultUser(), "ClientService");
    };

    return Call<hbase::Get, hbase::Request, hbase::Response, std::shared_ptr<hbase::Result>>(
        rpc_client, controller, loc, get, &hbase::RequestConverter::ToGetRequest, rpc_call,
        &hbase::ResponseConverter::FromGetResponse);
  }

  /* in real RawAsyncTableImpl, this should be private. */
  template <typename REQ, typename PREQ, typename PRESP, typename RESP>
  folly::Future<RESP> Call(
      std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<HBaseRpcController> controller,
      std::shared_ptr<RegionLocation> loc, const REQ& req,
      const ReqConverter<std::unique_ptr<PREQ>, REQ, std::string>& req_converter,
      const hbase::RpcCall<PREQ, PRESP>& rpc_call,
      const RespConverter<RESP, PRESP>& resp_converter) {
    rpc_call(rpc_client, loc, controller, std::move(req_converter(req, loc->region_name())))
        .then([&, this](std::unique_ptr<PRESP> presp) {
          std::shared_ptr<hbase::Result> result = hbase::ResponseConverter::FromGetResponse(*presp);
          promise_->setValue(result);
        })
        .onError([this](const std::exception& e) { promise_->setException(e); });
    return promise_->getFuture();
  }

 private:
  std::shared_ptr<CONN> conn_;
  std::shared_ptr<folly::Promise<std::shared_ptr<hbase::Result>>> promise_;
};

TEST(AsyncRpcRetryTest, TestGetBasic) {
  // Using TestUtil to populate test data
  auto test_util = std::make_unique<hbase::TestUtil>();
  test_util->StartMiniCluster(2);

  test_util->CreateTable("t", "d");
  test_util->TablePut("t", "test2", "d", "2", "value2");
  test_util->TablePut("t", "test2", "d", "extra", "value for extra");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>("t");
  auto row = "test2";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create a client
  Client client(*(test_util->conf()));

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  /* init region location and rpc channel */
  auto region_location = table->GetRegionLocation(row);

  auto io_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(1);
  auto codec = std::make_shared<hbase::KeyValueCodec>();
  auto rpc_client = std::make_shared<RpcClient>(io_executor_, codec);

  /* init connection configuration */
  auto connection_conf = std::make_shared<ConnectionConfiguration>(
      TimeUtil::SecondsToNanos(20),    // connect_timeout
      TimeUtil::SecondsToNanos(1200),  // operation_timeout
      TimeUtil::SecondsToNanos(60),    // rpc_timeout
      TimeUtil::MillisToNanos(100),    // pause
      31,                              // max retries
      9);                              // start log errors count

  /* init region locator */
  auto region_locator = std::make_shared<MockAsyncRegionLocator>(region_location);

  /* init hbase client connection */
  auto conn = std::make_shared<MockAsyncConnection>(connection_conf, rpc_client, region_locator);
  conn->Init();

  /* init retry caller factory */
  auto tableImpl = std::make_shared<MockRawAsyncTableImpl<MockAsyncConnection>>(conn);

  /* init request caller builder */
  auto builder = conn->caller_factory()->Single<std::shared_ptr<hbase::Result>>();

  /* call with retry to get result */
  try {
    auto async_caller =
        builder->table(std::make_shared<TableName>(tn))
            ->row(row)
            ->rpc_timeout(conn->connection_conf()->read_rpc_timeout())
            ->operation_timeout(conn->connection_conf()->operation_timeout())
            ->action([=, &get](std::shared_ptr<hbase::HBaseRpcController> controller,
                               std::shared_ptr<hbase::RegionLocation> loc,
                               std::shared_ptr<hbase::RpcClient> rpc_client)
                         -> folly::Future<std::shared_ptr<hbase::Result>> {
                           return tableImpl->GetCall(rpc_client, controller, loc, get);
                         })
            ->Build();

    auto result = async_caller->Call().get();

    // Test the values, should be same as in put executed on hbase shell
    ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
    EXPECT_EQ("test2", result->Row());
    EXPECT_EQ("value2", *(result->Value("d", "2")));
    EXPECT_EQ("value for extra", *(result->Value("d", "extra")));
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
    throw e;
  }

  table->Close();
  client.Close();
}
