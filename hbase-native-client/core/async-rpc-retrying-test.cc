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
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <gmock/gmock.h>
#include <google/protobuf/stubs/callback.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>

#include <chrono>
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

using hbase::AsyncRpcRetryingCallerFactory;
using hbase::AsyncConnection;
using hbase::AsyncRegionLocator;
using hbase::ConnectionConfiguration;
using hbase::Configuration;
using hbase::HBaseRpcController;
using hbase::RegionLocation;
using hbase::RegionLocateType;
using hbase::RpcClient;
using hbase::RequestConverter;
using hbase::ResponseConverter;
using hbase::ReqConverter;
using hbase::RespConverter;
using hbase::Put;
using hbase::TimeUtil;
using hbase::Client;

using ::testing::Return;
using ::testing::_;
using std::chrono::nanoseconds;
using std::chrono::milliseconds;

using namespace hbase;

using folly::exception_wrapper;

class AsyncRpcRetryTest : public ::testing::Test {
 public:
  static std::unique_ptr<hbase::TestUtil> test_util;

  static void SetUpTestCase() {
    google::InstallFailureSignalHandler();
    test_util = std::make_unique<hbase::TestUtil>();
    test_util->StartMiniCluster(2);
  }
};
std::unique_ptr<hbase::TestUtil> AsyncRpcRetryTest::test_util = nullptr;

class AsyncRegionLocatorBase : public AsyncRegionLocator {
 public:
  AsyncRegionLocatorBase() {}
  explicit AsyncRegionLocatorBase(std::shared_ptr<RegionLocation> region_location)
      : region_location_(region_location) {}
  virtual ~AsyncRegionLocatorBase() = default;

  folly::Future<std::shared_ptr<hbase::RegionLocation>> LocateRegion(const hbase::pb::TableName &,
                                                                     const std::string &,
                                                                     const RegionLocateType,
                                                                     const int64_t) override {
    folly::Promise<std::shared_ptr<RegionLocation>> promise;
    promise.setValue(region_location_);
    return promise.getFuture();
  }

  virtual void set_region_location(std::shared_ptr<RegionLocation> region_location) {
    region_location_ = region_location;
  }

  void UpdateCachedLocation(const RegionLocation &, const folly::exception_wrapper &) override {}

 protected:
  std::shared_ptr<RegionLocation> region_location_;
};

class MockAsyncRegionLocator : public AsyncRegionLocatorBase {
 public:
  MockAsyncRegionLocator() : AsyncRegionLocatorBase() {}
  explicit MockAsyncRegionLocator(std::shared_ptr<RegionLocation> region_location)
      : AsyncRegionLocatorBase(region_location) {}
  virtual ~MockAsyncRegionLocator() {}
};

class MockWrongRegionAsyncRegionLocator : public AsyncRegionLocatorBase {
 private:
  uint32_t tries_ = 0;
  uint32_t num_fails_ = 0;

 public:
  explicit MockWrongRegionAsyncRegionLocator(uint32_t num_fails)
      : AsyncRegionLocatorBase(), num_fails_(num_fails) {}
  explicit MockWrongRegionAsyncRegionLocator(std::shared_ptr<RegionLocation> region_location)
      : AsyncRegionLocatorBase(region_location) {}
  virtual ~MockWrongRegionAsyncRegionLocator() {}

  folly::Future<std::shared_ptr<hbase::RegionLocation>> LocateRegion(
      const hbase::pb::TableName &tn, const std::string &row,
      const RegionLocateType locate_type = RegionLocateType::kCurrent,
      const int64_t locate_ns = 0) override {
    // Fail for num_fails_ times, then delegate to the super class which will give the correct
    // region location.
    if (tries_++ > num_fails_) {
      return AsyncRegionLocatorBase::LocateRegion(tn, row, locate_type, locate_ns);
    }
    folly::Promise<std::shared_ptr<RegionLocation>> promise;
    /* set random region name, simulating invalid region */
    auto result = std::make_shared<RegionLocation>(
        "whatever-region-name", region_location_->region_info(), region_location_->server_name(),
        region_location_->service());
    promise.setValue(result);
    return promise.getFuture();
  }
};

class MockFailingAsyncRegionLocator : public AsyncRegionLocatorBase {
 private:
  uint32_t tries_ = 0;
  uint32_t num_fails_ = 0;

 public:
  explicit MockFailingAsyncRegionLocator(uint32_t num_fails)
      : AsyncRegionLocatorBase(), num_fails_(num_fails) {}
  explicit MockFailingAsyncRegionLocator(std::shared_ptr<RegionLocation> region_location)
      : AsyncRegionLocatorBase(region_location) {}
  virtual ~MockFailingAsyncRegionLocator() {}
  folly::Future<std::shared_ptr<hbase::RegionLocation>> LocateRegion(
      const hbase::pb::TableName &tn, const std::string &row,
      const RegionLocateType locate_type = RegionLocateType::kCurrent,
      const int64_t locate_ns = 0) override {
    // Fail for num_fails_ times, then delegate to the super class which will give the correct
    // region location.
    if (tries_++ > num_fails_) {
      return AsyncRegionLocatorBase::LocateRegion(tn, row, locate_type, locate_ns);
    }
    folly::Promise<std::shared_ptr<RegionLocation>> promise;
    promise.setException(std::runtime_error{"Failed to look up region location"});
    return promise.getFuture();
  }
};

class MockAsyncConnection : public AsyncConnection,
                            public std::enable_shared_from_this<MockAsyncConnection> {
 public:
  MockAsyncConnection(std::shared_ptr<ConnectionConfiguration> conn_conf,
                      std::shared_ptr<folly::HHWheelTimer> retry_timer,
                      std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor,
                      std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor,
                      std::shared_ptr<wangle::IOThreadPoolExecutor> retry_executor,
                      std::shared_ptr<RpcClient> rpc_client,
                      std::shared_ptr<AsyncRegionLocator> region_locator)
      : conn_conf_(conn_conf),
        retry_timer_(retry_timer),
        cpu_executor_(cpu_executor),
        io_executor_(io_executor),
        retry_executor_(retry_executor),
        rpc_client_(rpc_client),
        region_locator_(region_locator) {}
  ~MockAsyncConnection() {}
  void Init() {
    caller_factory_ =
        std::make_shared<AsyncRpcRetryingCallerFactory>(shared_from_this(), retry_timer_);
  }

  std::shared_ptr<Configuration> conf() override { return nullptr; }
  std::shared_ptr<ConnectionConfiguration> connection_conf() override { return conn_conf_; }
  std::shared_ptr<AsyncRpcRetryingCallerFactory> caller_factory() override {
    return caller_factory_;
  }
  std::shared_ptr<RpcClient> rpc_client() override { return rpc_client_; }
  std::shared_ptr<AsyncRegionLocator> region_locator() override { return region_locator_; }
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor() override { return cpu_executor_; }
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor() override { return io_executor_; }
  std::shared_ptr<wangle::IOThreadPoolExecutor> retry_executor() override {
    return retry_executor_;
  }

  void Close() override {}
  std::shared_ptr<HBaseRpcController> CreateRpcController() override {
    return std::make_shared<HBaseRpcController>();
  }

 private:
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<ConnectionConfiguration> conn_conf_;
  std::shared_ptr<AsyncRpcRetryingCallerFactory> caller_factory_;
  std::shared_ptr<RpcClient> rpc_client_;
  std::shared_ptr<AsyncRegionLocator> region_locator_;
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor_;
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor_;
  std::shared_ptr<wangle::IOThreadPoolExecutor> retry_executor_;
};

template <typename CONN>
class MockRawAsyncTableImpl {
 public:
  explicit MockRawAsyncTableImpl(std::shared_ptr<CONN> conn) : conn_(conn) {}
  virtual ~MockRawAsyncTableImpl() = default;

  /* implement this in real RawAsyncTableImpl. */

  /* in real RawAsyncTableImpl, this should be private. */
  folly::Future<std::shared_ptr<hbase::Result>> GetCall(
      std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<HBaseRpcController> controller,
      std::shared_ptr<RegionLocation> loc, const hbase::Get &get) {
    hbase::RpcCall<hbase::Request, hbase::Response> rpc_call = [](
        std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<RegionLocation> loc,
        std::shared_ptr<HBaseRpcController> controller,
        std::unique_ptr<hbase::Request> preq) -> folly::Future<std::unique_ptr<hbase::Response>> {
      VLOG(1) << "entering MockRawAsyncTableImpl#GetCall, calling AsyncCall, loc:"
              << loc->DebugString();
      return rpc_client->AsyncCall(loc->server_name().host_name(), loc->server_name().port(),
                                   std::move(preq), User::defaultUser(), "ClientService");
    };

    return Call<hbase::Get, hbase::Request, hbase::Response, std::shared_ptr<hbase::Result>>(
        rpc_client, controller, loc, get, &hbase::RequestConverter::ToGetRequest, rpc_call,
        &hbase::ResponseConverter::FromGetResponse);
  }

  /* in real RawAsyncTableImpl, this should be private. */
  template <typename REQ, typename PREQ, typename PRESP, typename RESP>
  folly::Future<RESP> Call(std::shared_ptr<hbase::RpcClient> rpc_client,
                           std::shared_ptr<HBaseRpcController> controller,
                           std::shared_ptr<RegionLocation> loc, const REQ &req,
                           ReqConverter<std::unique_ptr<PREQ>, REQ, std::string> req_converter,
                           hbase::RpcCall<PREQ, PRESP> rpc_call,
                           RespConverter<RESP, PRESP> resp_converter) {
    promise_ = std::make_shared<folly::Promise<std::shared_ptr<hbase::Result>>>();
    auto f = promise_->getFuture();
    VLOG(1) << "calling rpc_call";
    rpc_call(rpc_client, loc, controller, std::move(req_converter(req, loc->region_name())))
        .then([&, this, resp_converter](std::unique_ptr<PRESP> presp) {
          VLOG(1) << "MockRawAsyncTableImpl#call succeded: ";
          RESP result = resp_converter(*presp);
          promise_->setValue(result);
        })
        .onError([this](const exception_wrapper &e) {
          VLOG(1) << "entering MockRawAsyncTableImpl#call, exception: " << e.what();
          VLOG(1) << "entering MockRawAsyncTableImpl#call, error typeinfo: " << typeid(e).name();
          promise_->setException(e);
        });
    return f;
  }

 private:
  std::shared_ptr<CONN> conn_;
  std::shared_ptr<folly::Promise<std::shared_ptr<hbase::Result>>> promise_;
};

void runTest(std::shared_ptr<AsyncRegionLocatorBase> region_locator, std::string tableName,
             uint32_t operation_timeout_millis = 1200000) {
  AsyncRpcRetryTest::test_util->CreateTable(tableName, "d");

  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(tableName);
  auto row = "test2";

  // Get to be performed on above HBase Table
  hbase::Get get(row);

  // Create a client
  Client client(*(AsyncRpcRetryTest::test_util->conf()));

  // Get connection to HBase Table
  auto table = client.Table(tn);
  ASSERT_TRUE(table) << "Unable to get connection to Table.";

  table->Put(Put{"test2"}.AddColumn("d", "2", "value2"));
  table->Put(Put{"test2"}.AddColumn("d", "extra", "value for extra"));

  /* init region location and rpc channel */
  auto region_location = table->GetRegionLocation(row);

  // auto io_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(4);
  auto cpu_executor_ = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io_executor_ = client.async_connection()->io_executor();
  auto retry_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(1);
  auto codec = std::make_shared<hbase::KeyValueCodec>();
  auto rpc_client = std::make_shared<RpcClient>(io_executor_, codec);
  // auto retry_event_base_ = std::make_shared<folly::ScopedEventBaseThread>(true);
  std::shared_ptr<folly::HHWheelTimer> retry_timer =
      folly::HHWheelTimer::newTimer(retry_executor_->getEventBase());

  /* init connection configuration */
  auto connection_conf = std::make_shared<ConnectionConfiguration>(
      TimeUtil::SecondsToNanos(20),                       // connect_timeout
      TimeUtil::MillisToNanos(operation_timeout_millis),  // operation_timeout
      TimeUtil::SecondsToNanos(60),                       // rpc_timeout
      TimeUtil::MillisToNanos(100),                       // pause
      5,                                                  // max retries
      9);                                                 // start log errors count

  /* set region locator */
  region_locator->set_region_location(region_location);

  /* init hbase client connection */
  auto conn = std::make_shared<MockAsyncConnection>(connection_conf, retry_timer, cpu_executor_,
                                                    io_executor_, retry_executor_, rpc_client,
                                                    region_locator);
  conn->Init();

  /* init retry caller factory */
  auto tableImpl = std::make_shared<MockRawAsyncTableImpl<MockAsyncConnection>>(conn);

  /* init request caller builder */
  auto builder = conn->caller_factory()->Single<std::shared_ptr<hbase::Result>>();

  /* call with retry to get result */

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

  auto promise = std::make_shared<folly::Promise<std::shared_ptr<hbase::Result>>>();

  auto result = async_caller->Call().get(milliseconds(500000));

  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!result->IsEmpty()) << "Result shouldn't be empty.";
  EXPECT_EQ("test2", result->Row());
  EXPECT_EQ("value2", *(result->Value("d", "2")));
  EXPECT_EQ("value for extra", *(result->Value("d", "extra")));

  retry_timer->destroy();
  table->Close();
  client.Close();
  retry_executor_->stop();
}

// Test successful case
TEST_F(AsyncRpcRetryTest, TestGetBasic) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockAsyncRegionLocator>());
  runTest(region_locator, "table1");
}

// Tests the RPC failing 3 times, then succeeding
TEST_F(AsyncRpcRetryTest, TestHandleException) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockWrongRegionAsyncRegionLocator>(3));
  runTest(region_locator, "table2");
}

// Tests the RPC failing 5 times, throwing an exception
TEST_F(AsyncRpcRetryTest, TestFailWithException) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockWrongRegionAsyncRegionLocator>(5));
  EXPECT_ANY_THROW(runTest(region_locator, "table3"));
}

// Tests the region location lookup failing 3 times, then succeeding
TEST_F(AsyncRpcRetryTest, TestHandleExceptionFromRegionLocationLookup) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(3));
  runTest(region_locator, "table4");
}

// Tests the region location lookup failing 5 times, throwing an exception
TEST_F(AsyncRpcRetryTest, TestFailWithExceptionFromRegionLocationLookup) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(5));
  EXPECT_ANY_THROW(runTest(region_locator, "table5"));
}

// Tests hitting operation timeout, thus not retrying anymore
TEST_F(AsyncRpcRetryTest, TestFailWithOperationTimeout) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(3));
  EXPECT_ANY_THROW(runTest(region_locator, "table6", 200));
}
