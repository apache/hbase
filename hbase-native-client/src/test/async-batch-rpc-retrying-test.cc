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
#include <gtest/gtest.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>

#include <chrono>
#include <functional>
#include <string>

#include "hbase/connection/rpc-client.h"
#include "hbase/client/async-batch-rpc-retrying-caller.h"
#include "hbase/client/async-connection.h"
#include "hbase/client/async-rpc-retrying-caller-factory.h"
#include "hbase/client/client.h"
#include "hbase/client/connection-configuration.h"
#include "hbase/client/keyvalue-codec.h"
#include "hbase/client/region-location.h"
#include "hbase/client/result.h"
#include "hbase/exceptions/exception.h"
#include "hbase/test-util/test-util.h"
#include "hbase/utils/time-util.h"

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
using hbase::Put;
using hbase::TimeUtil;
using hbase::Client;
using hbase::security::User;

using std::chrono::nanoseconds;
using std::chrono::milliseconds;

using namespace hbase;

using folly::exception_wrapper;

class AsyncBatchRpcRetryTest : public ::testing::Test {
 public:
  static std::unique_ptr<hbase::TestUtil> test_util;
  static std::string tableName;

  static void SetUpTestCase() {
    google::InstallFailureSignalHandler();
    test_util = std::make_unique<hbase::TestUtil>();
    test_util->StartMiniCluster(2);
    std::vector<std::string> keys{"test0",   "test100", "test200", "test300", "test400",
                                  "test500", "test600", "test700", "test800", "test900"};
    tableName = "split-table1";
    test_util->CreateTable(tableName, "d", keys);
  }
};
std::unique_ptr<hbase::TestUtil> AsyncBatchRpcRetryTest::test_util = nullptr;
std::string AsyncBatchRpcRetryTest::tableName;

class AsyncRegionLocatorBase : public AsyncRegionLocator {
 public:
  AsyncRegionLocatorBase() {}
  explicit AsyncRegionLocatorBase(std::shared_ptr<RegionLocation> region_location)
      : region_location_(region_location) {}
  virtual ~AsyncRegionLocatorBase() = default;

  folly::Future<std::shared_ptr<hbase::RegionLocation>> LocateRegion(const hbase::pb::TableName &,
                                                                     const std::string &row,
                                                                     const RegionLocateType,
                                                                     const int64_t) override {
    folly::Promise<std::shared_ptr<RegionLocation>> promise;
    promise.setValue(region_locations_.at(row));
    return promise.getFuture();
  }

  virtual void set_region_location(std::shared_ptr<RegionLocation> region_location) {
    region_location_ = region_location;
  }

  virtual void set_region_location(
      const std::map<std::string, std::shared_ptr<RegionLocation>> &reg_locs) {
    for (auto reg_loc : reg_locs) {
      region_locations_[reg_loc.first] = reg_loc.second;
    }
  }

  void UpdateCachedLocation(const RegionLocation &rl, const folly::exception_wrapper &ew) override {
  }

 protected:
  std::shared_ptr<RegionLocation> region_location_;
  std::map<std::string, std::shared_ptr<RegionLocation>> region_locations_;
  std::map<std::string, uint32_t> mtries_;
  std::map<std::string, uint32_t> mnum_fails_;

  void InitRetryMaps(uint32_t num_fails) {
    if (mtries_.size() == 0 && mnum_fails_.size() == 0) {
      for (auto reg_loc : region_locations_) {
        mtries_[reg_loc.first] = 0;
        mnum_fails_[reg_loc.first] = num_fails;
      }
    }
  }
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
  uint32_t counter_ = 0;
  uint32_t num_fails_ = 0;
  uint32_t tries_ = 0;

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
    InitRetryMaps(num_fails_);
    auto &tries = mtries_[row];
    auto &num_fails = mnum_fails_[row];
    if (++tries > num_fails) {
      return AsyncRegionLocatorBase::LocateRegion(tn, row, locate_type, locate_ns);
    }

    folly::Promise<std::shared_ptr<RegionLocation>> promise;
    /* set random region name, simulating invalid region */
    auto result = std::make_shared<RegionLocation>("whatever-region-name",
                                                   region_locations_.at(row)->region_info(),
                                                   region_locations_.at(row)->server_name());
    promise.setValue(result);
    return promise.getFuture();
  }
};

class MockFailingAsyncRegionLocator : public AsyncRegionLocatorBase {
 private:
  uint32_t tries_ = 0;
  uint32_t num_fails_ = 0;
  uint32_t counter_ = 0;

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
    InitRetryMaps(num_fails_);
    auto &tries = mtries_[row];
    auto &num_fails = mnum_fails_[row];
    if (++tries > num_fails) {
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

  void Close() override {
    retry_timer_->destroy();
    retry_executor_->stop();
    io_executor_->stop();
    cpu_executor_->stop();
  }
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

class MockRawAsyncTableImpl {
 public:
  explicit MockRawAsyncTableImpl(std::shared_ptr<MockAsyncConnection> conn,
                                 std::shared_ptr<hbase::pb::TableName> tn)
      : conn_(conn), tn_(tn) {}
  virtual ~MockRawAsyncTableImpl() = default;

  /* implement this in real RawAsyncTableImpl. */
  template <typename REQ, typename RESP>
  folly::Future<std::vector<folly::Try<RESP>>> Batch(const std::vector<REQ> &rows) {
    /* init request caller builder */
    auto builder = conn_->caller_factory()->Batch<REQ, RESP>();

    /* call with retry to get result */
    auto async_caller =
        builder->table(tn_)
            ->actions(std::make_shared<std::vector<REQ>>(rows))
            ->rpc_timeout(conn_->connection_conf()->read_rpc_timeout())
            ->operation_timeout(conn_->connection_conf()->operation_timeout())
            ->pause(conn_->connection_conf()->pause())
            ->max_attempts(conn_->connection_conf()->max_retries())
            ->start_log_errors_count(conn_->connection_conf()->start_log_errors_count())
            ->Build();

    return async_caller->Call().then([async_caller](auto r) { return r; });
  }

 private:
  std::shared_ptr<MockAsyncConnection> conn_;
  std::shared_ptr<hbase::pb::TableName> tn_;
};

std::shared_ptr<MockAsyncConnection> getAsyncConnection(
    Client &client, uint32_t operation_timeout_millis, uint32_t tries,
    std::shared_ptr<AsyncRegionLocatorBase> region_locator) {
  /* init region location and rpc channel */
  auto cpu_executor_ = std::make_shared<wangle::CPUThreadPoolExecutor>(4);
  auto io_executor_ = client.async_connection()->io_executor();
  auto retry_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(1);
  auto codec = std::make_shared<hbase::KeyValueCodec>();
  auto rpc_client = std::make_shared<RpcClient>(io_executor_, cpu_executor_, codec,
                                                AsyncBatchRpcRetryTest::test_util->conf());
  std::shared_ptr<folly::HHWheelTimer> retry_timer =
      folly::HHWheelTimer::newTimer(retry_executor_->getEventBase());

  /* init connection configuration */
  auto connection_conf = std::make_shared<ConnectionConfiguration>(
      TimeUtil::SecondsToNanos(20),                       // connect_timeout
      TimeUtil::MillisToNanos(operation_timeout_millis),  // operation_timeout
      TimeUtil::SecondsToNanos(60),                       // rpc_timeout
      TimeUtil::MillisToNanos(100),                       // pause
      tries,                                              // max retries
      1);                                                 // start log errors count

  return std::make_shared<MockAsyncConnection>(connection_conf, retry_timer, cpu_executor_,
                                               io_executor_, retry_executor_, rpc_client,
                                               region_locator);
}

template <typename ACTION>
std::vector<std::shared_ptr<hbase::Row>> getRows(std::vector<ACTION> actions) {
  std::vector<std::shared_ptr<hbase::Row>> rows;
  for (auto action : actions) {
    std::shared_ptr<hbase::Row> srow = std::make_shared<ACTION>(action);
    rows.push_back(srow);
  }
  return rows;
}

template <typename REQ, typename RESP>
std::vector<std::shared_ptr<hbase::Result>> getResults(std::vector<REQ> &actions,
                                                       std::vector<folly::Try<RESP>> &tresults) {
  std::vector<std::shared_ptr<hbase::Result>> results{};
  uint64_t num = 0;
  for (auto tresult : tresults) {
    if (tresult.hasValue()) {
      results.push_back(tresult.value());
    } else if (tresult.hasException()) {
      folly::exception_wrapper ew = tresult.exception();
      LOG(ERROR) << "Caught exception:- " << ew.what().toStdString() << " for "
                 << actions[num].row();
      throw ew;
    }
    ++num;
  }
  return results;
}

template <typename ACTION>
std::map<std::string, std::shared_ptr<RegionLocation>> getRegionLocationsAndActions(
    uint64_t num_rows, std::vector<ACTION> &actions, std::shared_ptr<Table> table) {
  std::map<std::string, std::shared_ptr<RegionLocation>> region_locations;
  for (uint64_t i = 0; i < num_rows; ++i) {
    auto row = "test" + std::to_string(i);
    ACTION action(row);
    actions.push_back(action);
    region_locations[row] = table->GetRegionLocation(row);
  }
  return region_locations;
}

void runMultiGets(std::shared_ptr<AsyncRegionLocatorBase> region_locator,
                  const std::string &table_name, bool split_regions, uint32_t tries = 3,
                  uint32_t operation_timeout_millis = 600000, uint64_t num_rows = 1000) {
  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(AsyncBatchRpcRetryTest::tableName);

  // Create a client
  Client client(*AsyncBatchRpcRetryTest::test_util->conf());

  // Get connection to HBase Table
  std::shared_ptr<Table> table = client.Table(tn);

  for (uint64_t i = 0; i < num_rows; i++) {
    table->Put(Put{"test" + std::to_string(i)}.AddColumn("d", std::to_string(i),
                                                         "value" + std::to_string(i)));
  }
  std::vector<hbase::Get> gets;
  auto region_locations = getRegionLocationsAndActions<hbase::Get>(num_rows, gets, table);

  /* set region locator */
  region_locator->set_region_location(region_locations);

  /* init hbase client connection */
  auto conn = getAsyncConnection(client, operation_timeout_millis, tries, region_locator);
  conn->Init();

  /* init retry caller factory */
  auto tableImpl =
      std::make_shared<MockRawAsyncTableImpl>(conn, std::make_shared<hbase::pb::TableName>(tn));

  std::vector<std::shared_ptr<hbase::Row>> rows = getRows<hbase::Get>(gets);
  auto tresults = tableImpl->Batch<std::shared_ptr<hbase::Row>, std::shared_ptr<Result>>(rows).get(
      milliseconds(operation_timeout_millis));
  ASSERT_TRUE(!tresults.empty()) << "tresults shouldn't be empty.";

  auto results = getResults<hbase::Get, std::shared_ptr<Result>>(gets, tresults);
  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!results.empty()) << "Results shouldn't be empty.";
  uint32_t i = 0;
  for (; i < num_rows; ++i) {
    ASSERT_TRUE(!results[i]->IsEmpty()) << "Result for Get " << gets[i].row()
                                        << " must not be empty";
    EXPECT_EQ("test" + std::to_string(i), results[i]->Row());
    EXPECT_EQ("value" + std::to_string(i), results[i]->Value("d", std::to_string(i)).value());
  }

  table->Close();
  client.Close();
  conn->Close();
}

void runMultiPuts(std::shared_ptr<AsyncRegionLocatorBase> region_locator,
                  const std::string &table_name, bool split_regions, uint32_t tries = 3,
                  uint32_t operation_timeout_millis = 600000, uint32_t num_rows = 1000) {
  // Create TableName and Row to be fetched from HBase
  auto tn = folly::to<hbase::pb::TableName>(AsyncBatchRpcRetryTest::tableName);

  // Create a client
  Client client(*AsyncBatchRpcRetryTest::test_util->conf());

  // Get connection to HBase Table
  std::shared_ptr<Table> table = client.Table(tn);

  std::vector<hbase::Put> puts;
  auto region_locations = getRegionLocationsAndActions<hbase::Put>(num_rows, puts, table);

  /* set region locator */
  region_locator->set_region_location(region_locations);

  /* init hbase client connection */
  auto conn = getAsyncConnection(client, operation_timeout_millis, tries, region_locator);
  conn->Init();

  /* init retry caller factory */
  auto tableImpl =
      std::make_shared<MockRawAsyncTableImpl>(conn, std::make_shared<hbase::pb::TableName>(tn));

  std::vector<std::shared_ptr<hbase::Row>> rows = getRows<hbase::Put>(puts);
  auto tresults = tableImpl->Batch<std::shared_ptr<hbase::Row>, std::shared_ptr<Result>>(rows).get(
      milliseconds(operation_timeout_millis));
  ASSERT_TRUE(!tresults.empty()) << "tresults shouldn't be empty.";

  auto results = getResults<hbase::Put, std::shared_ptr<Result>>(puts, tresults);
  // Test the values, should be same as in put executed on hbase shell
  ASSERT_TRUE(!results.empty()) << "Results shouldn't be empty.";

  table->Close();
  client.Close();
  conn->Close();
}

// Test successful case
TEST_F(AsyncBatchRpcRetryTest, MultiGets) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockAsyncRegionLocator>());
  runMultiGets(region_locator, "table1", false);
}

// Tests the RPC failing 3 times, then succeeding
TEST_F(AsyncBatchRpcRetryTest, HandleException) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockWrongRegionAsyncRegionLocator>(3));
  runMultiGets(region_locator, "table2", false, 5);
}

// Tests the RPC failing 4 times, throwing an exception
TEST_F(AsyncBatchRpcRetryTest, FailWithException) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockWrongRegionAsyncRegionLocator>(4));
  EXPECT_ANY_THROW(runMultiGets(region_locator, "table3", false));
}

// Tests the region location lookup failing 3 times, then succeeding
TEST_F(AsyncBatchRpcRetryTest, HandleExceptionFromRegionLocationLookup) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(3));
  runMultiGets(region_locator, "table4", false);
}

// Tests the region location lookup failing 5 times, throwing an exception
TEST_F(AsyncBatchRpcRetryTest, FailWithExceptionFromRegionLocationLookup) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(4));
  EXPECT_ANY_THROW(runMultiGets(region_locator, "table5", false, 3));
}

// Tests hitting operation timeout, thus not retrying anymore
TEST_F(AsyncBatchRpcRetryTest, FailWithOperationTimeout) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(6));
  EXPECT_ANY_THROW(runMultiGets(region_locator, "table6", false, 5, 100, 1000));
}

//////////////////////
// Test successful case
TEST_F(AsyncBatchRpcRetryTest, MultiPuts) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockAsyncRegionLocator>());
  runMultiPuts(region_locator, "table1", false);
}

// Tests the RPC failing 3 times, then succeeding
TEST_F(AsyncBatchRpcRetryTest, PutsHandleException) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockWrongRegionAsyncRegionLocator>(3));
  runMultiPuts(region_locator, "table2", false, 5);
}

// Tests the RPC failing 4 times, throwing an exception
TEST_F(AsyncBatchRpcRetryTest, PutsFailWithException) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockWrongRegionAsyncRegionLocator>(4));
  EXPECT_ANY_THROW(runMultiPuts(region_locator, "table3", false));
}

// Tests the region location lookup failing 3 times, then succeeding
TEST_F(AsyncBatchRpcRetryTest, PutsHandleExceptionFromRegionLocationLookup) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(3));
  runMultiPuts(region_locator, "table4", false);
}

// Tests the region location lookup failing 5 times, throwing an exception
TEST_F(AsyncBatchRpcRetryTest, PutsFailWithExceptionFromRegionLocationLookup) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(4));
  EXPECT_ANY_THROW(runMultiPuts(region_locator, "table5", false, 3));
}

// Tests hitting operation timeout, thus not retrying anymore
TEST_F(AsyncBatchRpcRetryTest, PutsFailWithOperationTimeout) {
  std::shared_ptr<AsyncRegionLocatorBase> region_locator(
      std::make_shared<MockFailingAsyncRegionLocator>(6));
  EXPECT_ANY_THROW(runMultiPuts(region_locator, "table6", false, 5, 100, 1000));
}

 // Test successful case
 TEST_F(AsyncBatchRpcRetryTest, MultiGetsSplitRegions) {
 std::shared_ptr<AsyncRegionLocatorBase> region_locator(
 std::make_shared<MockAsyncRegionLocator>());
 runMultiGets(region_locator, "table7", true);
 }

 // Tests the RPC failing 3 times, then succeeding
 TEST_F(AsyncBatchRpcRetryTest, HandleExceptionSplitRegions) {
 std::shared_ptr<AsyncRegionLocatorBase> region_locator(
 std::make_shared<MockWrongRegionAsyncRegionLocator>(3));
 runMultiGets(region_locator, "table8", true, 5);
 }

 // Tests the RPC failing 4 times, throwing an exception
 TEST_F(AsyncBatchRpcRetryTest, FailWithExceptionSplitRegions) {
 std::shared_ptr<AsyncRegionLocatorBase> region_locator(
 std::make_shared<MockWrongRegionAsyncRegionLocator>(4));
 EXPECT_ANY_THROW(runMultiGets(region_locator, "table9", true));
 }

 // Tests the region location lookup failing 3 times, then succeeding
 TEST_F(AsyncBatchRpcRetryTest, HandleExceptionFromRegionLocationLookupSplitRegions) {
 std::shared_ptr<AsyncRegionLocatorBase> region_locator(
 std::make_shared<MockFailingAsyncRegionLocator>(3));
 runMultiGets(region_locator, "table10", true);
 }

 // Tests the region location lookup failing 5 times, throwing an exception
 TEST_F(AsyncBatchRpcRetryTest, FailWithExceptionFromRegionLocationLookupSplitRegions) {
 std::shared_ptr<AsyncRegionLocatorBase> region_locator(
 std::make_shared<MockFailingAsyncRegionLocator>(4));
 EXPECT_ANY_THROW(runMultiGets(region_locator, "table11", true, 3));
 }

 // Tests hitting operation timeout, thus not retrying anymore
 TEST_F(AsyncBatchRpcRetryTest, FailWithOperationTimeoutSplitRegions) {
 std::shared_ptr<AsyncRegionLocatorBase> region_locator(
 std::make_shared<MockFailingAsyncRegionLocator>(6));
 EXPECT_ANY_THROW(runMultiGets(region_locator, "table12", true, 5, 100, 1000));
 }
