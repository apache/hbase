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
#pragma once

#include <folly/Logging.h>
#include <folly/io/async/EventBase.h>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "connection/rpc-client.h"
#include "core/async-batch-rpc-retrying-caller.h"
#include "core/async-rpc-retrying-caller.h"
#include "core/async-scan-rpc-retrying-caller.h"
#include "core/raw-scan-result-consumer.h"
#include "core/region-location.h"
#include "core/row.h"
#include "core/scan-result-cache.h"
#include "core/scan.h"

#include "if/Client.pb.h"
#include "if/HBase.pb.h"

namespace hbase {

class AsyncConnection;

template <typename RESP>
class SingleRequestCallerBuilder
    : public std::enable_shared_from_this<SingleRequestCallerBuilder<RESP>> {
 public:
  explicit SingleRequestCallerBuilder(std::shared_ptr<AsyncConnection> conn,
                                      std::shared_ptr<folly::HHWheelTimer> retry_timer)
      : conn_(conn),
        retry_timer_(retry_timer),
        table_name_(nullptr),
        rpc_timeout_nanos_(conn->connection_conf()->rpc_timeout()),
        pause_(conn->connection_conf()->pause()),
        operation_timeout_nanos_(conn->connection_conf()->operation_timeout()),
        max_retries_(conn->connection_conf()->max_retries()),
        start_log_errors_count_(conn->connection_conf()->start_log_errors_count()),
        locate_type_(RegionLocateType::kCurrent) {}

  virtual ~SingleRequestCallerBuilder() = default;

  typedef SingleRequestCallerBuilder<RESP> GenericThisType;
  typedef std::shared_ptr<GenericThisType> SharedThisPtr;

  SharedThisPtr table(std::shared_ptr<pb::TableName> table_name) {
    table_name_ = table_name;
    return shared_this();
  }

  SharedThisPtr rpc_timeout(std::chrono::nanoseconds rpc_timeout_nanos) {
    rpc_timeout_nanos_ = rpc_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr operation_timeout(std::chrono::nanoseconds operation_timeout_nanos) {
    operation_timeout_nanos_ = operation_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr pause(std::chrono::nanoseconds pause) {
    pause_ = pause;
    return shared_this();
  }

  SharedThisPtr max_retries(uint32_t max_retries) {
    max_retries_ = max_retries;
    return shared_this();
  }

  SharedThisPtr start_log_errors_count(uint32_t start_log_errors_count) {
    start_log_errors_count_ = start_log_errors_count;
    return shared_this();
  }

  SharedThisPtr row(const std::string& row) {
    row_ = row;
    return shared_this();
  }

  SharedThisPtr locate_type(RegionLocateType locate_type) {
    locate_type_ = locate_type;
    return shared_this();
  }

  SharedThisPtr action(Callable<RESP> callable) {
    callable_ = callable;
    return shared_this();
  }

  folly::Future<RESP> Call() { return Build()->Call(); }

  std::shared_ptr<AsyncSingleRequestRpcRetryingCaller<RESP>> Build() {
    return std::make_shared<AsyncSingleRequestRpcRetryingCaller<RESP>>(
        conn_, retry_timer_, table_name_, row_, locate_type_, callable_, pause_, max_retries_,
        operation_timeout_nanos_, rpc_timeout_nanos_, start_log_errors_count_);
  }

 private:
  SharedThisPtr shared_this() {
    return std::enable_shared_from_this<GenericThisType>::shared_from_this();
  }

 private:
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<pb::TableName> table_name_;
  std::chrono::nanoseconds rpc_timeout_nanos_;
  std::chrono::nanoseconds operation_timeout_nanos_;
  std::chrono::nanoseconds pause_;
  uint32_t max_retries_;
  uint32_t start_log_errors_count_;
  std::string row_;
  RegionLocateType locate_type_;
  Callable<RESP> callable_;
};  // end of SingleRequestCallerBuilder

class BatchCallerBuilder : public std::enable_shared_from_this<BatchCallerBuilder> {
 public:
  explicit BatchCallerBuilder(std::shared_ptr<AsyncConnection> conn,
                              std::shared_ptr<folly::HHWheelTimer> retry_timer)
      : conn_(conn), retry_timer_(retry_timer) {}

  virtual ~BatchCallerBuilder() = default;

  typedef std::shared_ptr<BatchCallerBuilder> SharedThisPtr;

  SharedThisPtr table(std::shared_ptr<pb::TableName> table_name) {
    table_name_ = table_name;
    return shared_this();
  }

  SharedThisPtr actions(std::shared_ptr<std::vector<hbase::Get>> actions) {
    actions_ = actions;
    return shared_this();
  }

  SharedThisPtr operation_timeout(std::chrono::nanoseconds operation_timeout_nanos) {
    operation_timeout_nanos_ = operation_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr rpc_timeout(std::chrono::nanoseconds rpc_timeout_nanos) {
    rpc_timeout_nanos_ = rpc_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr pause(std::chrono::nanoseconds pause_ns) {
    pause_ns_ = pause_ns;
    return shared_this();
  }

  SharedThisPtr max_attempts(int32_t max_attempts) {
    max_attempts_ = max_attempts;
    return shared_this();
  }

  SharedThisPtr start_log_errors_count(int32_t start_log_errors_count) {
    start_log_errors_count_ = start_log_errors_count;
    return shared_this();
  }

  folly::Future<std::vector<folly::Try<std::shared_ptr<Result>>>> Call() { return Build()->Call(); }

  std::shared_ptr<AsyncBatchRpcRetryingCaller> Build() {
    return std::make_shared<AsyncBatchRpcRetryingCaller>(
        conn_, retry_timer_, table_name_, *actions_, pause_ns_, max_attempts_,
        operation_timeout_nanos_, rpc_timeout_nanos_, start_log_errors_count_);
  }

 private:
  SharedThisPtr shared_this() {
    return std::enable_shared_from_this<BatchCallerBuilder>::shared_from_this();
  }

 private:
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<hbase::pb::TableName> table_name_ = nullptr;
  std::shared_ptr<std::vector<hbase::Get>> actions_ = nullptr;
  std::chrono::nanoseconds pause_ns_;
  int32_t max_attempts_ = 0;
  std::chrono::nanoseconds operation_timeout_nanos_;
  std::chrono::nanoseconds rpc_timeout_nanos_;
  int32_t start_log_errors_count_ = 0;
};

class ScanCallerBuilder : public std::enable_shared_from_this<ScanCallerBuilder> {
 public:
  explicit ScanCallerBuilder(std::shared_ptr<AsyncConnection> conn,
                             std::shared_ptr<folly::HHWheelTimer> retry_timer)
      : conn_(conn),
        retry_timer_(retry_timer),
        rpc_timeout_nanos_(conn->connection_conf()->rpc_timeout()),
        pause_(conn->connection_conf()->pause()),
        scan_timeout_nanos_(conn->connection_conf()->scan_timeout()),
        max_retries_(conn->connection_conf()->max_retries()),
        start_log_errors_count_(conn->connection_conf()->start_log_errors_count()),
        scanner_id_(-1) {}

  virtual ~ScanCallerBuilder() = default;

  typedef ScanCallerBuilder GenericThisType;
  typedef std::shared_ptr<ScanCallerBuilder> SharedThisPtr;

  SharedThisPtr rpc_client(std::shared_ptr<hbase::RpcClient> rpc_client) {
    rpc_client_ = rpc_client;
    return shared_this();
  }

  SharedThisPtr rpc_timeout(nanoseconds rpc_timeout_nanos) {
    rpc_timeout_nanos_ = rpc_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr scan_timeout(nanoseconds scan_timeout_nanos) {
    scan_timeout_nanos_ = scan_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr scanner_lease_timeout(nanoseconds scanner_lease_timeout_nanos) {
    scanner_lease_timeout_nanos_ = scanner_lease_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr pause(nanoseconds pause) {
    pause_ = pause;
    return shared_this();
  }

  SharedThisPtr max_retries(uint32_t max_retries) {
    max_retries_ = max_retries;
    return shared_this();
  }

  SharedThisPtr start_log_errors_count(uint32_t start_log_errors_count) {
    start_log_errors_count_ = start_log_errors_count;
    return shared_this();
  }

  SharedThisPtr region_location(std::shared_ptr<RegionLocation> region_location) {
    region_location_ = region_location;
    return shared_this();
  }

  SharedThisPtr scanner_id(int64_t scanner_id) {
    scanner_id_ = scanner_id;
    return shared_this();
  }

  SharedThisPtr scan(std::shared_ptr<Scan> scan) {
    scan_ = scan;
    return shared_this();
  }

  SharedThisPtr results_cache(std::shared_ptr<ScanResultCache> results_cache) {
    results_cache_ = results_cache;
    return shared_this();
  }

  SharedThisPtr consumer(std::shared_ptr<RawScanResultConsumer> consumer) {
    consumer_ = consumer;
    return shared_this();
  }

  std::shared_ptr<AsyncScanRpcRetryingCaller> Build() {
    return std::make_shared<AsyncScanRpcRetryingCaller>(
        conn_, retry_timer_, rpc_client_, scan_, scanner_id_, results_cache_, consumer_,
        region_location_, scanner_lease_timeout_nanos_, pause_, max_retries_, scan_timeout_nanos_,
        rpc_timeout_nanos_, start_log_errors_count_);
  }

 private:
  SharedThisPtr shared_this() {
    return std::enable_shared_from_this<GenericThisType>::shared_from_this();
  }

 private:
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<hbase::RpcClient> rpc_client_;
  std::shared_ptr<Scan> scan_;
  nanoseconds rpc_timeout_nanos_;
  nanoseconds scan_timeout_nanos_;
  nanoseconds scanner_lease_timeout_nanos_;
  nanoseconds pause_;
  uint32_t max_retries_;
  uint32_t start_log_errors_count_;
  std::shared_ptr<RegionLocation> region_location_;
  int64_t scanner_id_;
  std::shared_ptr<RawScanResultConsumer> consumer_;
  std::shared_ptr<ScanResultCache> results_cache_;
};  // end of ScanCallerBuilder

class AsyncRpcRetryingCallerFactory {
 private:
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;

 public:
  explicit AsyncRpcRetryingCallerFactory(std::shared_ptr<AsyncConnection> conn,
                                         std::shared_ptr<folly::HHWheelTimer> retry_timer)
      : conn_(conn), retry_timer_(retry_timer) {}

  virtual ~AsyncRpcRetryingCallerFactory() = default;

  template <typename RESP>
  std::shared_ptr<SingleRequestCallerBuilder<RESP>> Single() {
    return std::make_shared<SingleRequestCallerBuilder<RESP>>(conn_, retry_timer_);
  }

  std::shared_ptr<BatchCallerBuilder> Batch() {
    return std::make_shared<BatchCallerBuilder>(conn_, retry_timer_);
  }

  std::shared_ptr<ScanCallerBuilder> Scan() {
    return std::make_shared<ScanCallerBuilder>(conn_, retry_timer_);
  }
};

}  // namespace hbase
