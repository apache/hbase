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
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <chrono>
#include <memory>
#include <string>

#include "connection/rpc-client.h"
#include "core/async-rpc-retrying-caller.h"
#include "if/Client.pb.h"
#include "if/HBase.pb.h"

using hbase::pb::TableName;
using std::chrono::nanoseconds;

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

  typedef SingleRequestCallerBuilder<RESP> GenenericThisType;
  typedef std::shared_ptr<GenenericThisType> SharedThisPtr;

  SharedThisPtr table(std::shared_ptr<TableName> table_name) {
    table_name_ = table_name;
    return shared_this();
  }

  SharedThisPtr rpc_timeout(nanoseconds rpc_timeout_nanos) {
    rpc_timeout_nanos_ = rpc_timeout_nanos;
    return shared_this();
  }

  SharedThisPtr operation_timeout(nanoseconds operation_timeout_nanos) {
    operation_timeout_nanos_ = operation_timeout_nanos;
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
    return std::enable_shared_from_this<GenenericThisType>::shared_from_this();
  }

 private:
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<TableName> table_name_;
  nanoseconds rpc_timeout_nanos_;
  nanoseconds operation_timeout_nanos_;
  nanoseconds pause_;
  uint32_t max_retries_;
  uint32_t start_log_errors_count_;
  std::string row_;
  RegionLocateType locate_type_;
  Callable<RESP> callable_;
};  // end of SingleRequestCallerBuilder

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
};

}  // namespace hbase
