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

template <typename CONN, typename RESP, typename RPC_CLIENT>
class SingleRequestCallerBuilder
    : public std::enable_shared_from_this<SingleRequestCallerBuilder<CONN, RESP, RPC_CLIENT>> {
 public:
  explicit SingleRequestCallerBuilder(std::shared_ptr<CONN> conn)
      : conn_(conn),
        table_name_(nullptr),
        rpc_timeout_nanos_(0),
        operation_timeout_nanos_(0),
        locate_type_(RegionLocateType::kCurrent) {}

  virtual ~SingleRequestCallerBuilder() = default;

  typedef SingleRequestCallerBuilder<CONN, RESP, RPC_CLIENT> GenenericThisType;
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

  SharedThisPtr row(const std::string& row) {
    row_ = row;
    return shared_this();
  }

  SharedThisPtr locate_type(RegionLocateType locate_type) {
    locate_type_ = locate_type;
    return shared_this();
  }

  SharedThisPtr action(Callable<RESP, RPC_CLIENT> callable) {
    callable_ = callable;
    return shared_this();
  }

  folly::Future<RESP> Call() { return Build()->Call(); }

  std::shared_ptr<AsyncSingleRequestRpcRetryingCaller<CONN, RESP, RPC_CLIENT>> Build() {
    return std::make_shared<AsyncSingleRequestRpcRetryingCaller<CONN, RESP, RPC_CLIENT>>(
        conn_, table_name_, row_, locate_type_, callable_, conn_->get_conn_conf()->GetPauseNs(),
        conn_->get_conn_conf()->GetMaxRetries(), operation_timeout_nanos_, rpc_timeout_nanos_,
        conn_->get_conn_conf()->GetStartLogErrorsCount());
  }

 private:
  SharedThisPtr shared_this() {
    return std::enable_shared_from_this<GenenericThisType>::shared_from_this();
  }

 private:
  std::shared_ptr<CONN> conn_;
  std::shared_ptr<TableName> table_name_;
  nanoseconds rpc_timeout_nanos_;
  nanoseconds operation_timeout_nanos_;
  std::string row_;
  RegionLocateType locate_type_;
  Callable<RESP, RPC_CLIENT> callable_;
};  // end of SingleRequestCallerBuilder

template <typename CONN>
class AsyncRpcRetryingCallerFactory {
 private:
  std::shared_ptr<CONN> conn_;

 public:
  explicit AsyncRpcRetryingCallerFactory(std::shared_ptr<CONN> conn) : conn_(conn) {}

  virtual ~AsyncRpcRetryingCallerFactory() = default;

  template <typename RESP, typename RPC_CLIENT = hbase::RpcClient>
  std::shared_ptr<SingleRequestCallerBuilder<CONN, RESP, RPC_CLIENT>> Single() {
    return std::make_shared<SingleRequestCallerBuilder<CONN, RESP, RPC_CLIENT>>(conn_);
  }
};

}  // namespace hbase
