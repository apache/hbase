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

#include <folly/ExceptionWrapper.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/HHWheelTimer.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include "core/async-connection.h"
#include "core/hbase-rpc-controller.h"
#include "core/region-location.h"
#include "exceptions/exception.h"
#include "if/HBase.pb.h"

namespace hbase {

template <typename T>
using Supplier = std::function<T()>;

template <typename T>
using Consumer = std::function<void(T)>;

template <typename R, typename S, typename... I>
using ReqConverter = std::function<R(const S&, const I&...)>;

template <typename R, typename S>
using RespConverter = std::function<R(const S&)>;

template <typename RESP>
using RpcCallback = std::function<void(const RESP&)>;

template <typename REQ, typename RESP>
using RpcCall = std::function<folly::Future<std::unique_ptr<RESP>>(
    std::shared_ptr<RpcClient>, std::shared_ptr<RegionLocation>,
    std::shared_ptr<HBaseRpcController>, std::unique_ptr<REQ>)>;

template <typename RESP>
using Callable =
    std::function<folly::Future<RESP>(std::shared_ptr<HBaseRpcController>,
                                      std::shared_ptr<RegionLocation>, std::shared_ptr<RpcClient>)>;

template <typename RESP>
class AsyncSingleRequestRpcRetryingCaller {
 public:
  AsyncSingleRequestRpcRetryingCaller(
      std::shared_ptr<AsyncConnection> conn, std::shared_ptr<folly::HHWheelTimer> retry_timer,
      std::shared_ptr<hbase::pb::TableName> table_name, const std::string& row,
      RegionLocateType locate_type, Callable<RESP> callable, std::chrono::nanoseconds pause,
      uint32_t max_retries, std::chrono::nanoseconds operation_timeout_nanos,
      std::chrono::nanoseconds rpc_timeout_nanos, uint32_t start_log_errors_count);

  virtual ~AsyncSingleRequestRpcRetryingCaller();

  folly::Future<RESP> Call();

 private:
  void LocateThenCall();

  void OnError(const folly::exception_wrapper& error, Supplier<std::string> err_msg,
               Consumer<folly::exception_wrapper> update_cached_location);

  void Call(const RegionLocation& loc);

  void CompleteExceptionally();

  int64_t RemainingTimeNs();

  static void ResetController(std::shared_ptr<HBaseRpcController> controller,
                              const int64_t& timeout_ns);

 private:
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<hbase::pb::TableName> table_name_;
  std::string row_;
  RegionLocateType locate_type_;
  Callable<RESP> callable_;
  std::chrono::nanoseconds pause_;
  uint32_t max_retries_;
  std::chrono::nanoseconds operation_timeout_nanos_;
  std::chrono::nanoseconds rpc_timeout_nanos_;
  uint32_t start_log_errors_count_;
  std::shared_ptr<folly::Promise<RESP>> promise_;
  std::shared_ptr<HBaseRpcController> controller_;
  uint64_t start_ns_;
  uint32_t tries_;
  std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions_;
  uint32_t max_attempts_;
};
} /* namespace hbase */
