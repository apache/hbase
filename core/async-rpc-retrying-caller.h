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

#include <folly/Format.h>
#include <folly/Logging.h>
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
#include "connection/rpc-client.h"
#include "core/async-connection.h"
#include "core/hbase-rpc-controller.h"
#include "core/region-location.h"
#include "exceptions/exception.h"
#include "if/HBase.pb.h"
#include "utils/connection-util.h"
#include "utils/sys-util.h"
#include "utils/time-util.h"

using std::chrono::nanoseconds;
using std::chrono::milliseconds;

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
  AsyncSingleRequestRpcRetryingCaller(std::shared_ptr<AsyncConnection> conn,
                                      std::shared_ptr<hbase::pb::TableName> table_name,
                                      const std::string& row, RegionLocateType locate_type,
                                      Callable<RESP> callable, nanoseconds pause,
                                      uint32_t max_retries, nanoseconds operation_timeout_nanos,
                                      nanoseconds rpc_timeout_nanos,
                                      uint32_t start_log_errors_count)
      : conn_(conn),
        table_name_(table_name),
        row_(row),
        locate_type_(locate_type),
        callable_(callable),
        pause_(pause),
        max_retries_(max_retries),
        operation_timeout_nanos_(operation_timeout_nanos),
        rpc_timeout_nanos_(rpc_timeout_nanos),
        start_log_errors_count_(start_log_errors_count),
        promise_(std::make_shared<folly::Promise<RESP>>()),
        tries_(1) {
    controller_ = conn_->CreateRpcController();
    start_ns_ = TimeUtil::GetNowNanos();
    max_attempts_ = ConnectionUtils::Retries2Attempts(max_retries);
    exceptions_ = std::make_shared<std::vector<ThrowableWithExtraContext>>();
    retry_timer_ = folly::HHWheelTimer::newTimer(&event_base_);
  }

  virtual ~AsyncSingleRequestRpcRetryingCaller() {}

  folly::Future<RESP> Call() {
    auto f = promise_->getFuture();
    LocateThenCall();
    return f;
  }

 private:
  void LocateThenCall() {
    int64_t locate_timeout_ns;
    if (operation_timeout_nanos_.count() > 0) {
      locate_timeout_ns = RemainingTimeNs();
      if (locate_timeout_ns <= 0) {
        CompleteExceptionally();
        return;
      }
    } else {
      locate_timeout_ns = -1L;
    }

    conn_->region_locator()
        ->LocateRegion(*table_name_, row_, locate_type_, locate_timeout_ns)
        .then([this](std::shared_ptr<RegionLocation> loc) { Call(*loc); })
        .onError([this](const std::exception& e) {
          OnError(e,
                  [this]() -> std::string {
                    return "Locate '" + row_ + "' in " + table_name_->namespace_() + "::" +
                           table_name_->qualifier() + " failed, tries = " + std::to_string(tries_) +
                           ", maxAttempts = " + std::to_string(max_attempts_) + ", timeout = " +
                           TimeUtil::ToMillisStr(operation_timeout_nanos_) +
                           " ms, time elapsed = " + TimeUtil::ElapsedMillisStr(this->start_ns_) +
                           " ms";
                  },
                  [](const std::exception& error) {});
        });
  }

  void OnError(const std::exception& error, Supplier<std::string> err_msg,
               Consumer<std::exception> update_cached_location) {
    ThrowableWithExtraContext twec(std::make_shared<std::exception>(error),
                                   TimeUtil::GetNowNanos());
    exceptions_->push_back(twec);
    if (SysUtil::InstanceOf<DoNotRetryIOException, std::exception>(error) ||
        tries_ >= max_retries_) {
      CompleteExceptionally();
      return;
    }

    int64_t delay_ns;
    if (operation_timeout_nanos_.count() > 0) {
      int64_t max_delay_ns = RemainingTimeNs() - ConnectionUtils::kSleepDeltaNs;
      if (max_delay_ns <= 0) {
        CompleteExceptionally();
        return;
      }
      delay_ns = std::min(max_delay_ns, ConnectionUtils::GetPauseTime(pause_.count(), tries_ - 1));
    } else {
      delay_ns = ConnectionUtils::GetPauseTime(pause_.count(), tries_ - 1);
    }
    update_cached_location(error);
    tries_++;
    retry_timer_->scheduleTimeoutFn([this]() { LocateThenCall(); },
                                    milliseconds(TimeUtil::ToMillis(delay_ns)));
  }

  void Call(const RegionLocation& loc) {
    int64_t call_timeout_ns;
    if (operation_timeout_nanos_.count() > 0) {
      call_timeout_ns = this->RemainingTimeNs();
      if (call_timeout_ns <= 0) {
        this->CompleteExceptionally();
        return;
      }
      call_timeout_ns = std::min(call_timeout_ns, rpc_timeout_nanos_.count());
    } else {
      call_timeout_ns = rpc_timeout_nanos_.count();
    }

    std::shared_ptr<RpcClient> rpc_client;
    try {
      // TODO: There is no connection attempt happening here, no need to try-catch.
      rpc_client = conn_->rpc_client();
    } catch (const IOException& e) {
      OnError(e,
              [&, this]() -> std::string {
                return "Get async rpc_client to " +
                       folly::sformat("{0}:{1}", loc.server_name().host_name(),
                                      loc.server_name().port()) +
                       " for '" + row_ + "' in " + loc.DebugString() + " of " +
                       table_name_->namespace_() + "::" + table_name_->qualifier() +
                       " failed, tries = " + std::to_string(tries_) + ", maxAttempts = " +
                       std::to_string(max_attempts_) + ", timeout = " +
                       TimeUtil::ToMillisStr(this->operation_timeout_nanos_) +
                       " ms, time elapsed = " + TimeUtil::ElapsedMillisStr(this->start_ns_) + " ms";
              },
              [&, this](const std::exception& error) {
                conn_->region_locator()->UpdateCachedLocation(loc, error);
              });
      return;
    }

    ResetController(controller_, call_timeout_ns);

    callable_(controller_, std::make_shared<RegionLocation>(loc), rpc_client)
        .then([this](const RESP& resp) { this->promise_->setValue(std::move(resp)); })
        .onError([&, this](const std::exception& e) {
          OnError(e,
                  [&, this]() -> std::string {
                    return "Call to " + folly::sformat("{0}:{1}", loc.server_name().host_name(),
                                                       loc.server_name().port()) +
                           " for '" + row_ + "' in " + loc.DebugString() + " of " +
                           table_name_->namespace_() + "::" + table_name_->qualifier() +
                           " failed, tries = " + std::to_string(tries_) + ", maxAttempts = " +
                           std::to_string(max_attempts_) + ", timeout = " +
                           TimeUtil::ToMillisStr(this->operation_timeout_nanos_) +
                           " ms, time elapsed = " + TimeUtil::ElapsedMillisStr(this->start_ns_) +
                           " ms";
                  },
                  [&, this](const std::exception& error) {
                    conn_->region_locator()->UpdateCachedLocation(loc, error);
                  });
          return;
        });
  }

  void CompleteExceptionally() {
    this->promise_->setException(RetriesExhaustedException(tries_ - 1, exceptions_));
  }

  int64_t RemainingTimeNs() {
    return operation_timeout_nanos_.count() - (TimeUtil::GetNowNanos() - start_ns_);
  }

  static void ResetController(std::shared_ptr<HBaseRpcController> controller,
                              const int64_t& timeout_ns) {
    controller->Reset();
    if (timeout_ns >= 0) {
      controller->set_call_timeout(
          milliseconds(std::min(static_cast<int64_t>(INT_MAX), TimeUtil::ToMillis(timeout_ns))));
    }
  }

 private:
  folly::HHWheelTimer::UniquePtr retry_timer_;
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<hbase::pb::TableName> table_name_;
  std::string row_;
  RegionLocateType locate_type_;
  Callable<RESP> callable_;
  nanoseconds pause_;
  uint32_t max_retries_;
  nanoseconds operation_timeout_nanos_;
  nanoseconds rpc_timeout_nanos_;
  uint32_t start_log_errors_count_;
  std::shared_ptr<folly::Promise<RESP>> promise_;
  std::shared_ptr<HBaseRpcController> controller_;
  uint64_t start_ns_;
  uint32_t tries_;
  std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions_;
  uint32_t max_attempts_;
  folly::EventBase event_base_;
};

} /* namespace hbase */
