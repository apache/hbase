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

#include "core/async-rpc-retrying-caller.h"

#include <folly/Format.h>
#include <folly/Logging.h>
#include <folly/futures/Unit.h>

#include "connection/rpc-client.h"
#include "core/async-connection.h"
#include "core/hbase-rpc-controller.h"
#include "core/region-location.h"
#include "core/result.h"
#include "exceptions/exception.h"
#include "if/HBase.pb.h"
#include "utils/connection-util.h"
#include "utils/sys-util.h"
#include "utils/time-util.h"

namespace hbase {

template <typename RESP>
AsyncSingleRequestRpcRetryingCaller<RESP>::AsyncSingleRequestRpcRetryingCaller(
    std::shared_ptr<AsyncConnection> conn, std::shared_ptr<hbase::pb::TableName> table_name,
    const std::string& row, RegionLocateType locate_type, Callable<RESP> callable,
    nanoseconds pause, uint32_t max_retries, nanoseconds operation_timeout_nanos,
    nanoseconds rpc_timeout_nanos, uint32_t start_log_errors_count)
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

template <typename RESP>
AsyncSingleRequestRpcRetryingCaller<RESP>::~AsyncSingleRequestRpcRetryingCaller() {}

template <typename RESP>
folly::Future<RESP> AsyncSingleRequestRpcRetryingCaller<RESP>::Call() {
  auto f = promise_->getFuture();
  LocateThenCall();
  return f;
}

template <typename RESP>
void AsyncSingleRequestRpcRetryingCaller<RESP>::LocateThenCall() {
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
                         TimeUtil::ToMillisStr(operation_timeout_nanos_) + " ms, time elapsed = " +
                         TimeUtil::ElapsedMillisStr(this->start_ns_) + " ms";
                },
                [](const std::exception& error) {});
      });
}

template <typename RESP>
void AsyncSingleRequestRpcRetryingCaller<RESP>::OnError(
    const std::exception& error, Supplier<std::string> err_msg,
    Consumer<std::exception> update_cached_location) {
  ThrowableWithExtraContext twec(std::make_shared<std::exception>(error), TimeUtil::GetNowNanos());
  exceptions_->push_back(twec);
  if (SysUtil::InstanceOf<DoNotRetryIOException, std::exception>(error) || tries_ >= max_retries_) {
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

template <typename RESP>
void AsyncSingleRequestRpcRetryingCaller<RESP>::Call(const RegionLocation& loc) {
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

template <typename RESP>
void AsyncSingleRequestRpcRetryingCaller<RESP>::CompleteExceptionally() {
  this->promise_->setException(RetriesExhaustedException(tries_ - 1, exceptions_));
}

template <typename RESP>
int64_t AsyncSingleRequestRpcRetryingCaller<RESP>::RemainingTimeNs() {
  return operation_timeout_nanos_.count() - (TimeUtil::GetNowNanos() - start_ns_);
}

template <typename RESP>
void AsyncSingleRequestRpcRetryingCaller<RESP>::ResetController(
    std::shared_ptr<HBaseRpcController> controller, const int64_t& timeout_ns) {
  controller->Reset();
  if (timeout_ns >= 0) {
    controller->set_call_timeout(
        milliseconds(std::min(static_cast<int64_t>(INT_MAX), TimeUtil::ToMillis(timeout_ns))));
  }
}

// explicit instantiations for the linker. Otherwise, you have to #include the .cc file for the
// templetized
// class definitions.
template class AsyncSingleRequestRpcRetryingCaller<std::shared_ptr<hbase::Result>>;
template class AsyncSingleRequestRpcRetryingCaller<folly::Unit>;
} /* namespace hbase */
