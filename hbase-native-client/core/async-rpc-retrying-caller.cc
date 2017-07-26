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

#include <folly/Conv.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Format.h>
#include <folly/Logging.h>
#include <folly/Unit.h>

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

using folly::exception_wrapper;

namespace hbase {

template <typename RESP>
AsyncSingleRequestRpcRetryingCaller<RESP>::AsyncSingleRequestRpcRetryingCaller(
    std::shared_ptr<AsyncConnection> conn, std::shared_ptr<folly::HHWheelTimer> retry_timer,
    std::shared_ptr<hbase::pb::TableName> table_name, const std::string& row,
    RegionLocateType locate_type, Callable<RESP> callable, std::chrono::nanoseconds pause,
    uint32_t max_retries, std::chrono::nanoseconds operation_timeout_nanos,
    std::chrono::nanoseconds rpc_timeout_nanos, uint32_t start_log_errors_count)
    : conn_(conn),
      retry_timer_(retry_timer),
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
      .onError([this](const exception_wrapper& e) {
        OnError(e,
                [this, e]() -> std::string {
                  return "Locate '" + row_ + "' in " + table_name_->namespace_() + "::" +
                         table_name_->qualifier() + " failed with e.what()=" +
                         e.what().toStdString() + ", tries = " + std::to_string(tries_) +
                         ", maxAttempts = " + std::to_string(max_attempts_) + ", timeout = " +
                         TimeUtil::ToMillisStr(operation_timeout_nanos_) + " ms, time elapsed = " +
                         TimeUtil::ElapsedMillisStr(this->start_ns_) + " ms";
                },
                [](const exception_wrapper& error) {});
      });
}

template <typename RESP>
void AsyncSingleRequestRpcRetryingCaller<RESP>::OnError(
    const exception_wrapper& error, Supplier<std::string> err_msg,
    Consumer<exception_wrapper> update_cached_location) {
  ThrowableWithExtraContext twec(error, TimeUtil::GetNowNanos());
  exceptions_->push_back(twec);
  if (!ExceptionUtil::ShouldRetry(error) || tries_ >= max_retries_) {
    CompleteExceptionally();
    return;
  }

  if (tries_ > start_log_errors_count_) {
    LOG(WARNING) << err_msg();
  } else {
    VLOG(1) << err_msg();
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

  /*
   * The HHWheelTimer::scheduleTimeout() fails with an assertion from
   * EventBase::isInEventBaseThread() if we execute the schedule in a random thread, or one of
   * the IOThreadPool threads (with num threads > 1). I think there is a bug there in using retry
   * timer from IOThreadPool threads. It only works when executed from a single-thread pool
   * (retry_executor() is). However, the scheduled "work" which is the LocateThenCall() should
   * still happen in a thread pool, that is why we are submitting the work to the CPUThreadPool.
   * IOThreadPool cannot be used without fixing the blocking call that we do at TCP connection
   * establishment time (see ConnectionFactory::Connect()), otherwise, the IOThreadPool thread
   * just hangs because it deadlocks itself.
   */
  conn_->retry_executor()->add([&]() {
    retry_timer_->scheduleTimeoutFn(
        [this]() { conn_->cpu_executor()->add([&]() { LocateThenCall(); }); },
        std::chrono::milliseconds(TimeUtil::ToMillis(delay_ns)));
  });
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

  rpc_client = conn_->rpc_client();

  ResetController(controller_, call_timeout_ns);

  // TODO: RegionLocation should propagate through these method chains as a shared_ptr.
  // Otherwise, it may get deleted underneat us. We are just copying for now.
  auto loc_ptr = std::make_shared<RegionLocation>(loc);
  callable_(controller_, loc_ptr, rpc_client)
      .then([loc_ptr, this](const RESP& resp) { this->promise_->setValue(std::move(resp)); })
      .onError([&, loc_ptr, this](const exception_wrapper& e) {
        OnError(
            e,
            [&, this, e]() -> std::string {
              return "Call to " + folly::sformat("{0}:{1}", loc_ptr->server_name().host_name(),
                                                 loc_ptr->server_name().port()) +
                     " for '" + row_ + "' in " + loc_ptr->DebugString() + " of " +
                     table_name_->namespace_() + "::" + table_name_->qualifier() +
                     " failed with e.what()=" + e.what().toStdString() + ", tries = " +
                     std::to_string(tries_) + ", maxAttempts = " + std::to_string(max_attempts_) +
                     ", timeout = " + TimeUtil::ToMillisStr(this->operation_timeout_nanos_) +
                     " ms, time elapsed = " + TimeUtil::ElapsedMillisStr(this->start_ns_) + " ms";
            },
            [&, this](const exception_wrapper& error) {
              conn_->region_locator()->UpdateCachedLocation(*loc_ptr, error);
            });
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
    controller->set_call_timeout(std::chrono::milliseconds(
        std::min(static_cast<int64_t>(INT_MAX), TimeUtil::ToMillis(timeout_ns))));
  }
}

// explicit instantiations for the linker. Otherwise, you have to #include the .cc file for the
// templetized
// class definitions.
class OpenScannerResponse;
template class AsyncSingleRequestRpcRetryingCaller<std::shared_ptr<hbase::Result>>;
template class AsyncSingleRequestRpcRetryingCaller<folly::Unit>;
template class AsyncSingleRequestRpcRetryingCaller<std::shared_ptr<OpenScannerResponse>>;
template class AsyncSingleRequestRpcRetryingCaller<bool>;
} /* namespace hbase */
