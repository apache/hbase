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

#include "core/async-scan-rpc-retrying-caller.h"

namespace hbase {

ScanResumerImpl::ScanResumerImpl(std::shared_ptr<AsyncScanRpcRetryingCaller> caller)
    : caller_(caller), mutex_() {}

void ScanResumerImpl::Resume() {
  // just used to fix findbugs warnings. In fact, if resume is called before prepare, then we
  // just return at the first if condition without loading the resp and numValidResuls field. If
  // resume is called after suspend, then it is also safe to just reference resp and
  // numValidResults after the synchronized block as no one will change it anymore.
  std::shared_ptr<pb::ScanResponse> local_resp;
  int64_t local_num_complete_rows;

  {
    std::unique_lock<std::mutex> mlock{mutex_};
    if (state_ == ScanResumerState::kInitialized) {
      // user calls this method before we call prepare, so just set the state to
      // RESUMED, the implementation will just go on.
      state_ = ScanResumerState::kResumed;
      return;
    }
    if (state_ == ScanResumerState::kResumed) {
      // already resumed, give up.
      return;
    }
    state_ = ScanResumerState::kResumed;
    local_resp = resp_;
    local_num_complete_rows = num_complete_rows_;
  }

  caller_->CompleteOrNext(local_resp);
}

bool ScanResumerImpl::Prepare(std::shared_ptr<pb::ScanResponse> resp, int num_complete_rows) {
  std::unique_lock<std::mutex> mlock(mutex_);
  if (state_ == ScanResumerState::kResumed) {
    // user calls resume before we actually suspend the scan, just continue;
    return false;
  }
  state_ = ScanResumerState::kSuspended;
  resp_ = resp;
  num_complete_rows_ = num_complete_rows;

  return true;
}

ScanControllerImpl::ScanControllerImpl(std::shared_ptr<AsyncScanRpcRetryingCaller> caller)
    : caller_(caller) {}

std::shared_ptr<ScanResumer> ScanControllerImpl::Suspend() {
  PreCheck();
  state_ = ScanControllerState::kSuspended;
  resumer_ = std::make_shared<ScanResumerImpl>(caller_);
  return resumer_;
}

void ScanControllerImpl::Terminate() {
  PreCheck();
  state_ = ScanControllerState::kTerminated;
}

// return the current state, and set the state to DESTROYED.
ScanControllerState ScanControllerImpl::Destroy() {
  ScanControllerState state = state_;
  state_ = ScanControllerState::kDestroyed;
  return state;
}

void ScanControllerImpl::PreCheck() {
  CHECK(std::this_thread::get_id() == caller_thread_id_)
      << "The current thread is" << std::this_thread::get_id() << ", expected thread is "
      << caller_thread_id_ << ", you should not call this method outside OnNext or OnHeartbeat";

  CHECK(state_ == ScanControllerState::kInitialized) << "Invalid Stopper state "
                                                     << DebugString(state_);
}

std::string ScanControllerImpl::DebugString(ScanControllerState state) {
  switch (state) {
    case ScanControllerState::kInitialized:
      return "kInitialized";
    case ScanControllerState::kSuspended:
      return "kSuspended";
    case ScanControllerState::kTerminated:
      return "kTerminated";
    case ScanControllerState::kDestroyed:
      return "kDestroyed";
    default:
      return "UNKNOWN";
  }
}

std::string ScanControllerImpl::DebugString(ScanResumerState state) {
  switch (state) {
    case ScanResumerState::kInitialized:
      return "kInitialized";
    case ScanResumerState::kSuspended:
      return "kSuspended";
    case ScanResumerState::kResumed:
      return "kResumed";
    default:
      return "UNKNOWN";
  }
}

AsyncScanRpcRetryingCaller::AsyncScanRpcRetryingCaller(
    std::shared_ptr<AsyncConnection> conn, std::shared_ptr<folly::HHWheelTimer> retry_timer,
    std::shared_ptr<hbase::RpcClient> rpc_client, std::shared_ptr<Scan> scan, int64_t scanner_id,
    std::shared_ptr<ScanResultCache> results_cache, std::shared_ptr<RawScanResultConsumer> consumer,
    std::shared_ptr<RegionLocation> region_location, nanoseconds scanner_lease_timeout_nanos,
    nanoseconds pause, uint32_t max_retries, nanoseconds scan_timeout_nanos,
    nanoseconds rpc_timeout_nanos, uint32_t start_log_errors_count)
    : conn_(conn),
      retry_timer_(retry_timer),
      rpc_client_(rpc_client),
      scan_(scan),
      scanner_id_(scanner_id),
      results_cache_(results_cache),
      consumer_(consumer),
      region_location_(region_location),
      scanner_lease_timeout_nanos_(scanner_lease_timeout_nanos),
      pause_(pause),
      max_retries_(max_retries),
      scan_timeout_nanos_(scan_timeout_nanos),
      rpc_timeout_nanos_(rpc_timeout_nanos),
      start_log_errors_count_(start_log_errors_count),
      promise_(std::make_shared<folly::Promise<bool>>()),
      tries_(1) {
  controller_ = conn_->CreateRpcController();
  start_ns_ = TimeUtil::GetNowNanos();
  max_attempts_ = ConnectionUtils::Retries2Attempts(max_retries);
  exceptions_ = std::make_shared<std::vector<ThrowableWithExtraContext>>();
}

folly::Future<bool> AsyncScanRpcRetryingCaller::Start(
    std::shared_ptr<HBaseRpcController> controller,
    std::shared_ptr<pb::ScanResponse> open_scan_resp,
    const std::shared_ptr<CellScanner> cell_scanner) {
  OnComplete(controller, open_scan_resp, cell_scanner);
  return promise_->getFuture();
}

int64_t AsyncScanRpcRetryingCaller::RemainingTimeNs() {
  return scan_timeout_nanos_.count() - (TimeUtil::GetNowNanos() - start_ns_);
}

void AsyncScanRpcRetryingCaller::OnComplete(std::shared_ptr<HBaseRpcController> controller,
                                            std::shared_ptr<pb::ScanResponse> resp,
                                            const std::shared_ptr<CellScanner> cell_scanner) {
  VLOG(5) << "Scan: OnComplete, scanner_id:" << scanner_id_;

  if (controller->Failed()) {
    OnError(controller->exception());
    return;
  }

  bool is_heartbeat = resp->has_heartbeat_message() && resp->heartbeat_message();

  int64_t num_complete_rows_before = results_cache_->num_complete_rows();
  try {
    auto raw_results = ResponseConverter::FromScanResponse(resp, cell_scanner);

    auto results = results_cache_->AddAndGet(raw_results, is_heartbeat);

    auto scan_controller = std::make_shared<ScanControllerImpl>(shared_from_this());

    if (results.size() > 0) {
      UpdateNextStartRowWhenError(*results[results.size() - 1]);
      VLOG(5) << "Calling consumer->OnNext()";
      consumer_->OnNext(results, scan_controller);
    } else if (is_heartbeat) {
      consumer_->OnHeartbeat(scan_controller);
    }

    ScanControllerState state = scan_controller->Destroy();
    if (state == ScanControllerState::kTerminated) {
      if (resp->has_more_results_in_region() && !resp->more_results_in_region()) {
        // we have more results in region but user request to stop the scan, so we need to close the
        // scanner explicitly.
        CloseScanner();
      }
      CompleteNoMoreResults();
      return;
    }

    int64_t num_complete_rows = results_cache_->num_complete_rows() - num_complete_rows_before;
    if (state == ScanControllerState::kSuspended) {
      if (scan_controller->resumer()->Prepare(resp, num_complete_rows)) {
        return;
      }
    }
  } catch (const std::runtime_error& e) {
    // We can not retry here. The server has responded normally and the call sequence has been
    // increased so a new scan with the same call sequence will cause an
    // OutOfOrderScannerNextException. Let the upper layer open a new scanner.
    LOG(WARNING) << "Received exception in reading the scan response:" << e.what();
    CompleteWhenError(true);
    return;
  }

  CompleteOrNext(resp);
}

void AsyncScanRpcRetryingCaller::CompleteOrNext(std::shared_ptr<pb::ScanResponse> resp) {
  VLOG(5) << "Scan: CompleteOrNext, scanner_id" << scanner_id_
          << ", response:" << resp->ShortDebugString();

  if (resp->has_more_results() && !resp->more_results()) {
    // RS tells us there is no more data for the whole scan
    CompleteNoMoreResults();
    return;
  }
  // TODO: Implement Scan::limit(), and check the limit here

  if (resp->has_more_results_in_region() && !resp->more_results_in_region()) {
    // TODO: check whether Scan is reversed here
    CompleteWhenNoMoreResultsInRegion();
    return;
  }
  Next();
}

void AsyncScanRpcRetryingCaller::CompleteExceptionally(bool close_scanner) {
  VLOG(5) << "Scan: CompleteExceptionally";
  results_cache_->Clear();
  if (close_scanner) {
    CloseScanner();
  }
  this->promise_->setException(RetriesExhaustedException(tries_ - 1, exceptions_));
}

void AsyncScanRpcRetryingCaller::CompleteNoMoreResults() {
  // In master code, scanners auto-close if we have exhausted the region. It may not be the case
  // in branch-1 code. If this is backported, make sure that the scanner is closed.
  VLOG(5) << "Scan: CompleteNoMoreResults, scanner_id:" << scanner_id_;
  promise_->setValue(false);
}

void AsyncScanRpcRetryingCaller::CompleteWhenNoMoreResultsInRegion() {
  VLOG(5) << "Scan: CompleteWhenNoMoreResultsInRegion, scanner_id:" << scanner_id_;
  // In master code, scanners auto-close if we have exhausted the region. It may not be the case
  // in branch-1 code. If this is backported, make sure that the scanner is closed.
  if (NoMoreResultsForScan(*scan_, region_location_->region_info())) {
    CompleteNoMoreResults();
  } else {
    CompleteWithNextStartRow(region_location_->region_info().end_key(), true);
  }
}

void AsyncScanRpcRetryingCaller::CompleteWithNextStartRow(std::string row, bool inclusive) {
  VLOG(5) << "Scan: CompleteWithNextStartRow: region scan is complete, move to next region";
  scan_->SetStartRow(row);
  // TODO: set inclusive if it is reverse scans
  promise_->setValue(true);
}

void AsyncScanRpcRetryingCaller::UpdateNextStartRowWhenError(const Result& result) {
  next_start_row_when_error_ = optional<std::string>(result.Row());
  include_next_start_row_when_error_ = result.Partial();
}

void AsyncScanRpcRetryingCaller::CompleteWhenError(bool close_scanner) {
  VLOG(5) << "Scan: CompleteWhenError, scanner_id:" << scanner_id_;
  results_cache_->Clear();
  if (close_scanner) {
    CloseScanner();
  }
  if (next_start_row_when_error_) {
    // TODO: HBASE-17583 adds include start / stop row to the Scan. Once we rebase and implement
    // those options in Scan , we can start using that here.
    scan_->SetStartRow(include_next_start_row_when_error_
                           ? *next_start_row_when_error_
                           : BytesUtil::CreateClosestRowAfter(*next_start_row_when_error_));
  }
  promise_->setValue(true);
}

void AsyncScanRpcRetryingCaller::OnError(const folly::exception_wrapper& error) {
  VLOG(5) << "Scan: OnError, scanner_id:" << scanner_id_;
  if (tries_ > start_log_errors_count_ || VLOG_IS_ON(5)) {
    LOG(WARNING) << "Call to " << region_location_->server_name().ShortDebugString()
                 << " for scanner id = " << scanner_id_ << " for "
                 << region_location_->region_info().ShortDebugString()
                 << " failed, , tries = " << tries_ << ", maxAttempts = " << max_attempts_
                 << ", timeout = " << TimeUtil::ToMillis(scan_timeout_nanos_).count()
                 << " ms, time elapsed = " << TimeUtil::ElapsedMillis(start_ns_) << " ms"
                 << error.what().toStdString();
  }

  bool scanner_closed = ExceptionUtil::IsScannerClosed(error);
  ThrowableWithExtraContext twec(error, TimeUtil::GetNowNanos());
  exceptions_->push_back(twec);
  if (tries_ >= max_retries_) {
    CompleteExceptionally(!scanner_closed);
    return;
  }

  int64_t delay_ns;
  if (scan_timeout_nanos_.count() > 0) {
    int64_t max_delay_ns = RemainingTimeNs() - ConnectionUtils::kSleepDeltaNs;
    if (max_delay_ns <= 0) {
      CompleteExceptionally(!scanner_closed);
      return;
    }
    delay_ns = std::min(max_delay_ns, ConnectionUtils::GetPauseTime(pause_.count(), tries_ - 1));
  } else {
    delay_ns = ConnectionUtils::GetPauseTime(pause_.count(), tries_ - 1);
  }

  if (scanner_closed) {
    CompleteWhenError(false);
    return;
  }

  if (ExceptionUtil::IsScannerOutOfOrder(error)) {
    CompleteWhenError(true);
    return;
  }
  if (!ExceptionUtil::ShouldRetry(error)) {
    CompleteExceptionally(true);
    return;
  }
  tries_++;

  auto self(shared_from_this());
  conn_->retry_executor()->add([&]() {
    retry_timer_->scheduleTimeoutFn(
        [self]() { self->conn_->cpu_executor()->add([&]() { self->Call(); }); },
        std::chrono::milliseconds(TimeUtil::ToMillis(delay_ns)));
  });
}

bool AsyncScanRpcRetryingCaller::NoMoreResultsForScan(const Scan& scan,
                                                      const pb::RegionInfo& info) {
  if (BytesUtil::IsEmptyStopRow(info.end_key())) {
    return true;
  }
  if (BytesUtil::IsEmptyStopRow(scan.StopRow())) {
    return false;
  }
  int32_t c = BytesUtil::CompareTo(info.end_key(), scan.StopRow());
  // 1. if our stop row is less than the endKey of the region
  // 2. if our stop row is equal to the endKey of the region and we do not include the stop row
  // for scan.
  return c > 0 ||
         (c == 0 /* && !scan.IncludeStopRow()*/);  // TODO: Scans always exclude StopRow for now.
}

void AsyncScanRpcRetryingCaller::Next() {
  VLOG(5) << "Scan: Next";
  next_call_seq_++;
  tries_ = 1;
  exceptions_->clear();
  start_ns_ = TimeUtil::GetNowNanos();
  Call();
}

void AsyncScanRpcRetryingCaller::Call() {
  VLOG(5) << "Scan: Call";
  auto self(shared_from_this());
  // As we have a call sequence for scan, it is useless to have a different rpc timeout which is
  // less than the scan timeout. If the server does not respond in time(usually this will not
  // happen as we have heartbeat now), we will get an OutOfOrderScannerNextException when
  // resending the next request and the only way to fix this is to close the scanner and open a
  // new one.
  int64_t call_timeout_nanos;
  if (scan_timeout_nanos_.count() > 0) {
    int64_t remaining_nanos = scan_timeout_nanos_.count() - (TimeUtil::GetNowNanos() - start_ns_);
    if (remaining_nanos <= 0) {
      CompleteExceptionally(true);
      return;
    }
    call_timeout_nanos = remaining_nanos;
  } else {
    call_timeout_nanos = 0L;
  }

  ResetController(controller_, call_timeout_nanos);

  auto req =
      RequestConverter::ToScanRequest(scanner_id_, scan_->Caching(), false, next_call_seq_, false);

  // do the RPC call
  rpc_client_
      ->AsyncCall(region_location_->server_name().host_name(),
                  region_location_->server_name().port(), std::move(req),
                  security::User::defaultUser(), "ClientService")
      .via(conn_->cpu_executor().get())
      .then([self, this](const std::unique_ptr<Response>& resp) {
        auto scan_resp = std::static_pointer_cast<pb::ScanResponse>(resp->resp_msg());
        return OnComplete(controller_, scan_resp, resp->cell_scanner());
      })
      .onError([self, this](const folly::exception_wrapper& e) { OnError(e); });
}

void AsyncScanRpcRetryingCaller::CloseScanner() {
  auto self(shared_from_this());
  ResetController(controller_, rpc_timeout_nanos_.count());

  VLOG(5) << "Closing scanner with scanner_id:" << folly::to<std::string>(scanner_id_);

  // Do a close scanner RPC. Fire and forget.
  auto req = RequestConverter::ToScanRequest(scanner_id_, 0, true);
  rpc_client_
      ->AsyncCall(region_location_->server_name().host_name(),
                  region_location_->server_name().port(), std::move(req),
                  security::User::defaultUser(), "ClientService")
      .onError([self, this](const folly::exception_wrapper& e) -> std::unique_ptr<Response> {
        LOG(WARNING) << "Call to " + region_location_->server_name().ShortDebugString() +
                            " for closing scanner_id = " + folly::to<std::string>(scanner_id_) +
                            " for " + region_location_->region_info().ShortDebugString() +
                            " failed, ignore, probably already closed. Exception:" +
                            e.what().toStdString();
        return nullptr;
      });
}

void AsyncScanRpcRetryingCaller::ResetController(std::shared_ptr<HBaseRpcController> controller,
                                                 const int64_t& timeout_nanos) {
  controller->Reset();
  if (timeout_nanos >= 0) {
    controller->set_call_timeout(
        milliseconds(std::min(static_cast<int64_t>(INT_MAX), TimeUtil::ToMillis(timeout_nanos))));
  }
}

}  // namespace hbase
