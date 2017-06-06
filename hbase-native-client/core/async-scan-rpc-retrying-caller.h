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

#include <folly/Conv.h>
#include <folly/ExceptionWrapper.h>
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
#include "core/raw-scan-result-consumer.h"
#include "core/region-location.h"
#include "core/request-converter.h"
#include "core/response-converter.h"
#include "core/result.h"
#include "core/scan-result-cache.h"
#include "core/scan.h"
#include "exceptions/exception.h"
#include "if/Client.pb.h"
#include "if/HBase.pb.h"
#include "utils/bytes-util.h"
#include "utils/connection-util.h"
#include "utils/optional.h"
#include "utils/sys-util.h"
#include "utils/time-util.h"

using std::chrono::nanoseconds;
using std::chrono::milliseconds;

namespace hbase {

class AsyncScanRpcRetryingCaller;

// The resume method is allowed to be called in another thread so here we also use the
// ResumerState to prevent race. The initial state is INITIALIZED, and in most cases, when back
// from onNext or onHeartbeat, we will call the prepare method to change the state to SUSPENDED,
// and when user calls resume method, we will change the state to RESUMED. But the resume method
// could be called in other thread, and in fact, user could just do this:
// controller.suspend().resume()
// This is strange but valid. This means the scan could be resumed before we call the prepare
// method to do the actual suspend work. So in the resume method, we will check if the state is
// INTIALIZED, if it is, then we will just set the state to RESUMED and return. And in prepare
// method, if the state is RESUMED already, we will just return an let the scan go on.
// Notice that, the public methods of this class is supposed to be called by upper layer only, and
// package private methods can only be called within the implementation of
// AsyncScanSingleRegionRpcRetryingCaller.
// TODO: Unlike the Java counter part, we do not do scan lease renewals in a background thread.
// Since there is also no async scan API exposed to the users, only ScanResultConsumer is the
// AsyncTableResultScanner which will only pause the scanner if the result cache is maxed. The
// application is expected to consume the scan results before the scanner lease timeout.
class ScanResumerImpl : public ScanResumer {
 public:
  explicit ScanResumerImpl(std::shared_ptr<AsyncScanRpcRetryingCaller> caller);

  virtual ~ScanResumerImpl() = default;

  /**
   * Resume the scan. You are free to call it multiple time but only the first call will take
   * effect.
   */
  void Resume() override;

  // return false if the scan has already been resumed. See the comment above for ScanResumerImpl
  // for more details.
  bool Prepare(std::shared_ptr<pb::ScanResponse> resp, int num_complete_rows);

 private:
  // INITIALIZED -> SUSPENDED -> RESUMED
  // INITIALIZED -> RESUMED
  ScanResumerState state_ = ScanResumerState::kInitialized;
  std::mutex mutex_;
  std::shared_ptr<pb::ScanResponse> resp_ = nullptr;
  int64_t num_complete_rows_ = 0;
  std::shared_ptr<AsyncScanRpcRetryingCaller> caller_;
};

class ScanControllerImpl : public ScanController {
 public:
  virtual ~ScanControllerImpl() = default;

  explicit ScanControllerImpl(std::shared_ptr<AsyncScanRpcRetryingCaller> caller);

  /**
   * Suspend the scan.
   * <p>
   * This means we will stop fetching data in background, i.e., will not call onNext any more
   * before you resume the scan.
   * @return A resumer used to resume the scan later.
   */
  std::shared_ptr<ScanResumer> Suspend();

  /**
   * Terminate the scan.
   * <p>
   * This is useful when you have got enough results and want to stop the scan in onNext method,
   * or you want to stop the scan in onHeartbeat method because it has spent too many time.
   */
  void Terminate();

  // return the current state, and set the state to DESTROYED.
  ScanControllerState Destroy();

  std::shared_ptr<ScanResumerImpl> resumer() { return resumer_; }

 private:
  void PreCheck();

  std::string DebugString(ScanControllerState state);

  std::string DebugString(ScanResumerState state);

 private:
  // Make sure the methods are only called in this thread.
  std::thread::id caller_thread_id_ = std::this_thread::get_id();
  // INITIALIZED -> SUSPENDED -> DESTROYED
  // INITIALIZED -> TERMINATED -> DESTROYED
  // INITIALIZED -> DESTROYED
  // If the state is incorrect we will throw IllegalStateException.
  ScanControllerState state_ = ScanControllerState::kInitialized;
  std::shared_ptr<ScanResumerImpl> resumer_ = nullptr;
  std::shared_ptr<AsyncScanRpcRetryingCaller> caller_;
};

class AsyncScanRpcRetryingCaller : public std::enable_shared_from_this<AsyncScanRpcRetryingCaller> {
 public:
  AsyncScanRpcRetryingCaller(std::shared_ptr<AsyncConnection> conn,
                             std::shared_ptr<folly::HHWheelTimer> retry_timer,
                             std::shared_ptr<hbase::RpcClient> rpc_client,
                             std::shared_ptr<Scan> scan, int64_t scanner_id,
                             std::shared_ptr<ScanResultCache> results_cache,
                             std::shared_ptr<RawScanResultConsumer> consumer,
                             std::shared_ptr<RegionLocation> region_location,
                             nanoseconds scanner_lease_timeout_nanos, nanoseconds pause,
                             uint32_t max_retries, nanoseconds scan_timeout_nanos,
                             nanoseconds rpc_timeout_nanos, uint32_t start_log_errors_count);

  folly::Future<bool> Start(std::shared_ptr<HBaseRpcController> controller,
                            std::shared_ptr<pb::ScanResponse> open_scan_resp,
                            const std::shared_ptr<CellScanner> cell_scanner);

 private:
  int64_t RemainingTimeNs();
  void OnComplete(std::shared_ptr<HBaseRpcController> controller,
                  std::shared_ptr<pb::ScanResponse> resp,
                  const std::shared_ptr<CellScanner> cell_scanner);

  void CompleteOrNext(std::shared_ptr<pb::ScanResponse> resp);

  void CompleteExceptionally(bool close_scanner);

  void CompleteNoMoreResults();

  void CompleteWhenNoMoreResultsInRegion();

  void CompleteWithNextStartRow(std::string row, bool inclusive);

  void UpdateNextStartRowWhenError(const Result& result);

  void CompleteWhenError(bool close_scanner);

  void OnError(const folly::exception_wrapper& e);

  bool NoMoreResultsForScan(const Scan& scan, const pb::RegionInfo& info);

  void Next();

  void Call();

  void CloseScanner();

  void ResetController(std::shared_ptr<HBaseRpcController> controller,
                       const int64_t& timeout_nanos);

 private:
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<folly::HHWheelTimer> retry_timer_;
  std::shared_ptr<hbase::RpcClient> rpc_client_;
  std::shared_ptr<Scan> scan_;
  int64_t scanner_id_;
  std::shared_ptr<ScanResultCache> results_cache_;
  std::shared_ptr<RawScanResultConsumer> consumer_;
  std::shared_ptr<RegionLocation> region_location_;
  nanoseconds scanner_lease_timeout_nanos_;
  nanoseconds pause_;
  uint32_t max_retries_;
  nanoseconds scan_timeout_nanos_;
  nanoseconds rpc_timeout_nanos_;
  uint32_t start_log_errors_count_;
  std::shared_ptr<folly::Promise<bool>> promise_;
  std::shared_ptr<HBaseRpcController> controller_;
  optional<std::string> next_start_row_when_error_ = optional<std::string>();
  bool include_next_start_row_when_error_ = true;
  uint64_t start_ns_;
  uint32_t tries_;
  std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions_;
  uint32_t max_attempts_;
  int64_t next_call_seq_ = -1L;

  friend class ScanResumerImpl;
  friend class ScanControllerImpl;
};

}  // namespace hbase
