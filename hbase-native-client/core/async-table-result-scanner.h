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
#include <folly/Logging.h>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "core/raw-scan-result-consumer.h"
#include "core/result-scanner.h"
#include "core/result.h"
#include "if/Client.pb.h"
#include "if/HBase.pb.h"

namespace hbase {

class AsyncTableResultScanner : public ResultScanner, public RawScanResultConsumer {
 public:
  explicit AsyncTableResultScanner(int64_t max_cache_size);

  virtual ~AsyncTableResultScanner();

  void Close() override;

  std::shared_ptr<Result> Next() override;

  void OnNext(const std::vector<std::shared_ptr<Result>> &results,
              std::shared_ptr<ScanController> controller) override;

  /**
   * Indicate that there is an heartbeat message but we have not cumulated enough cells to call
   * onNext.
   * <p>
   * This method give you a chance to terminate a slow scan operation.
   * @param controller used to suspend or terminate the scan. Notice that the {@code controller}
   *          instance is only valid within the scope of onHeartbeat method. You can only call its
   *          method in onHeartbeat, do NOT store it and call it later outside onHeartbeat.
   */
  void OnHeartbeat(std::shared_ptr<ScanController> controller) override;

  /**
   * Indicate that we hit an unrecoverable error and the scan operation is terminated.
   * <p>
   * We will not call {@link #onComplete()} after calling {@link #onError(Throwable)}.
   */
  void OnError(const folly::exception_wrapper &error) override;

  /**
   * Indicate that the scan operation is completed normally.
   */
  void OnComplete() override;

  // For testing
  uint32_t num_prefetch_stopped() { return num_prefetch_stopped_; }

 private:
  void AddToCache(const std::vector<std::shared_ptr<Result>> &results);

  template <typename T>
  inline size_t EstimatedSizeWithSharedPtr(std::shared_ptr<T> t);

  void StopPrefetch(std::shared_ptr<ScanController> controller);

 private:
  std::queue<std::shared_ptr<Result>> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
  folly::exception_wrapper error_;
  int64_t cache_size_;
  int64_t max_cache_size_;
  bool closed_;
  std::shared_ptr<ScanResumer> resumer_ = nullptr;
  uint32_t num_prefetch_stopped_ = 0;
};
}  // namespace hbase
