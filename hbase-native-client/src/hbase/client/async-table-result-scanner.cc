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

#include "hbase/client/async-table-result-scanner.h"

#include <vector>

namespace hbase {
AsyncTableResultScanner::AsyncTableResultScanner(int64_t max_cache_size)
    : max_cache_size_(max_cache_size) {
  closed_ = false;
  cache_size_ = 0;
}

AsyncTableResultScanner::~AsyncTableResultScanner() { Close(); }

void AsyncTableResultScanner::Close() {
  std::unique_lock<std::mutex> mlock(mutex_);
  closed_ = true;
  while (!queue_.empty()) {
    queue_.pop();
  }
  cache_size_ = 0;
  if (resumer_ != nullptr) {
    resumer_->Resume();
  }
  cond_.notify_all();
}

std::shared_ptr<Result> AsyncTableResultScanner::Next() {
  VLOG(5) << "AsyncTableResultScanner: Next()";

  std::shared_ptr<Result> result = nullptr;
  std::shared_ptr<ScanResumer> local_resumer = nullptr;
  {
    std::unique_lock<std::mutex> mlock(mutex_);
    while (queue_.empty()) {
      if (closed_) {
        return nullptr;
      }
      if (error_) {
        throw error_;
      }
      cond_.wait(mlock);
    }
    result = queue_.front();
    queue_.pop();

    cache_size_ -= EstimatedSizeWithSharedPtr(result);
    if (resumer_ != nullptr && cache_size_ <= max_cache_size_ / 2) {
      VLOG(1) << std::this_thread::get_id() << " resume scan prefetching";
      local_resumer = resumer_;
      resumer_ = nullptr;
    }
  }

  // Need to call ScanResumer::Resume() outside of the scope of the mutex. The reason is that
  // folly/wangle event loop might end up running the attached logic(.then()) at the Scan RPC
  // in the same event thread before returning from the previous call. This seems like the
  // wrong thing to do(™), but we cannot fix that now. Since the call back can end up calling
  // this::OnNext(), we should unlock the mutex.
  if (local_resumer != nullptr) {
    local_resumer->Resume();
  }
  return result;
}

void AsyncTableResultScanner::AddToCache(const std::vector<std::shared_ptr<Result>> &results) {
  VLOG(5) << "AsyncTableResultScanner: AddToCache()";
  for (const auto r : results) {
    queue_.push(r);
    cache_size_ += EstimatedSizeWithSharedPtr(r);
  }
}

template <typename T>
inline size_t AsyncTableResultScanner::EstimatedSizeWithSharedPtr(std::shared_ptr<T> t) {
  return t->EstimatedSize() + sizeof(std::shared_ptr<T>);
}

void AsyncTableResultScanner::OnNext(const std::vector<std::shared_ptr<Result>> &results,
                                     std::shared_ptr<ScanController> controller) {
  VLOG(5) << "AsyncTableResultScanner: OnNext()";
  {
    std::unique_lock<std::mutex> mlock(mutex_);
    if (closed_) {
      controller->Terminate();
      return;
    }
    AddToCache(results);

    if (cache_size_ >= max_cache_size_) {
      StopPrefetch(controller);
    }
  }
  cond_.notify_all();
}

void AsyncTableResultScanner::StopPrefetch(std::shared_ptr<ScanController> controller) {
  VLOG(1) << std::this_thread::get_id()
          << ": stop prefetching when scanning as the cache size " +
                 folly::to<std::string>(cache_size_) + " is greater than the max_cache_size " +
                 folly::to<std::string>(max_cache_size_);

  resumer_ = controller->Suspend();
  num_prefetch_stopped_++;
}

/**
 * Indicate that there is an heartbeat message but we have not cumulated enough cells to call
 * onNext.
 * <p>
 * This method give you a chance to terminate a slow scan operation.
 * @param controller used to suspend or terminate the scan. Notice that the {@code controller}
 *          instance is only valid within the scope of onHeartbeat method. You can only call its
 *          method in onHeartbeat, do NOT store it and call it later outside onHeartbeat.
 */
void AsyncTableResultScanner::OnHeartbeat(std::shared_ptr<ScanController> controller) {
  std::unique_lock<std::mutex> mlock(mutex_);
  if (closed_) {
    controller->Terminate();
  }
}

/**
 * Indicate that we hit an unrecoverable error and the scan operation is terminated.
 * <p>
 * We will not call {@link #onComplete()} after calling {@link #onError(Throwable)}.
 */
void AsyncTableResultScanner::OnError(const folly::exception_wrapper &error) {
  LOG(WARNING) << "Scanner received error" << error.what();
  std::unique_lock<std::mutex> mlock(mutex_);
  error_ = error;
  cond_.notify_all();
}

/**
 * Indicate that the scan operation is completed normally.
 */
void AsyncTableResultScanner::OnComplete() {
  std::unique_lock<std::mutex> mlock(mutex_);
  closed_ = true;
  cond_.notify_all();
}
}  // namespace hbase
