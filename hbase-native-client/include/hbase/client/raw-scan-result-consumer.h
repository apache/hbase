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
#include <folly/Logging.h>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "hbase/client/result.h"
#include "hbase/if/Client.pb.h"
#include "hbase/if/HBase.pb.h"

namespace hbase {

enum class ScanControllerState { kInitialized, kSuspended, kTerminated, kDestroyed };

enum class ScanResumerState { kInitialized, kSuspended, kResumed };

/**
 * Used to resume a scan.
 */
class ScanResumer {
 public:
  virtual ~ScanResumer() = default;

  /**
   * Resume the scan. You are free to call it multiple time but only the first call will take
   * effect.
   */
  virtual void Resume() = 0;
};

/**
 * Used to suspend or stop a scan.
 * <p>
 * Notice that, you should only call the methods below inside onNext or onHeartbeat method. A
 * IllegalStateException will be thrown if you call them at other places.
 * <p>
 * You can only call one of the methods below, i.e., call suspend or terminate(of course you are
 * free to not call them both), and the methods are not reentrant. A IllegalStateException will be
 * thrown if you have already called one of the methods.
 */
class ScanController {
 public:
  virtual ~ScanController() = default;

  /**
   * Suspend the scan.
   * <p>
   * This means we will stop fetching data in background, i.e., will not call onNext any more
   * before you resume the scan.
   * @return A resumer used to resume the scan later.
   */
  virtual std::shared_ptr<ScanResumer> Suspend() = 0;

  /**
   * Terminate the scan.
   * <p>
   * This is useful when you have got enough results and want to stop the scan in onNext method,
   * or you want to stop the scan in onHeartbeat method because it has spent too many time.
   */
  virtual void Terminate() = 0;
};

/**
 * Receives {@link Result} for an asynchronous scan.
 * <p>
 * Notice that, the {@link #onNext(Result[], ScanController)} method will be called in the thread
 * which we send request to HBase service. So if you want the asynchronous scanner fetch data from
 * HBase in background while you process the returned data, you need to move the processing work to
 * another thread to make the {@code onNext} call return immediately. And please do NOT do any time
 * consuming tasks in all methods below unless you know what you are doing.
 */
class RawScanResultConsumer {
 public:
  virtual ~RawScanResultConsumer() = default;

  /**
   * Indicate that we have receive some data.
   * @param results the data fetched from HBase service.
   * @param controller used to suspend or terminate the scan. Notice that the {@code controller}
   *          instance is only valid within scope of onNext method. You can only call its method in
   *          onNext, do NOT store it and call it later outside onNext.
   */
  virtual void OnNext(const std::vector<std::shared_ptr<Result>> &results,
                      std::shared_ptr<ScanController> controller) {}

  /**
   * Indicate that there is an heartbeat message but we have not cumulated enough cells to call
   * onNext.
   * <p>
   * This method give you a chance to terminate a slow scan operation.
   * @param controller used to suspend or terminate the scan. Notice that the {@code controller}
   *          instance is only valid within the scope of onHeartbeat method. You can only call its
   *          method in onHeartbeat, do NOT store it and call it later outside onHeartbeat.
   */
  virtual void OnHeartbeat(std::shared_ptr<ScanController> controller) {}

  /**
   * Indicate that we hit an unrecoverable error and the scan operation is terminated.
   * <p>
   * We will not call {@link #onComplete()} after calling {@link #onError(Throwable)}.
   */
  virtual void OnError(const folly::exception_wrapper &error) {}

  /**
   * Indicate that the scan operation is completed normally.
   */
  virtual void OnComplete() {}
};
}  // namespace hbase
