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

#include <chrono>
#include <climits>
#include <string>

#include "core/configuration.h"

using std::chrono::nanoseconds;
using std::chrono::milliseconds;

namespace hbase {

/**
 * Timeout configs.
 */
class ConnectionConfiguration {
 public:
  explicit ConnectionConfiguration(const Configuration& conf) {
    connect_timeout_ =
        ToNanos(conf.GetInt(kClientSocketConnectTimeout, kDefaultClientSocketConnectTimeout));
    meta_operation_timeout_ =
        ToNanos(conf.GetLong(kClientMetaOperationTimeout, kDefaultClientOperationTimeout));
    operation_timeout_ =
        ToNanos(conf.GetLong(kClientOperationTimeout, kDefaultClientOperationTimeout));
    rpc_timeout_ = ToNanos(conf.GetLong(kRpcTimeout, kDefaultRpcTimeout));
    read_rpc_timeout_ = ToNanos(conf.GetLong(kRpcReadTimeout, ToMillis(rpc_timeout_)));
    write_rpc_timeout_ = ToNanos(conf.GetLong(kRpcWriteTimeout, ToMillis(rpc_timeout_)));
    pause_ = ToNanos(conf.GetLong(kClientPause, kDefaultClientPause));
    max_retries_ = conf.GetInt(kClientRetriesNumber, kDefaultClientRetriesNumber);
    start_log_errors_count_ =
        conf.GetInt(kStartLogErrorsAfterCount, kDefaultStartLogErrorsAfterCount);
    scan_timeout_ =
        ToNanos(conf.GetLong(kClientScannerTimeoutPeriod, kDefaultClientScannerTimeoutPeriod));
    scanner_caching_ = conf.GetInt(kClientScannerCaching, kDefaultClientScannerCaching);
    scanner_max_result_size_ =
        conf.GetLong(kClientScannerMaxResultsSize, kDefaultClientScannerMaxResultsSize);
  }

  // Used by tests
  ConnectionConfiguration(nanoseconds connect_timeout, nanoseconds operation_timeout,
                          nanoseconds rpc_timeout, nanoseconds pause, uint32_t max_retries,
                          uint32_t start_log_errors_count)
      : connect_timeout_(connect_timeout),
        operation_timeout_(operation_timeout),
        meta_operation_timeout_(operation_timeout),
        rpc_timeout_(rpc_timeout),
        read_rpc_timeout_(rpc_timeout),
        write_rpc_timeout_(rpc_timeout),
        pause_(pause),
        max_retries_(max_retries),
        start_log_errors_count_(start_log_errors_count) {}

  nanoseconds connect_timeout() const { return connect_timeout_; }

  nanoseconds meta_operation_timeout() const { return meta_operation_timeout_; }

  // timeout for a whole operation such as get, put or delete. Notice that scan will not be effected
  // by this value, see scanTimeoutNs.
  nanoseconds operation_timeout() const { return operation_timeout_; }

  // timeout for each rpc request. Can be overridden by a more specific config, such as
  // readRpcTimeout or writeRpcTimeout.
  nanoseconds rpc_timeout() const { return rpc_timeout_; }

  // timeout for each read rpc request
  nanoseconds read_rpc_timeout() const { return read_rpc_timeout_; }

  // timeout for each write rpc request
  nanoseconds write_rpc_timeout() const { return write_rpc_timeout_; }

  nanoseconds pause() const { return pause_; }

  uint32_t max_retries() const { return max_retries_; }

  /** How many retries are allowed before we start to log */
  uint32_t start_log_errors_count() const { return start_log_errors_count_; }

  // The scan timeout is used as operation timeout for every
  // operations in a scan, such as openScanner or next.
  nanoseconds scan_timeout() const { return scan_timeout_; }

  uint32_t scanner_caching() const { return scanner_caching_; }

  uint64_t scanner_max_result_size() const { return scanner_max_result_size_; }

 private:
  /** Parameter name for HBase client CPU thread pool size. Defaults to (2 * num cpus) */
  static constexpr const char* kClientSocketConnectTimeout =
      "hbase.ipc.client.socket.timeout.connect";
  /** Parameter name for HBase client CPU thread pool size. Defaults to (2 * num cpus) */
  static constexpr const uint32_t kDefaultClientSocketConnectTimeout = 10000;  // 10 secs

  /** Parameter name for HBase client operation timeout. */
  static constexpr const char* kClientOperationTimeout = "hbase.client.operation.timeout";

  /** Parameter name for HBase client meta operation timeout. */
  static constexpr const char* kClientMetaOperationTimeout = "hbase.client.meta.operation.timeout";

  /** Default HBase client operation timeout, which is tantamount to a blocking call */
  static constexpr const uint32_t kDefaultClientOperationTimeout = 1200000;

  /** timeout for each RPC */
  static constexpr const char* kRpcTimeout = "hbase.rpc.timeout";

  /** timeout for each read RPC */
  static constexpr const char* kRpcReadTimeout = "hbase.rpc.read.timeout";

  /** timeout for each write RPC */
  static constexpr const char* kRpcWriteTimeout = "hbase.rpc.write.timeout";

  static constexpr const uint32_t kDefaultRpcTimeout = 60000;

  /**
    * Parameter name for client pause value, used mostly as value to wait
    * before running a retry of a failed get, region lookup, etc.
    */
  static constexpr const char* kClientPause = "hbase.client.pause";

  static constexpr const uint64_t kDefaultClientPause = 100;

  /**
   * Parameter name for maximum retries, used as maximum for all retryable
   * operations such as fetching of the root region from root region server,
   * getting a cell's value, starting a row update, etc.
   */
  static constexpr const char* kClientRetriesNumber = "hbase.client.retries.number";

  static constexpr const uint32_t kDefaultClientRetriesNumber = 31;

  /**
    * Configure the number of failures after which the client will start logging. A few failures
    * is fine: region moved, then is not opened, then is overloaded. We try to have an acceptable
    * heuristic for the number of errors we don't log. 9 was chosen because we wait for 1s at
    * this stage.
    */
  static constexpr const char* kStartLogErrorsAfterCount = "hbase.client.start.log.errors.counter";
  static constexpr const uint32_t kDefaultStartLogErrorsAfterCount = 9;

  /** The client scanner timeout period in milliseconds. */
  static constexpr const char* kClientScannerTimeoutPeriod = "hbase.client.scanner.timeout.period";

  static constexpr const uint32_t kDefaultClientScannerTimeoutPeriod = 60000;

  /**
   * Parameter name to set the default scanner caching for all clients.
   */
  static constexpr const char* kClientScannerCaching = "hbase.client.scanner.caching";

  static constexpr const uint32_t kDefaultClientScannerCaching = INT_MAX;

  /**
   * Parameter name for maximum number of bytes returned when calling a scanner's next method.
   * Controlled by the client.
   */
  static constexpr const char* kClientScannerMaxResultsSize =
      "hbase.client.scanner.max.result.size";

  /**
   * Maximum number of bytes returned when calling a scanner's next method.
   * Note that when a single row is larger than this limit the row is still
   * returned completely.
   *
   * The default value is 2MB.
   */
  static constexpr const uint64_t kDefaultClientScannerMaxResultsSize = 2 * 1024 * 1024;

  nanoseconds connect_timeout_;
  nanoseconds meta_operation_timeout_;
  nanoseconds operation_timeout_;
  nanoseconds rpc_timeout_;
  nanoseconds read_rpc_timeout_;
  nanoseconds write_rpc_timeout_;
  nanoseconds pause_;
  uint32_t max_retries_;
  uint32_t start_log_errors_count_;
  nanoseconds scan_timeout_;
  uint32_t scanner_caching_;
  uint64_t scanner_max_result_size_;

  static nanoseconds ToNanos(const uint64_t& millis) {
    return std::chrono::duration_cast<nanoseconds>(milliseconds(millis));
  }

  static uint64_t ToMillis(const nanoseconds& nanos) {
    return std::chrono::duration_cast<milliseconds>(nanos).count();
  }
};

}  // namespace hbase
