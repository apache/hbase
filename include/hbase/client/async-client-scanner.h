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

#include "hbase/connection/rpc-client.h"
#include "hbase/client/async-connection.h"
#include "hbase/client/async-rpc-retrying-caller-factory.h"
#include "hbase/client/async-rpc-retrying-caller.h"
#include "hbase/client/hbase-rpc-controller.h"
#include "hbase/client/raw-scan-result-consumer.h"
#include "hbase/client/region-location.h"
#include "hbase/client/request-converter.h"
#include "hbase/client/response-converter.h"
#include "hbase/client/result.h"
#include "hbase/client/scan-result-cache.h"
#include "hbase/client/scan.h"
#include "hbase/exceptions/exception.h"
#include "hbase/if/Client.pb.h"
#include "hbase/if/HBase.pb.h"
#include "hbase/utils/connection-util.h"
#include "hbase/utils/sys-util.h"
#include "hbase/utils/time-util.h"

using std::chrono::nanoseconds;
using std::chrono::milliseconds;

namespace hbase {
class OpenScannerResponse {
 public:
  OpenScannerResponse(std::shared_ptr<hbase::RpcClient> rpc_client,
                      const std::unique_ptr<Response>& resp,
                      std::shared_ptr<RegionLocation> region_location,
                      std::shared_ptr<hbase::HBaseRpcController> controller)
      : rpc_client_(rpc_client), region_location_(region_location), controller_(controller) {
    scan_resp_ = std::static_pointer_cast<pb::ScanResponse>(resp->resp_msg());
    cell_scanner_ = resp->cell_scanner();
  }
  std::shared_ptr<hbase::RpcClient> rpc_client_;
  std::shared_ptr<pb::ScanResponse> scan_resp_;
  std::shared_ptr<RegionLocation> region_location_;
  std::shared_ptr<hbase::HBaseRpcController> controller_;
  std::shared_ptr<CellScanner> cell_scanner_;
};

class AsyncClientScanner : public std::enable_shared_from_this<AsyncClientScanner> {
 public:
  template <typename... T>
  static std::shared_ptr<AsyncClientScanner> Create(T&&... all) {
    return std::shared_ptr<AsyncClientScanner>(new AsyncClientScanner(std::forward<T>(all)...));
  }

  void Start();

 private:
  // methods
  AsyncClientScanner(std::shared_ptr<AsyncConnection> conn, std::shared_ptr<Scan> scan,
                     std::shared_ptr<pb::TableName> table_name,
                     std::shared_ptr<RawScanResultConsumer> consumer, nanoseconds pause,
                     uint32_t max_retries, nanoseconds scan_timeout_nanos,
                     nanoseconds rpc_timeout_nanos, uint32_t start_log_errors_count);

  folly::Future<std::shared_ptr<OpenScannerResponse>> CallOpenScanner(
      std::shared_ptr<hbase::RpcClient> rpc_client,
      std::shared_ptr<hbase::HBaseRpcController> controller,
      std::shared_ptr<hbase::RegionLocation> loc);

  void OpenScanner();

  void StartScan(std::shared_ptr<OpenScannerResponse> resp);

  RegionLocateType GetLocateType(const Scan& scan);

 private:
  // data
  std::shared_ptr<AsyncConnection> conn_;
  std::shared_ptr<Scan> scan_;
  std::shared_ptr<pb::TableName> table_name_;
  std::shared_ptr<ScanResultCache> results_cache_;
  std::shared_ptr<RawScanResultConsumer> consumer_;
  nanoseconds pause_;
  uint32_t max_retries_;
  nanoseconds scan_timeout_nanos_;
  nanoseconds rpc_timeout_nanos_;
  uint32_t start_log_errors_count_;
  uint32_t max_attempts_;
  uint32_t open_scanner_tries_ = 0;
};
}  // namespace hbase
