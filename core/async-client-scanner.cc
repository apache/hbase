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

#include "core/async-client-scanner.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace hbase {

AsyncClientScanner::AsyncClientScanner(
    std::shared_ptr<AsyncConnection> conn, std::shared_ptr<Scan> scan,
    std::shared_ptr<pb::TableName> table_name, std::shared_ptr<RawScanResultConsumer> consumer,
    nanoseconds pause, uint32_t max_retries, nanoseconds scan_timeout_nanos,
    nanoseconds rpc_timeout_nanos, uint32_t start_log_errors_count)
    : conn_(conn),
      scan_(scan),
      table_name_(table_name),
      consumer_(consumer),
      pause_(pause),
      max_retries_(max_retries),
      scan_timeout_nanos_(scan_timeout_nanos),
      rpc_timeout_nanos_(rpc_timeout_nanos),
      start_log_errors_count_(start_log_errors_count) {
  results_cache_ = std::make_shared<ScanResultCache>();
  max_attempts_ = ConnectionUtils::Retries2Attempts(max_retries);
}

void AsyncClientScanner::Start() { OpenScanner(); }

folly::Future<std::shared_ptr<OpenScannerResponse>> AsyncClientScanner::CallOpenScanner(
    std::shared_ptr<hbase::RpcClient> rpc_client,
    std::shared_ptr<hbase::HBaseRpcController> controller,
    std::shared_ptr<hbase::RegionLocation> loc) {
  open_scanner_tries_++;

  auto preq = RequestConverter::ToScanRequest(*scan_, loc->region_name(), scan_->Caching(), false);

  auto self(shared_from_this());
  VLOG(5) << "Calling RPC Client to open the scanner for region:" << loc->DebugString();
  return rpc_client
      ->AsyncCall(loc->server_name().host_name(), loc->server_name().port(), std::move(preq),
                  security::User::defaultUser(), "ClientService")
      .then([self, loc, controller, rpc_client](const std::unique_ptr<Response>& presp) {
        VLOG(5) << "Scan Response:" << presp->DebugString();
        return std::make_shared<OpenScannerResponse>(rpc_client, presp, loc, controller);
      });
}

void AsyncClientScanner::OpenScanner() {
  auto self(shared_from_this());
  open_scanner_tries_ = 1;

  auto caller = conn_->caller_factory()
                    ->Single<std::shared_ptr<OpenScannerResponse>>()
                    ->table(table_name_)
                    ->row(scan_->StartRow())
                    ->locate_type(GetLocateType(*scan_))
                    ->rpc_timeout(rpc_timeout_nanos_)
                    ->operation_timeout(scan_timeout_nanos_)
                    ->pause(pause_)
                    ->max_retries(max_retries_)
                    ->start_log_errors_count(start_log_errors_count_)
                    ->action([&](std::shared_ptr<hbase::HBaseRpcController> controller,
                                 std::shared_ptr<hbase::RegionLocation> loc,
                                 std::shared_ptr<hbase::RpcClient> rpc_client)
                                 -> folly::Future<std::shared_ptr<OpenScannerResponse>> {
                                   return CallOpenScanner(rpc_client, controller, loc);
                                 })
                    ->Build();

  caller->Call()
      .then([this, self](std::shared_ptr<OpenScannerResponse> resp) {
        VLOG(3) << "Opened scanner with id:" << resp->scan_resp_->scanner_id()
                << ", region:" << resp->region_location_->DebugString() << ", starting scan";
        StartScan(resp);
      })
      .onError([this, self](const folly::exception_wrapper& e) {
        VLOG(3) << "Open scan request received error:" << e.what();
        consumer_->OnError(e);
      })
      .then([caller, self](const auto r) { return r; });
}

void AsyncClientScanner::StartScan(std::shared_ptr<OpenScannerResponse> resp) {
  auto self(shared_from_this());
  auto caller = conn_->caller_factory()
                    ->Scan()
                    ->scanner_id(resp->scan_resp_->scanner_id())
                    ->region_location(resp->region_location_)
                    ->scanner_lease_timeout(TimeUtil::MillisToNanos(resp->scan_resp_->ttl()))
                    ->scan(scan_)
                    ->rpc_client(resp->rpc_client_)
                    ->consumer(consumer_)
                    ->results_cache(results_cache_)
                    ->rpc_timeout(rpc_timeout_nanos_)
                    ->scan_timeout(scan_timeout_nanos_)
                    ->pause(pause_)
                    ->max_retries(max_retries_)
                    ->start_log_errors_count(start_log_errors_count_)
                    ->Build();

  caller->Start(resp->controller_, resp->scan_resp_, resp->cell_scanner_)
      .then([caller, self](const bool has_more) {
        if (has_more) {
          // open the next scanner on the next region.
          self->OpenScanner();
        } else {
          self->consumer_->OnComplete();
        }
      })
      .onError([caller, self](const folly::exception_wrapper& e) { self->consumer_->OnError(e); })
      .then([caller, self](const auto r) { return r; });
}

RegionLocateType AsyncClientScanner::GetLocateType(const Scan& scan) {
  // TODO: In C++, there is no Scan::IncludeStartRow() and Scan::IncludeStopRow().
  // When added, this method should be modified to return other RegionLocateTypes
  // (see ConnectionUtils.java #getLocateType())
  // TODO: When reversed scans are implemented, return other RegionLocateTypes
  return RegionLocateType::kCurrent;
}

}  // namespace hbase
