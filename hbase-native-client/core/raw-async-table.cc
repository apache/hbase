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
#include <utility>

#include "core/raw-async-table.h"
#include "core/request-converter.h"
#include "core/response-converter.h"

using hbase::security::User;

namespace hbase {

template <typename RESP>
std::shared_ptr<SingleRequestCallerBuilder<RESP>> RawAsyncTable::CreateCallerBuilder(
    std::string row, std::chrono::nanoseconds rpc_timeout) {
  return connection_->caller_factory()
      ->Single<RESP>()
      ->table(table_name_)
      ->row(row)
      ->rpc_timeout(rpc_timeout)
      ->operation_timeout(connection_conf_->operation_timeout())
      ->pause(connection_conf_->pause())
      ->max_retries(connection_conf_->max_retries())
      ->start_log_errors_count(connection_conf_->start_log_errors_count());
}

template <typename REQ, typename PREQ, typename PRESP, typename RESP>
folly::Future<RESP> RawAsyncTable::Call(
    std::shared_ptr<RpcClient> rpc_client, std::shared_ptr<HBaseRpcController> controller,
    std::shared_ptr<RegionLocation> loc, const REQ& req,
    const ReqConverter<std::unique_ptr<PREQ>, REQ, std::string> req_converter,
    const RespConverter<RESP, PRESP> resp_converter) {
  std::unique_ptr<PREQ> preq = req_converter(req, loc->region_name());

  // No need to make take a callable argument, it is always the same
  return rpc_client
      ->AsyncCall(loc->server_name().host_name(), loc->server_name().port(), std::move(preq),
                  User::defaultUser(), "ClientService")
      .then(
          [resp_converter](const std::unique_ptr<PRESP>& presp) { return resp_converter(*presp); });
}

folly::Future<std::shared_ptr<Result>> RawAsyncTable::Get(const hbase::Get& get) {
  auto caller =
      CreateCallerBuilder<std::shared_ptr<Result>>(get.row(), connection_conf_->read_rpc_timeout())
          ->action([=, &get](std::shared_ptr<hbase::HBaseRpcController> controller,
                             std::shared_ptr<hbase::RegionLocation> loc,
                             std::shared_ptr<hbase::RpcClient> rpc_client)
                       -> folly::Future<std::shared_ptr<hbase::Result>> {
                         return Call<hbase::Get, hbase::Request, hbase::Response,
                                     std::shared_ptr<hbase::Result>>(
                             rpc_client, controller, loc, get,
                             &hbase::RequestConverter::ToGetRequest,
                             &hbase::ResponseConverter::FromGetResponse);
                       })
          ->Build();

  // Return the Future we obtain from the call(). However, we do not want the Caller to go out of
  // context and get deallocated since the caller injects a lot of closures which capture [this, &]
  // which is use-after-free. We are just passing an identity closure capturing caller by value to
  // ensure  that the lifecycle of the Caller object is longer than the retry lambdas.
  return caller->Call().then([caller](const auto r) { return r; });
}
folly::Future<std::shared_ptr<Result>> RawAsyncTable::Increment(const hbase::Increment& incr) {
  auto caller =
      CreateCallerBuilder<std::shared_ptr<Result>>(incr.row(),
                                                   connection_conf_->write_rpc_timeout())
          ->action([=, &incr](std::shared_ptr<hbase::HBaseRpcController> controller,
                              std::shared_ptr<hbase::RegionLocation> loc,
                              std::shared_ptr<hbase::RpcClient>
                                  rpc_client) -> folly::Future<std::shared_ptr<Result>> {
            return Call<hbase::Increment, hbase::Request, hbase::Response, std::shared_ptr<Result>>(
                rpc_client, controller, loc, incr,
                &hbase::RequestConverter::IncrementToMutateRequest,
                &hbase::ResponseConverter::FromMutateResponse);
          })
          ->Build();

  return caller->Call().then([caller](const auto r) { return r; });
}

folly::Future<folly::Unit> RawAsyncTable::Put(const hbase::Put& put) {
  auto caller =
      CreateCallerBuilder<folly::Unit>(put.row(), connection_conf_->write_rpc_timeout())
          ->action([=, &put](
                       std::shared_ptr<hbase::HBaseRpcController> controller,
                       std::shared_ptr<hbase::RegionLocation> loc,
                       std::shared_ptr<hbase::RpcClient> rpc_client) -> folly::Future<folly::Unit> {
            return Call<hbase::Put, hbase::Request, hbase::Response, folly::Unit>(
                rpc_client, controller, loc, put, &hbase::RequestConverter::ToMutateRequest,
                [](const Response& r) -> folly::Unit { return folly::unit; });
          })
          ->Build();

  return caller->Call().then([caller](const auto r) { return r; });
}

folly::Future<folly::Unit> RawAsyncTable::Delete(const hbase::Delete& del) {
  auto caller =
      CreateCallerBuilder<folly::Unit>(del.row(), connection_conf_->write_rpc_timeout())
          ->action([=, &del](
                       std::shared_ptr<hbase::HBaseRpcController> controller,
                       std::shared_ptr<hbase::RegionLocation> loc,
                       std::shared_ptr<hbase::RpcClient> rpc_client) -> folly::Future<folly::Unit> {
            return Call<hbase::Delete, hbase::Request, hbase::Response, folly::Unit>(
                rpc_client, controller, loc, del, &hbase::RequestConverter::DeleteToMutateRequest,
                [](const Response& r) -> folly::Unit { return folly::unit; });
          })
          ->Build();

  return caller->Call().then([caller](const auto r) { return r; });
}

folly::Future<std::shared_ptr<Result>> RawAsyncTable::Append(const hbase::Append& append) {
  auto caller =
      CreateCallerBuilder<std::shared_ptr<Result>>(append.row(),
                                                   connection_conf_->write_rpc_timeout())
          ->action([=, &append](std::shared_ptr<hbase::HBaseRpcController> controller,
                                std::shared_ptr<hbase::RegionLocation> loc,
                                std::shared_ptr<hbase::RpcClient>
                                    rpc_client) -> folly::Future<std::shared_ptr<Result>> {
            return Call<hbase::Append, hbase::Request, hbase::Response, std::shared_ptr<Result>>(
                rpc_client, controller, loc, append,
                &hbase::RequestConverter::AppendToMutateRequest,
                &hbase::ResponseConverter::FromMutateResponse);
          })
          ->Build();

  return caller->Call().then([caller](const auto r) { return r; });
}
folly::Future<std::vector<folly::Try<std::shared_ptr<Result>>>> RawAsyncTable::Get(
    const std::vector<hbase::Get>& gets) {
  return this->Batch(gets);
}

folly::Future<std::vector<folly::Try<std::shared_ptr<Result>>>> RawAsyncTable::Batch(
    const std::vector<hbase::Get>& gets) {
  auto caller = connection_->caller_factory()
                    ->Batch()
                    ->table(table_name_)
                    ->actions(std::make_shared<std::vector<hbase::Get>>(gets))
                    ->rpc_timeout(connection_conf_->read_rpc_timeout())
                    ->operation_timeout(connection_conf_->operation_timeout())
                    ->pause(connection_conf_->pause())
                    ->max_attempts(connection_conf_->max_retries())
                    ->start_log_errors_count(connection_conf_->start_log_errors_count())
                    ->Build();

  return caller->Call().then([caller](auto r) { return r; });
}

void RawAsyncTable::Scan(const hbase::Scan& scan, std::shared_ptr<RawScanResultConsumer> consumer) {
  auto scanner = AsyncClientScanner::Create(
      connection_, SetDefaultScanConfig(scan), table_name_, consumer, connection_conf_->pause(),
      connection_conf_->max_retries(), connection_conf_->scan_timeout(),
      connection_conf_->rpc_timeout(), connection_conf_->start_log_errors_count());
  scanner->Start();
}

std::shared_ptr<hbase::Scan> RawAsyncTable::SetDefaultScanConfig(const hbase::Scan& scan) {
  // always create a new scan object as we may reset the start row later.
  auto new_scan = std::make_shared<hbase::Scan>(scan);
  if (new_scan->Caching() <= 0) {
    new_scan->SetCaching(default_scanner_caching_);
  }
  if (new_scan->MaxResultSize() <= 0) {
    new_scan->SetMaxResultSize(default_scanner_max_result_size_);
  }
  return new_scan;
}
}  // namespace hbase
