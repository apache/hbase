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

#include "core/raw-async-table.h"
#include "core/request-converter.h"
#include "core/response-converter.h"

namespace hbase {

template <typename RESP>
std::shared_ptr<SingleRequestCallerBuilder<RESP>> RawAsyncTable::CreateCallerBuilder(
    std::string row, nanoseconds rpc_timeout) {
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
    const ReqConverter<std::unique_ptr<PREQ>, REQ, std::string>& req_converter,
    const RespConverter<RESP, PRESP>& resp_converter) {
  std::unique_ptr<PREQ> preq = req_converter(req, loc->region_name());

  // No need to make take a callable argument, it is always the same
  return rpc_client
      ->AsyncCall(loc->server_name().host_name(), loc->server_name().port(), std::move(preq),
                  User::defaultUser(), "ClientService")
      .then([&](const std::unique_ptr<Response>& presp) {
        return ResponseConverter::FromGetResponse(*presp);
        // return resp_converter(*presp); // TODO this is causing SEGFAULT, figure out why
      });
}

Future<std::shared_ptr<Result>> RawAsyncTable::Get(const hbase::Get& get) {
  auto caller =
      CreateCallerBuilder<std::shared_ptr<Result>>(get.Row(), connection_conf_->read_rpc_timeout())
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

} /* namespace hbase */
