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

#include "connection/rpc-client.h"

#include <folly/Logging.h>
#include <unistd.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>
#include <memory>
#include <string>

using hbase::security::User;
using std::chrono::nanoseconds;

namespace hbase {

RpcClient::RpcClient(std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor,
                     std::shared_ptr<Codec> codec, std::shared_ptr<Configuration> conf,
                     nanoseconds connect_timeout)
    : io_executor_(io_executor), conf_(conf) {
  cp_ = std::make_shared<ConnectionPool>(io_executor_, codec, conf, connect_timeout);
}

void RpcClient::Close() { io_executor_->stop(); }

std::unique_ptr<Response> RpcClient::SyncCall(const std::string& host, uint16_t port,
                                              std::unique_ptr<Request> req,
                                              std::shared_ptr<User> ticket) {
  return AsyncCall(host, port, std::move(req), ticket).get();
}

std::unique_ptr<Response> RpcClient::SyncCall(const std::string& host, uint16_t port,
                                              std::unique_ptr<Request> req,
                                              std::shared_ptr<User> ticket,
                                              const std::string& service_name) {
  return AsyncCall(host, port, std::move(req), ticket, service_name).get();
}

folly::Future<std::unique_ptr<Response>> RpcClient::AsyncCall(const std::string& host,
                                                              uint16_t port,
                                                              std::unique_ptr<Request> req,
                                                              std::shared_ptr<User> ticket) {
  auto remote_id = std::make_shared<ConnectionId>(host, port, ticket);
  return GetConnection(remote_id)->SendRequest(std::move(req));
}

folly::Future<std::unique_ptr<Response>> RpcClient::AsyncCall(const std::string& host,
                                                              uint16_t port,
                                                              std::unique_ptr<Request> req,
                                                              std::shared_ptr<User> ticket,
                                                              const std::string& service_name) {
  auto remote_id = std::make_shared<ConnectionId>(host, port, ticket, service_name);
  return GetConnection(remote_id)->SendRequest(std::move(req));
}

std::shared_ptr<RpcConnection> RpcClient::GetConnection(std::shared_ptr<ConnectionId> remote_id) {
  return cp_->GetConnection(remote_id);
}
}  // namespace hbase
