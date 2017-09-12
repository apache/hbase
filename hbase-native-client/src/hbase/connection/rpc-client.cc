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

#include "hbase/connection/rpc-client.h"

#include <folly/Format.h>
#include <folly/Logging.h>
#include <folly/futures/Future.h>
#include <unistd.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>
#include "hbase/exceptions/exception.h"

using hbase::security::User;
using std::chrono::nanoseconds;

namespace hbase {

RpcClient::RpcClient(std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor,
                     std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor,
                     std::shared_ptr<Codec> codec, std::shared_ptr<Configuration> conf,
                     nanoseconds connect_timeout)
    : io_executor_(io_executor), conf_(conf) {
  cp_ = std::make_shared<ConnectionPool>(io_executor_, cpu_executor, codec, conf, connect_timeout);
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
  return SendRequest(remote_id, std::move(req));
}

folly::Future<std::unique_ptr<Response>> RpcClient::AsyncCall(const std::string& host,
                                                              uint16_t port,
                                                              std::unique_ptr<Request> req,
                                                              std::shared_ptr<User> ticket,
                                                              const std::string& service_name) {
  auto remote_id = std::make_shared<ConnectionId>(host, port, ticket, service_name);
  return SendRequest(remote_id, std::move(req));
}

/**
 * There are two cases for ConnectionException:
 * 1. The first time connection
 * establishment, i.e. GetConnection(remote_id), AsyncSocketException being a cause.
 * 2. Writing request down the pipeline, i.e. RpcConnection::SendRequest, AsyncSocketException being
 * a cause as well.
 */
folly::Future<std::unique_ptr<Response>> RpcClient::SendRequest(
    std::shared_ptr<ConnectionId> remote_id, std::unique_ptr<Request> req) {
  try {
    return GetConnection(remote_id)
        ->SendRequest(std::move(req))
        .onError([&, this](const folly::exception_wrapper& ew) {
          VLOG(3) << folly::sformat("RpcClient Exception: {}", ew.what());
          ew.with_exception([&, this](const hbase::ConnectionException& re) {
            /* bad connection, remove it from pool. */
            cp_->Close(remote_id);
          });
          return GetFutureWithException(ew);
        });
  } catch (const ConnectionException& e) {
    CHECK(e.cause().get_exception() != nullptr);
    VLOG(3) << folly::sformat("RpcClient Exception: {}", e.cause().what());
    /* bad connection, remove it from pool. */
    cp_->Close(remote_id);
    return GetFutureWithException(e);
  }
}

template <typename EXCEPTION>
folly::Future<std::unique_ptr<Response>> RpcClient::GetFutureWithException(const EXCEPTION& e) {
  return GetFutureWithException(folly::exception_wrapper{e});
}

folly::Future<std::unique_ptr<Response>> RpcClient::GetFutureWithException(
    const folly::exception_wrapper& ew) {
  folly::Promise<std::unique_ptr<Response>> promise;
  auto future = promise.getFuture();
  promise.setException(ew);
  return future;
}

std::shared_ptr<RpcConnection> RpcClient::GetConnection(std::shared_ptr<ConnectionId> remote_id) {
  return cp_->GetConnection(remote_id);
}
}  // namespace hbase
