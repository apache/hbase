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

#include "connection/connection-id.h"
#include "connection/connection-pool.h"
#include "connection/request.h"
#include "connection/response.h"
#include "security/user.h"

#include <google/protobuf/service.h>

#include <chrono>
#include <utility>

using hbase::security::User;
using hbase::pb::ServerName;
using hbase::Request;
using hbase::Response;
using hbase::ConnectionId;
using hbase::ConnectionPool;
using hbase::RpcConnection;
using hbase::security::User;

using google::protobuf::Message;
using std::chrono::nanoseconds;

namespace hbase {

class RpcClient {
 public:
  RpcClient(std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor, std::shared_ptr<Codec> codec,
            std::shared_ptr<Configuration> conf, nanoseconds connect_timeout = nanoseconds(0));

  virtual ~RpcClient() { Close(); }

  virtual std::unique_ptr<Response> SyncCall(const std::string &host, uint16_t port,
                                             std::unique_ptr<Request> req,
                                             std::shared_ptr<User> ticket);

  virtual std::unique_ptr<Response> SyncCall(const std::string &host, uint16_t port,
                                             std::unique_ptr<Request> req,
                                             std::shared_ptr<User> ticket,
                                             const std::string &service_name);

  virtual folly::Future<std::unique_ptr<Response>> AsyncCall(const std::string &host, uint16_t port,
                                                             std::unique_ptr<Request> req,
                                                             std::shared_ptr<User> ticket);

  virtual folly::Future<std::unique_ptr<Response>> AsyncCall(const std::string &host, uint16_t port,
                                                             std::unique_ptr<Request> req,
                                                             std::shared_ptr<User> ticket,
                                                             const std::string &service_name);

  virtual void Close();

  std::shared_ptr<ConnectionPool> connection_pool() const { return cp_; }

 private:
  std::shared_ptr<RpcConnection> GetConnection(std::shared_ptr<ConnectionId> remote_id);

 private:
  std::shared_ptr<ConnectionPool> cp_;
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor_;
  std::shared_ptr<Configuration> conf_;
};
}  // namespace hbase
