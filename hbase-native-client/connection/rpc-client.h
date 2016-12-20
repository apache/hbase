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

using hbase::security::User;
using hbase::pb::ServerName;
using hbase::Request;
using hbase::Response;
using hbase::ConnectionId;
using hbase::ConnectionPool;
using hbase::RpcConnection;
using hbase::security::User;

using google::protobuf::MethodDescriptor;
using google::protobuf::RpcChannel;
using google::protobuf::Message;
using google::protobuf::RpcController;
using google::protobuf::Closure;

class RpcChannelImplementation;

namespace hbase {

class RpcClient : public std::enable_shared_from_this<RpcClient> {
  friend class RpcChannelImplementation;

 public:
  RpcClient();

  virtual ~RpcClient() { Close(); }

  virtual std::shared_ptr<Response> SyncCall(const std::string &host,
                                             uint16_t port,
                                             std::unique_ptr<Request> req,
                                             std::shared_ptr<User> ticket);

  virtual std::shared_ptr<Response> SyncCall(const std::string &host,
                                             uint16_t port,
                                             std::unique_ptr<Request> req,
                                             std::shared_ptr<User> ticket,
                                             const std::string &service_name);

  virtual folly::Future<Response> AsyncCall(const std::string &host,
                                            uint16_t port,
                                            std::unique_ptr<Request> req,
                                            std::shared_ptr<User> ticket);

  virtual folly::Future<Response> AsyncCall(const std::string &host,
                                            uint16_t port,
                                            std::unique_ptr<Request> req,
                                            std::shared_ptr<User> ticket,
                                            const std::string &service_name);

  virtual void Close();

  virtual std::shared_ptr<RpcChannel> CreateRpcChannel(
      const std::string &host, uint16_t port, std::shared_ptr<User> ticket,
      int rpc_timeout);

 private:
  void CallMethod(const MethodDescriptor *method, RpcController *controller,
                  const Message *req_msg, Message *resp_msg, Closure *done,
                  const std::string &host, uint16_t port,
                  std::shared_ptr<User> ticket);
  std::shared_ptr<RpcConnection> GetConnection(
      std::shared_ptr<ConnectionId> remote_id);

 private:
  std::shared_ptr<ConnectionPool> cp_;
};

class AbstractRpcChannel : public RpcChannel {
 public:
  AbstractRpcChannel(std::shared_ptr<RpcClient> rpc_client,
                     const std::string &host, uint16_t port,
                     std::shared_ptr<User> ticket, int rpc_timeout)
      : rpc_client_(rpc_client),
        host_(host),
        port_(port),
        ticket_(ticket),
        rpc_timeout_(rpc_timeout) {}

  virtual ~AbstractRpcChannel() = default;

 protected:
  std::shared_ptr<RpcClient> rpc_client_;
  std::string host_;
  uint16_t port_;
  std::shared_ptr<User> ticket_;
  int rpc_timeout_;
};
}  // namespace hbase
