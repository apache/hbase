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
#include <unistd.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>

using hbase::RpcClient;
using hbase::AbstractRpcChannel;

namespace hbase {

class RpcChannelImplementation : public AbstractRpcChannel {
 public:
  RpcChannelImplementation(std::shared_ptr<RpcClient> rpc_client,
                           const std::string& host, uint16_t port,
                           std::shared_ptr<User> ticket, int rpc_timeout)
      : AbstractRpcChannel(rpc_client, host, port, ticket, rpc_timeout) {}

  void CallMethod(const MethodDescriptor* method, RpcController* controller,
                  const Message* request, Message* response,
                  Closure* done) override {
    rpc_client_->CallMethod(method, controller, request, response, done, host_,
                            port_, ticket_);
  }
};
}  // namespace hbase

RpcClient::RpcClient() {
  io_executor_ = std::make_shared<wangle::IOThreadPoolExecutor>(
      sysconf(_SC_NPROCESSORS_ONLN));

  cp_ = std::make_shared<ConnectionPool>(io_executor_);
}

void RpcClient::Close() {
  io_executor_->stop();
}

std::shared_ptr<Response> RpcClient::SyncCall(const std::string& host,
                                              uint16_t port,
                                              std::unique_ptr<Request> req,
                                              std::shared_ptr<User> ticket) {
  return std::make_shared<Response>(
      AsyncCall(host, port, std::move(req), ticket).get());
}

std::shared_ptr<Response> RpcClient::SyncCall(const std::string& host,
                                              uint16_t port,
                                              std::unique_ptr<Request> req,
                                              std::shared_ptr<User> ticket,
                                              const std::string& service_name) {
  return std::make_shared<Response>(
      AsyncCall(host, port, std::move(req), ticket, service_name).get());
}

folly::Future<Response> RpcClient::AsyncCall(const std::string& host,
                                             uint16_t port,
                                             std::unique_ptr<Request> req,
                                             std::shared_ptr<User> ticket) {
  auto remote_id = std::make_shared<ConnectionId>(host, port, ticket);
  return GetConnection(remote_id)->SendRequest(std::move(req));
}

folly::Future<Response> RpcClient::AsyncCall(const std::string& host,
                                             uint16_t port,
                                             std::unique_ptr<Request> req,
                                             std::shared_ptr<User> ticket,
                                             const std::string& service_name) {
  auto remote_id =
      std::make_shared<ConnectionId>(host, port, ticket, service_name);
  return GetConnection(remote_id)->SendRequest(std::move(req));
}

std::shared_ptr<RpcConnection> RpcClient::GetConnection(
    std::shared_ptr<ConnectionId> remote_id) {
  return cp_->GetConnection(remote_id);
}

std::shared_ptr<RpcChannel> RpcClient::CreateRpcChannel(
    const std::string& host, uint16_t port, std::shared_ptr<User> ticket,
    int rpc_timeout) {
  std::shared_ptr<RpcChannelImplementation> channel =
      std::make_shared<RpcChannelImplementation>(shared_from_this(), host, port,
                                                 ticket, rpc_timeout);

  /* static_pointer_cast is safe since RpcChannelImplementation derives
   * from RpcChannel, otherwise, dynamic_pointer_cast should be used. */
  return std::static_pointer_cast<RpcChannel>(channel);
}

void RpcClient::CallMethod(const MethodDescriptor* method,
                           RpcController* controller, const Message* req_msg,
                           Message* resp_msg, Closure* done,
                           const std::string& host, uint16_t port,
                           std::shared_ptr<User> ticket) {
  std::shared_ptr<Message> shared_req(const_cast<Message*>(req_msg));
  std::shared_ptr<Message> shared_resp(resp_msg);

  std::unique_ptr<Request> req =
      std::make_unique<Request>(shared_req, shared_resp, method->name());

  AsyncCall(host, port, std::move(req), ticket, method->service()->name())
      .then([done, this](Response resp) {
	  done->Run();
  });
}
