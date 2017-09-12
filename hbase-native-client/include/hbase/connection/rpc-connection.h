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

#include <memory>
#include <mutex>
#include <utility>

#include "hbase/connection/connection-factory.h"
#include "hbase/connection/connection-id.h"
#include "hbase/connection/request.h"
#include "hbase/connection/response.h"
#include "hbase/connection/service.h"

namespace hbase {

class RpcConnection : public std::enable_shared_from_this<RpcConnection> {
 public:
  RpcConnection(std::shared_ptr<ConnectionId> connection_id, std::shared_ptr<ConnectionFactory> cf)
      : connection_id_(connection_id), cf_(cf), hbase_service_(nullptr) {}

  virtual ~RpcConnection() { Close(); }

  virtual std::shared_ptr<ConnectionId> remote_id() const { return connection_id_; }

  virtual folly::Future<std::unique_ptr<Response>> SendRequest(std::unique_ptr<Request> req) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (hbase_service_ == nullptr) {
      Connect();
    }
    VLOG(5) << "Calling RpcConnection::SendRequest()";  // TODO
    return (*hbase_service_)(std::move(req));
  }

  virtual void Close() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (hbase_service_) {
      hbase_service_->close();
      hbase_service_ = nullptr;
    }
    if (client_bootstrap_) {
      client_bootstrap_ = nullptr;
    }
  }

 private:
  void Connect() {
    client_bootstrap_ = cf_->MakeBootstrap();
    auto dispatcher = cf_->Connect(shared_from_this(), client_bootstrap_, remote_id()->host(),
                                   remote_id()->port());
    hbase_service_ = std::move(dispatcher);
  }

 private:
  std::recursive_mutex mutex_;
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor_;
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor_;
  std::shared_ptr<ConnectionId> connection_id_;
  std::shared_ptr<HBaseService> hbase_service_;
  std::shared_ptr<ConnectionFactory> cf_;
  std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>> client_bootstrap_;
};
}  // namespace hbase
