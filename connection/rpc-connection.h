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
#include "connection/request.h"
#include "connection/response.h"
#include "connection/service.h"

#include <memory>
#include <utility>

using hbase::HBaseService;

namespace hbase {
class RpcConnection {
 public:
  RpcConnection(std::shared_ptr<ConnectionId> connection_id,
                std::shared_ptr<HBaseService> hbase_service)
      : connection_id_(connection_id), hbase_service_(hbase_service) {}

  virtual ~RpcConnection() { Close(); }

  virtual std::shared_ptr<ConnectionId> remote_id() const {
    return connection_id_;
  }

  virtual std::shared_ptr<HBaseService> get_service() const {
    return hbase_service_;
  }

  virtual folly::Future<Response> SendRequest(std::unique_ptr<Request> req) {
    return (*hbase_service_)(std::move(req));
  }

  virtual void Close() { hbase_service_->close(); }

 private:
  std::shared_ptr<ConnectionId> connection_id_;
  std::shared_ptr<HBaseService> hbase_service_;
};
}  // namespace hbase
