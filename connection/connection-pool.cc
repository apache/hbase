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

#include "connection/connection-pool.h"

#include <wangle/service/Service.h>

using std::mutex;
using std::unique_ptr;
using std::shared_ptr;
using hbase::pb::ServerName;
using wangle::ServiceFilter;
using folly::SharedMutexWritePriority;

namespace hbase {

class RemoveServiceFilter
    : public ServiceFilter<unique_ptr<Request>, Response> {

public:
  RemoveServiceFilter(std::shared_ptr<HBaseService> service, ServerName sn,
                      ConnectionPool &cp)
      : ServiceFilter<unique_ptr<Request>, Response>(service), sn_(sn),
        cp_(cp) {}

  folly::Future<folly::Unit> close() override {
    if (!released.exchange(true)) {
      return this->service_->close().then(
          [this]() { this->cp_.close(this->sn_); });
    } else {
      return folly::makeFuture();
    }
  }

  virtual bool isAvailable() override { return service_->isAvailable(); }

  folly::Future<Response> operator()(unique_ptr<Request> req) override {
    return (*this->service_)(std::move(req));
  }

private:
  std::atomic<bool> released{false};
  hbase::pb::ServerName sn_;
  ConnectionPool &cp_;
};

ConnectionPool::ConnectionPool()
    : cf_(std::make_shared<ConnectionFactory>()), connections_(), map_mutex_() {
}
ConnectionPool::ConnectionPool(std::shared_ptr<ConnectionFactory> cf)
    : cf_(cf), connections_(), map_mutex_() {}

std::shared_ptr<HBaseService> ConnectionPool::get(const ServerName &sn) {
  SharedMutexWritePriority::UpgradeHolder holder(map_mutex_);
  auto found = connections_.find(sn);
  if (found == connections_.end() || found->second == nullptr) {
    SharedMutexWritePriority::WriteHolder holder(std::move(holder));
    auto new_con = cf_->make_connection(sn.host_name(), sn.port());
    auto wrapped = std::make_shared<RemoveServiceFilter>(new_con, sn, *this);
    connections_[sn] = wrapped;
    return new_con;
  }
  return found->second;
}
void ConnectionPool::close(ServerName sn) {
  SharedMutexWritePriority::WriteHolder holder(map_mutex_);

  auto found = connections_.find(sn);
  if (found == connections_.end() || found->second == nullptr) {
    return;
  }
  auto service = found->second;
  connections_.erase(found);
}
}
