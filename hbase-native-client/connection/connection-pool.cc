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

#include <folly/SocketAddress.h>
#include <wangle/service/Service.h>

#include <memory>
#include <utility>

using std::mutex;
using std::unique_ptr;
using std::shared_ptr;
using hbase::pb::ServerName;
using hbase::ConnectionPool;
using hbase::HBaseService;
using folly::SharedMutexWritePriority;
using folly::SocketAddress;

ConnectionPool::ConnectionPool(
    std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor)
    : cf_(std::make_shared<ConnectionFactory>(io_executor)),
      clients_(),
      connections_(),
      map_mutex_() {}
ConnectionPool::ConnectionPool(std::shared_ptr<ConnectionFactory> cf)
    : cf_(cf), clients_(), connections_(), map_mutex_() {}

ConnectionPool::~ConnectionPool() {
  SharedMutexWritePriority::WriteHolder holder(map_mutex_);
  for (auto &item : connections_) {
    auto &con = item.second;
    con->close();
  }
  connections_.clear();
  clients_.clear();
}

std::shared_ptr<HBaseService> ConnectionPool::Get(const ServerName &sn) {
  // Try and get th cached connection.
  auto found_ptr = GetCached(sn);

  // If there's no connection then create it.
  if (found_ptr == nullptr) {
    found_ptr = GetNew(sn);
  }
  return found_ptr;
}

std::shared_ptr<HBaseService> ConnectionPool::GetCached(const ServerName &sn) {
  SharedMutexWritePriority::ReadHolder holder(map_mutex_);
  auto found = connections_.find(sn);
  if (found == connections_.end()) {
    return nullptr;
  }
  return found->second;
}

std::shared_ptr<HBaseService> ConnectionPool::GetNew(const ServerName &sn) {
  // Grab the upgrade lock. While we are double checking other readers can
  // continue on
  SharedMutexWritePriority::UpgradeHolder u_holder{map_mutex_};

  // Now check if someone else created the connection before we got the lock
  // This is safe since we hold the upgrade lock.
  // upgrade lock is more power than the reader lock.
  auto found = connections_.find(sn);
  if (found != connections_.end() && found->second != nullptr) {
    return found->second;
  } else {
    // Yeah it looks a lot like there's no connection
    SharedMutexWritePriority::WriteHolder w_holder{std::move(u_holder)};

    // Make double sure there are not stale connections hanging around.
    connections_.erase(sn);

    // Nope we are the ones who should create the new connection.
    auto client = cf_->MakeBootstrap();
    auto dispatcher = cf_->Connect(client, sn.host_name(), sn.port());
    clients_.insert(std::make_pair(sn, client));
    connections_.insert(std::make_pair(sn, dispatcher));
    return dispatcher;
  }
}

void ConnectionPool::Close(const ServerName &sn) {
  SharedMutexWritePriority::WriteHolder holder{map_mutex_};

  auto found = connections_.find(sn);
  if (found == connections_.end() || found->second == nullptr) {
    return;
  }
  auto service = found->second;
  connections_.erase(found);
}
