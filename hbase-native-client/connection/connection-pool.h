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

#include <folly/SharedMutex.h>
#include <mutex>
#include <unordered_map>

#include "connection/connection-factory.h"
#include "connection/service.h"
#include "if/HBase.pb.h"

namespace hbase {
struct ServerNameEquals {
  bool operator()(const hbase::pb::ServerName &lhs,
                  const hbase::pb::ServerName &rhs) const {
    return lhs.host_name() == rhs.host_name() && lhs.port() == rhs.port();
  }
};
struct ServerNameHash {
  std::size_t operator()(hbase::pb::ServerName const &s) const {
    std::size_t h1 = std::hash<std::string>()(s.host_name());
    std::size_t h2 = std::hash<uint32_t>()(s.port());
    return h1 ^ (h2 << 1);
  }
};

class ConnectionPool {
public:
  ConnectionPool();
  explicit ConnectionPool(std::shared_ptr<ConnectionFactory> cf);
  std::shared_ptr<HBaseService> get(const hbase::pb::ServerName &sn);
  void close(const hbase::pb::ServerName &sn);

private:
  std::shared_ptr<ConnectionFactory> cf_;
  std::unordered_map<hbase::pb::ServerName, std::shared_ptr<HBaseService>,
                     ServerNameHash, ServerNameEquals>
      connections_;
  folly::SharedMutexWritePriority map_mutex_;
};

} // namespace hbase
