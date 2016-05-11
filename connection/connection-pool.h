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

#include <boost/functional/hash.hpp>
#include <folly/SharedMutex.h>
#include <mutex>
#include <unordered_map>

#include "connection/connection-factory.h"
#include "connection/service.h"
#include "if/HBase.pb.h"

namespace hbase {

/** Equals function for server name that ignores start time */
struct ServerNameEquals {

  /** equals */
  bool operator()(const hbase::pb::ServerName &lhs,
                  const hbase::pb::ServerName &rhs) const {
    return lhs.host_name() == rhs.host_name() && lhs.port() == rhs.port();
  }
};

/** Hash for ServerName that ignores the start time. */
struct ServerNameHash {
  /** hash */
  std::size_t operator()(hbase::pb::ServerName const &s) const {
    std::size_t h = 0;
    boost::hash_combine(h, s.host_name());
    boost::hash_combine(h, s.port());
    return h;
  }
};

/**
 * @brief Connection pooling for HBase rpc connection.
 *
 * This is a thread safe connection pool. It allows getting
 * a shared connection to HBase by server name. This is
 * useful for keeping a single connection no matter how many regions a
 * regionserver has on it.
 */
class ConnectionPool {
public:
  /** Create connection pool wit default connection factory */
  ConnectionPool(std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor);

  /**
   * Desctructor.
   * All connections will be close.
   * All connections will be released
   */
  ~ConnectionPool();

  /**
   * Constructor that allows specifiying the connetion factory.
   * This is useful for testing.
   */
  explicit ConnectionPool(std::shared_ptr<ConnectionFactory> cf);

  /**
   * Get a connection to the server name. Start time is ignored.
   * This can be a blocking operation for a short time.
   */
  std::shared_ptr<HBaseService> Get(const hbase::pb::ServerName &sn);

  /**
   * Close/remove a connection.
   */
  void Close(const hbase::pb::ServerName &sn);

private:
  std::shared_ptr<HBaseService> GetCached(const hbase::pb::ServerName& sn);
  std::shared_ptr<HBaseService> GetNew(const hbase::pb::ServerName& sn);
  std::unordered_map<hbase::pb::ServerName, std::shared_ptr<HBaseService>,
                     ServerNameHash, ServerNameEquals>
      connections_;
  std::unordered_map<
      hbase::pb::ServerName,
      std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>>,
      ServerNameHash, ServerNameEquals>
      clients_;
  folly::SharedMutexWritePriority map_mutex_;
  std::shared_ptr<ConnectionFactory> cf_;
};

} // namespace hbase
