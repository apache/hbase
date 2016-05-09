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

#include <folly/Executor.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>
#include <zookeeper/zookeeper.h>

#include <memory>
#include <mutex>
#include <string>

#include "connection/connection-pool.h"
#include "core/meta-utils.h"
#include "core/region-location.h"
#include "serde/table-name.h"

namespace hbase {
// Forward
class Request;
class Response;
namespace pb {
class ServerName;
}

/**
 * Class that can look up and cache locations.
 */
class LocationCache {
public:
  /**
   * Constructor.
   * @param quorum_spec Where to connect for Zookeeper.
   * @param cpu_executor executor used to run non network IO based
   * continuations.
   * @param io_executor executor used to talk to the network
   */
  LocationCache(std::string quorum_spec,
                std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_exector,
                std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor);
  /**
   * Destructor.
   * This will clean up the zookeeper connections.
   */
  ~LocationCache();

  /**
   * Where is meta hosted.
   *
   * TODO: This should be a RegionLocation.
   */
  folly::Future<hbase::pb::ServerName> LocateMeta();

  /**
   * Go read meta and find out where a region is located.
   *
   * @param tn Table name of the table to look up. This object must live until
   * after the future is returned
   *
   * @param row of the table to look up. This object must live until after the
   * future is returned
   */
  folly::Future<std::shared_ptr<RegionLocation>>
  LocateFromMeta(const hbase::pb::TableName &tn, const std::string &row);

  /**
   * Remove the cached location of meta.
   */
  void InvalidateMeta();

private:
  void RefreshMetaLocation();
  hbase::pb::ServerName ReadMetaLocation();
  std::shared_ptr<RegionLocation> CreateLocation(const Response &resp);

  /* data */
  std::string quorum_spec_;
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor_;
  std::unique_ptr<folly::SharedPromise<hbase::pb::ServerName>> meta_promise_;
  std::mutex meta_lock_;
  MetaUtil meta_util_;
  ConnectionPool cp_;

  // TODO: migrate this to a smart pointer with a deleter.
  zhandle_t *zk_;
};
} // namespace hbase
