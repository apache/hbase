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

#include <folly/ExceptionWrapper.h>
#include <folly/Executor.h>
#include <folly/SharedMutex.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>
#include <zookeeper/zookeeper.h>

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "connection/connection-pool.h"
#include "core/async-region-locator.h"
#include "core/configuration.h"
#include "core/meta-utils.h"
#include "core/region-location.h"
#include "serde/table-name.h"
#include "zk-util.h"

namespace hbase {
// Forward
class Request;
class Response;
namespace pb {
class ServerName;
class TableName;
}

/** Equals function for TableName (uses namespace and table name) */
struct TableNameEquals {
  /** equals */
  bool operator()(const hbase::pb::TableName &lht, const hbase::pb::TableName &rht) const {
    return lht.namespace_() == rht.namespace_() && lht.qualifier() == rht.qualifier();
  }
};

/** Hash for TableName. */
struct TableNameHash {
  /** hash */
  std::size_t operator()(hbase::pb::TableName const &t) const {
    std::size_t h = 0;
    boost::hash_combine(h, t.namespace_());
    boost::hash_combine(h, t.qualifier());
    return h;
  }
};

// typedefs for location cache
typedef std::map<std::string, std::shared_ptr<RegionLocation>> PerTableLocationMap;
typedef std::unordered_map<hbase::pb::TableName, std::shared_ptr<PerTableLocationMap>,
                           TableNameHash, TableNameEquals>
    RegionLocationMap;

/**
 * Class that can look up and cache locations.
 */
class LocationCache : public AsyncRegionLocator {
 public:
  /**
   * Constructor.
   * @param conf Configuration instance to fetch Zookeeper Quorum and Zookeeper Znode.
   * @param cpu_executor executor used to run non network IO based
   * continuations.
   * @param io_executor executor used to talk to the network
   */
  LocationCache(std::shared_ptr<hbase::Configuration> conf,
                std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor,
                std::shared_ptr<ConnectionPool> cp);
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
   * Go read meta and find out where a region is located. Most users should
   * never call this method directly and should use LocateRegion() instead.
   *
   * @param tn Table name of the table to look up. This object must live until
   * after the future is returned
   *
   * @param row of the table to look up. This object must live until after the
   * future is returned
   */
  folly::Future<std::shared_ptr<RegionLocation>> LocateFromMeta(const hbase::pb::TableName &tn,
                                                                const std::string &row);

  /**
   * The only method clients should use for meta lookups. If corresponding
   * location is cached, it's returned from the cache, otherwise lookup
   * in meta table is done, location is cached and then returned.
   * It's expected that tiny fraction of invocations incurs meta scan.
   * This method is to look up non-meta regions; use LocateMeta() to get the
   * location of hbase:meta region.
   *
   * @param tn Table name of the table to look up. This object must live until
   * after the future is returned
   *
   * @param row of the table to look up. This object must live until after the
   * future is returned
   */
  virtual folly::Future<std::shared_ptr<RegionLocation>> LocateRegion(
      const hbase::pb::TableName &tn, const std::string &row,
      const RegionLocateType locate_type = RegionLocateType::kCurrent,
      const int64_t locate_ns = 0) override;

  /**
   * Remove the cached location of meta.
   */
  std::shared_ptr<folly::SharedPromise<hbase::pb::ServerName>> InvalidateMeta();

  /**
   * Return cached region location corresponding to this row,
   * nullptr if this location isn't cached.
   */
  std::shared_ptr<RegionLocation> GetCachedLocation(const hbase::pb::TableName &tn,
                                                    const std::string &row);

  /**
   * Add non-meta region location in the cache (location of meta itself
   * is cached separately).
   */
  void CacheLocation(const hbase::pb::TableName &tn, const std::shared_ptr<RegionLocation> loc);

  /**
   * Check if location corresponding to this row key is cached.
   */
  bool IsLocationCached(const hbase::pb::TableName &tn, const std::string &row);

  /**
   * Return cached location for all region of this table.
   */
  std::shared_ptr<PerTableLocationMap> GetTableLocations(const hbase::pb::TableName &tn);

  /**
   * Completely clear location cache.
   */
  void ClearCache();

  /**
   * Clear all cached locations for one table.
   */
  void ClearCachedLocations(const hbase::pb::TableName &tn);

  /**
   * Clear cached region location.
   */
  void ClearCachedLocation(const hbase::pb::TableName &tn, const std::string &row);

  /**
   * Update cached region location, possibly using the information from exception.
   */
  virtual void UpdateCachedLocation(const RegionLocation &loc,
                                    const folly::exception_wrapper &error) override;

  const std::string &zk_quorum() { return zk_quorum_; }

 private:
  void CloseZooKeeperConnection();
  void EnsureZooKeeperConnection();

 private:
  void RefreshMetaLocation();
  hbase::pb::ServerName ReadMetaLocation();
  std::shared_ptr<RegionLocation> CreateLocation(const Response &resp);
  std::shared_ptr<hbase::PerTableLocationMap> GetCachedTableLocations(
      const hbase::pb::TableName &tn);
  std::shared_ptr<hbase::PerTableLocationMap> GetNewTableLocations(const hbase::pb::TableName &tn);

  /* data */
  std::shared_ptr<hbase::Configuration> conf_;
  std::string zk_quorum_;
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor_;
  std::shared_ptr<folly::SharedPromise<hbase::pb::ServerName>> meta_promise_;
  std::recursive_mutex meta_lock_;
  MetaUtil meta_util_;
  std::shared_ptr<ConnectionPool> cp_;

  // cached region locations
  RegionLocationMap cached_locations_;
  folly::SharedMutexWritePriority locations_lock_;

  // TODO: migrate this to a smart pointer with a deleter.
  zhandle_t *zk_;
};
}  // namespace hbase
