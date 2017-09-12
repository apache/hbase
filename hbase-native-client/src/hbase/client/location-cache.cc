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
#include "hbase/client/location-cache.h"

#include <folly/Conv.h>
#include <folly/Logging.h>
#include <folly/io/IOBuf.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>

#include <map>
#include <shared_mutex>
#include <utility>

#include "hbase/connection/response.h"
#include "hbase/connection/rpc-connection.h"
#include "hbase/client/meta-utils.h"
#include "hbase/exceptions/exception.h"
#include "hbase/if/Client.pb.h"
#include "hbase/if/ZooKeeper.pb.h"
#include "hbase/serde/region-info.h"
#include "hbase/serde/server-name.h"
#include "hbase/serde/zk.h"

using hbase::pb::MetaRegionServer;
using hbase::pb::ServerName;
using hbase::pb::TableName;

namespace hbase {

LocationCache::LocationCache(std::shared_ptr<hbase::Configuration> conf,
                             std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor,
                             std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor,
                             std::shared_ptr<ConnectionPool> cp)
    : conf_(conf),
      io_executor_(io_executor),
      cpu_executor_(cpu_executor),
      cp_(cp),
      meta_promise_(nullptr),
      meta_lock_(),
      meta_util_(),
      zk_(nullptr),
      cached_locations_(),
      locations_lock_() {
  zk_quorum_ = ZKUtil::ParseZooKeeperQuorum(*conf_);
  EnsureZooKeeperConnection();
}

LocationCache::~LocationCache() { CloseZooKeeperConnection(); }

void LocationCache::CloseZooKeeperConnection() {
  if (zk_ != nullptr) {
    zookeeper_close(zk_);
    zk_ = nullptr;
    LOG(INFO) << "Closed connection to ZooKeeper.";
  }
}

void LocationCache::EnsureZooKeeperConnection() {
  if (zk_ == nullptr) {
    LOG(INFO) << "Connecting to ZooKeeper. Quorum:" + zk_quorum_;
    auto session_timeout = ZKUtil::SessionTimeout(*conf_);
    zk_ = zookeeper_init(zk_quorum_.c_str(), nullptr, session_timeout, nullptr, nullptr, 0);
  }
}

folly::Future<ServerName> LocationCache::LocateMeta() {
  std::lock_guard<std::recursive_mutex> g(meta_lock_);
  if (meta_promise_ == nullptr) {
    this->RefreshMetaLocation();
  }
  return meta_promise_->getFuture().onError([&](const folly::exception_wrapper &ew) {
    auto promise = InvalidateMeta();
    if (promise) {
      promise->setException(ew);
    }
    throw ew;
    return ServerName{};
  });
}

std::shared_ptr<folly::SharedPromise<hbase::pb::ServerName>> LocationCache::InvalidateMeta() {
  VLOG(2) << "Invalidating meta location";
  std::lock_guard<std::recursive_mutex> g(meta_lock_);
  if (meta_promise_ != nullptr) {
    // return the unique_ptr back to the caller.
    std::shared_ptr<folly::SharedPromise<hbase::pb::ServerName>> ret = nullptr;
    std::swap(ret, meta_promise_);
    return ret;
  } else {
    return nullptr;
  }
}

void LocationCache::RefreshMetaLocation() {
  meta_promise_ = std::make_shared<folly::SharedPromise<ServerName>>();
  auto p = meta_promise_;
  cpu_executor_->add([this, p] {
    std::lock_guard<std::recursive_mutex> g(meta_lock_);
    p->setWith([&] { return this->ReadMetaLocation(); });
  });
}

// Note: this is a blocking call to zookeeper
ServerName LocationCache::ReadMetaLocation() {
  auto buf = folly::IOBuf::create(4096);
  ZkDeserializer derser;
  EnsureZooKeeperConnection();

  // This needs to be int rather than size_t as that's what ZK expects.
  int len = buf->capacity();
  std::string zk_node = ZKUtil::MetaZNode(*conf_);
  int zk_result = zoo_get(this->zk_, zk_node.c_str(), 0,
                          reinterpret_cast<char *>(buf->writableData()), &len, nullptr);
  if (zk_result != ZOK || len < 9) {
    LOG(ERROR) << "Error getting meta location.";
    // We just close the zk connection, and let the upper levels retry.
    CloseZooKeeperConnection();
    throw std::runtime_error("Error getting meta location. Quorum: " + zk_quorum_);
  }
  buf->append(len);

  MetaRegionServer mrs;
  if (derser.Parse(buf.get(), &mrs) == false) {
    LOG(ERROR) << "Unable to decode";
    throw std::runtime_error("Error getting meta location (Unable to decode). Quorum: " +
                             zk_quorum_);
  }
  return mrs.server();
}

folly::Future<std::shared_ptr<RegionLocation>> LocationCache::LocateFromMeta(
    const TableName &tn, const std::string &row) {
  return this->LocateMeta()
      .via(cpu_executor_.get())
      .then([this](ServerName sn) {
        // TODO: use RpcClient?
        auto remote_id = std::make_shared<ConnectionId>(sn.host_name(), sn.port());
        return this->cp_->GetConnection(remote_id);
      })
      .then([tn, row, this](std::shared_ptr<RpcConnection> rpc_connection) {
        return rpc_connection->SendRequest(std::move(meta_util_.MetaRequest(tn, row)));
      })
      .onError([&](const folly::exception_wrapper &ew) {
        auto promise = InvalidateMeta();
        throw ew;
        return static_cast<std::unique_ptr<Response>>(nullptr);
      })
      .then([tn, this](std::unique_ptr<Response> resp) {
        // take the protobuf response and make it into
        // a region location.
        return meta_util_.CreateLocation(std::move(*resp), tn);
      })
      .then([tn, this](std::shared_ptr<RegionLocation> rl) {
        // Make sure that the correct location was found.
        if (rl->region_info().table_name().namespace_() != tn.namespace_() ||
            rl->region_info().table_name().qualifier() != tn.qualifier()) {
          throw TableNotFoundException(folly::to<std::string>(tn));
        }
        return rl;
      })
      .then([this](std::shared_ptr<RegionLocation> rl) {
        auto remote_id =
            std::make_shared<ConnectionId>(rl->server_name().host_name(), rl->server_name().port());
        return rl;
      })
      .then([tn, this](std::shared_ptr<RegionLocation> rl) {
        // now add fetched location to the cache.
        this->CacheLocation(tn, rl);
        return rl;
      });
}

constexpr const char *MetaUtil::kMetaRegionName;

folly::Future<std::shared_ptr<RegionLocation>> LocationCache::LocateRegion(
    const TableName &tn, const std::string &row, const RegionLocateType locate_type,
    const int64_t locate_ns) {
  // We maybe asked to locate meta itself
  if (MetaUtil::IsMeta(tn)) {
    return LocateMeta().then([this](const ServerName &server_name) {
      auto rl = std::make_shared<RegionLocation>(MetaUtil::kMetaRegionName,
                                                 meta_util_.meta_region_info(), server_name);
      return rl;
    });
  }

  // TODO: implement region locate type and timeout
  auto cached_loc = this->GetCachedLocation(tn, row);
  if (cached_loc != nullptr) {
    return cached_loc;
  } else {
    return this->LocateFromMeta(tn, row);
  }
}

// must hold shared lock on locations_lock_
std::shared_ptr<RegionLocation> LocationCache::GetCachedLocation(const hbase::pb::TableName &tn,
                                                                 const std::string &row) {
  auto t_locs = this->GetTableLocations(tn);
  std::shared_lock<folly::SharedMutexWritePriority> lock(locations_lock_);

  // looking for the "floor" key as a start key
  auto possible_region = t_locs->upper_bound(row);

  if (t_locs->empty()) {
    VLOG(5) << "Could not find region in cache, table map is empty";
    return nullptr;
  }

  if (possible_region == t_locs->begin()) {
    VLOG(5) << "Could not find region in cache, all keys are greater, row:" << row
            << " ,possible_region:" << possible_region->second->DebugString();
    return nullptr;
  }
  --possible_region;

  VLOG(5) << "Found possible region in cache for row:" << row
          << " ,possible_region:" << possible_region->second->DebugString();

  // found possible start key, now need to check end key
  if (possible_region->second->region_info().end_key() == "" ||
      possible_region->second->region_info().end_key() > row) {
    VLOG(2) << "Found region in cache for row:" << row
            << " ,region:" << possible_region->second->DebugString();
    return possible_region->second;
  } else {
    return nullptr;
  }
}

// must hold unique lock on locations_lock_
void LocationCache::CacheLocation(const hbase::pb::TableName &tn,
                                  const std::shared_ptr<RegionLocation> loc) {
  auto t_locs = this->GetTableLocations(tn);
  std::unique_lock<folly::SharedMutexWritePriority> lock(locations_lock_);

  (*t_locs)[loc->region_info().start_key()] = loc;
  VLOG(1) << "Cached location for region:" << loc->DebugString();
}

// must hold shared lock on locations_lock_
bool LocationCache::IsLocationCached(const hbase::pb::TableName &tn, const std::string &row) {
  return (this->GetCachedLocation(tn, row) != nullptr);
}

// shared lock needed for cases when this table has been requested before;
// in the rare case it hasn't, unique lock will be grabbed to add it to cache
std::shared_ptr<hbase::PerTableLocationMap> LocationCache::GetTableLocations(
    const hbase::pb::TableName &tn) {
  auto found_locs = this->GetCachedTableLocations(tn);
  if (found_locs == nullptr) {
    found_locs = this->GetNewTableLocations(tn);
  }
  return found_locs;
}

std::shared_ptr<hbase::PerTableLocationMap> LocationCache::GetCachedTableLocations(
    const hbase::pb::TableName &tn) {
  folly::SharedMutexWritePriority::ReadHolder r_holder{locations_lock_};

  auto table_locs = cached_locations_.find(tn);
  if (table_locs != cached_locations_.end()) {
    return table_locs->second;
  } else {
    return nullptr;
  }
}

std::shared_ptr<hbase::PerTableLocationMap> LocationCache::GetNewTableLocations(
    const hbase::pb::TableName &tn) {
  // double-check locking under upgradable lock
  folly::SharedMutexWritePriority::UpgradeHolder u_holder{locations_lock_};

  auto table_locs = cached_locations_.find(tn);
  if (table_locs != cached_locations_.end()) {
    return table_locs->second;
  }
  folly::SharedMutexWritePriority::WriteHolder w_holder{std::move(u_holder)};

  auto t_locs_p = std::make_shared<std::map<std::string, std::shared_ptr<RegionLocation>>>();
  cached_locations_.insert(std::make_pair(tn, t_locs_p));
  return t_locs_p;
}

// must hold unique lock on locations_lock_
void LocationCache::ClearCache() {
  std::unique_lock<folly::SharedMutexWritePriority> lock(locations_lock_);
  cached_locations_.clear();
}

// must hold unique lock on locations_lock_
void LocationCache::ClearCachedLocations(const hbase::pb::TableName &tn) {
  VLOG(1) << "ClearCachedLocations, table:" << folly::to<std::string>(tn);
  std::unique_lock<folly::SharedMutexWritePriority> lock(locations_lock_);
  cached_locations_.erase(tn);
  if (MetaUtil::IsMeta(tn)) {
    InvalidateMeta();
  }
}

// must hold unique lock on locations_lock_
void LocationCache::ClearCachedLocation(const hbase::pb::TableName &tn, const std::string &row) {
  VLOG(1) << "ClearCachedLocation, table:" << folly::to<std::string>(tn) << ", row:" << row;
  auto table_locs = this->GetTableLocations(tn);
  std::unique_lock<folly::SharedMutexWritePriority> lock(locations_lock_);
  table_locs->erase(row);
  if (MetaUtil::IsMeta(tn)) {
    InvalidateMeta();
  }
}

void LocationCache::UpdateCachedLocation(const RegionLocation &loc,
                                         const folly::exception_wrapper &error) {
  // TODO: just clears the location for now. We can inspect RegionMovedExceptions, etc later.
  ClearCachedLocation(loc.region_info().table_name(), loc.region_info().start_key());
}
}  // namespace hbase
