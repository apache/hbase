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
#include "core/location-cache.h"

#include <folly/Conv.h>
#include <folly/Logging.h>
#include <folly/io/IOBuf.h>
#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>

#include "connection/response.h"
#include "connection/rpc-connection.h"
#include "exceptions/exception.h"
#include "if/Client.pb.h"
#include "if/ZooKeeper.pb.h"
#include "serde/region-info.h"
#include "serde/server-name.h"
#include "serde/zk.h"

#include <utility>

using hbase::pb::MetaRegionServer;
using hbase::pb::ServerName;
using hbase::pb::TableName;

namespace hbase {

LocationCache::LocationCache(std::shared_ptr<hbase::Configuration> conf,
                             std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor,
                             std::shared_ptr<ConnectionPool> cp)
    : conf_(conf),
      cpu_executor_(cpu_executor),
      meta_promise_(nullptr),
      meta_lock_(),
      cp_(cp),
      meta_util_(),
      zk_(nullptr),
      cached_locations_(),
      locations_lock_() {
  zk_quorum_ = ZKUtil::ParseZooKeeperQuorum(*conf_);
  zk_ = zookeeper_init(zk_quorum_.c_str(), nullptr, 1000, 0, 0, 0);
}

LocationCache::~LocationCache() {
  zookeeper_close(zk_);
  zk_ = nullptr;
  LOG(INFO) << "Closed connection to ZooKeeper.";
}

folly::Future<ServerName> LocationCache::LocateMeta() {
  std::lock_guard<std::mutex> g(meta_lock_);
  if (meta_promise_ == nullptr) {
    this->RefreshMetaLocation();
  }
  return meta_promise_->getFuture();
}

void LocationCache::InvalidateMeta() {
  if (meta_promise_ != nullptr) {
    std::lock_guard<std::mutex> g(meta_lock_);
    meta_promise_ = nullptr;
  }
}

/// MUST hold the meta_lock_
void LocationCache::RefreshMetaLocation() {
  meta_promise_ = std::make_unique<folly::SharedPromise<ServerName>>();
  cpu_executor_->add([&] { meta_promise_->setWith([&] { return this->ReadMetaLocation(); }); });
}

ServerName LocationCache::ReadMetaLocation() {
  auto buf = folly::IOBuf::create(4096);
  ZkDeserializer derser;

  // This needs to be int rather than size_t as that's what ZK expects.
  int len = buf->capacity();
  std::string zk_node = ZKUtil::MetaZNode(*conf_);
  // TODO(elliott): handle disconnects/reconntion as needed.
  int zk_result = zoo_get(this->zk_, zk_node.c_str(), 0,
                          reinterpret_cast<char *>(buf->writableData()), &len, nullptr);
  if (zk_result != ZOK || len < 9) {
    LOG(ERROR) << "Error getting meta location.";
    throw std::runtime_error("Error getting meta location. Quorum: " + zk_quorum_);
  }
  buf->append(len);

  MetaRegionServer mrs;
  if (derser.Parse(buf.get(), &mrs) == false) {
    LOG(ERROR) << "Unable to decode";
  }
  return mrs.server();
}

folly::Future<std::shared_ptr<RegionLocation>> LocationCache::LocateFromMeta(
    const TableName &tn, const std::string &row) {
  return this->LocateMeta()
      .via(cpu_executor_.get())
      .then([this](ServerName sn) {
        auto remote_id = std::make_shared<ConnectionId>(sn.host_name(), sn.port());
        return this->cp_->GetConnection(remote_id);
      })
      .then([tn, row, this](std::shared_ptr<RpcConnection> rpc_connection) {
        return (*rpc_connection->get_service())(std::move(meta_util_.MetaRequest(tn, row)));
      })
      .then([this](std::unique_ptr<Response> resp) {
        // take the protobuf response and make it into
        // a region location.
        return meta_util_.CreateLocation(std::move(*resp));
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
        // Now fill out the connection.
        // rl->set_service(cp_->GetConnection(remote_id)->get_service()); TODO: this causes wangle
        // assertion errors
        return rl;
      })
      .then([tn, this](std::shared_ptr<RegionLocation> rl) {
        // now add fetched location to the cache.
        this->CacheLocation(tn, rl);
        return rl;
      });
}

folly::Future<std::shared_ptr<RegionLocation>> LocationCache::LocateRegion(
    const TableName &tn, const std::string &row, const RegionLocateType locate_type,
    const int64_t locate_ns) {
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

  if (VLOG_IS_ON(2)) {
    for (const auto &p : *t_locs) {
      VLOG(2) << "t_locs[" << p.first << "] = " << p.second->DebugString();
    }
  }

  // looking for the "floor" key as a start key
  auto possible_region = t_locs->upper_bound(row);

  if (t_locs->empty()) {
    VLOG(2) << "Could not find region in cache, table map is empty";
    return nullptr;
  }

  if (possible_region == t_locs->begin()) {
    VLOG(2) << "Could not find region in cache, all keys are greater, row:" << row
            << " ,possible_region:" << possible_region->second->DebugString();
    return nullptr;
  }
  --possible_region;

  VLOG(2) << "Found possible region in cache for row:" << row
          << " ,possible_region:" << possible_region->second->DebugString();

  // found possible start key, now need to check end key
  if (possible_region->second->region_info().end_key() == "" ||
      possible_region->second->region_info().end_key() > row) {
    VLOG(1) << "Found region in cache for row:" << row
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
  std::unique_lock<folly::SharedMutexWritePriority> lock(locations_lock_);
  cached_locations_.erase(tn);
}

// must hold unique lock on locations_lock_
void LocationCache::ClearCachedLocation(const hbase::pb::TableName &tn, const std::string &row) {
  auto table_locs = this->GetTableLocations(tn);
  std::unique_lock<folly::SharedMutexWritePriority> lock(locations_lock_);
  table_locs->erase(row);
}

void LocationCache::UpdateCachedLocation(const RegionLocation &loc,
                                         const folly::exception_wrapper &error) {
  // TODO: just clears the location for now. We can inspect RegionMovedExceptions, etc later.
  ClearCachedLocation(loc.region_info().table_name(), loc.region_info().start_key());
}
}  // namespace hbase
