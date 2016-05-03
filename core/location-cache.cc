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

#include <folly/Logging.h>
#include <folly/io/IOBuf.h>
#include <wangle/concurrent/GlobalExecutor.h>

#include "connection/response.h"
#include "if/Client.pb.h"
#include "if/ZooKeeper.pb.h"
#include "serde/server-name.h"
#include "serde/region-info.h"
#include "serde/zk.h"

using namespace std;
using namespace folly;

using wangle::ServiceFilter;
using hbase::Request;
using hbase::Response;
using hbase::LocationCache;
using hbase::RegionLocation;
using hbase::HBaseService;
using hbase::ConnectionPool;
using hbase::pb::ScanResponse;
using hbase::pb::TableName;
using hbase::pb::ServerName;
using hbase::pb::MetaRegionServer;
using hbase::pb::RegionInfo;

// TODO(eclark): make this configurable on client creation
static const char META_ZNODE_NAME[] = "/hbase/meta-region-server";

LocationCache::LocationCache(string quorum_spec,
                             shared_ptr<folly::Executor> executor)
    : quorum_spec_(quorum_spec), executor_(executor), meta_promise_(nullptr),
      meta_lock_(), cp_(), meta_util_(), zk_(nullptr) {
  zk_ = zookeeper_init(quorum_spec.c_str(), nullptr, 1000, 0, 0, 0);
}

LocationCache::~LocationCache() {
  zookeeper_close(zk_);
  zk_ = nullptr;
  LOG(INFO) << "Closed connection to ZooKeeper.";
}

Future<ServerName> LocationCache::LocateMeta() {
  lock_guard<mutex> g(meta_lock_);
  if (meta_promise_ == nullptr) {
    this->RefreshMetaLocation();
  }
  return meta_promise_->getFuture();
}

void LocationCache::InvalidateMeta() {
  if (meta_promise_ != nullptr) {
    lock_guard<mutex> g(meta_lock_);
    meta_promise_ = nullptr;
  }
}

/// MUST hold the meta_lock_
void LocationCache::RefreshMetaLocation() {
  meta_promise_ = make_unique<SharedPromise<ServerName>>();
  executor_->add([&] {
    meta_promise_->setWith([&] { return this->ReadMetaLocation(); });
  });
}

ServerName LocationCache::ReadMetaLocation() {
  auto buf = IOBuf::create(4096);
  ZkDeserializer derser;

  // This needs to be int rather than size_t as that's what ZK expects.
  int len = buf->capacity();
  // TODO(elliott): handle disconnects/reconntion as needed.
  int zk_result =
      zoo_get(this->zk_, META_ZNODE_NAME, 0,
              reinterpret_cast<char *>(buf->writableData()), &len, nullptr);
  if (zk_result != ZOK || len < 9) {
    LOG(ERROR) << "Error getting meta location.";
    throw runtime_error("Error getting meta location");
  }
  buf->append(len);

  MetaRegionServer mrs;
  if (derser.Parse(buf.get(), &mrs) == false) {
    LOG(ERROR) << "Unable to decode";
  }
  return mrs.server();
}

Future<std::shared_ptr<RegionLocation>>
LocationCache::LocateFromMeta(const TableName &tn, const string &row) {
  auto exc = wangle::getIOExecutor();
  return this->LocateMeta()
      .then([&](ServerName sn) { return this->cp_.get(sn); })
      .via(exc.get()) // Need to handle all rpc's on the IOExecutor.
      .then([&](std::shared_ptr<HBaseService> service) {
        return (*service)(std::move(meta_util_.MetaRequest(tn, row)));
      })
      .then([&](Response resp) {
        // take the protobuf response and make it into
        // a region location.
        return this->CreateLocation(std::move(resp));
      });
}

class RemoveServiceFilter
    : public ServiceFilter<std::unique_ptr<Request>, Response> {

public:
  RemoveServiceFilter(std::shared_ptr<HBaseService> service, ServerName sn,
                      ConnectionPool &cp)
      : ServiceFilter<unique_ptr<Request>, Response>(service), sn_(sn),
        cp_(cp) {}

  folly::Future<folly::Unit> close() override {
    if (!released.exchange(true)) {
      return this->service_->close().then([this]() {
        // TODO(eclark): remove the service from the meta cache.
        this->cp_.close(this->sn_);
      });
    } else {
      return folly::makeFuture();
    }
  }

  virtual bool isAvailable() override {
    return !released && service_->isAvailable();
  }

  folly::Future<Response> operator()(unique_ptr<Request> req) override {
    // TODO(eclark): add in an on error handler that will
    // remove the region location from the cache if needed.
    // Also close the connection if this is likely to be an error
    // that needs to get a new connection.
    return (*this->service_)(std::move(req));
  }

private:
  std::atomic<bool> released{false};
  hbase::pb::ServerName sn_;
  ConnectionPool &cp_;
};

std::shared_ptr<RegionLocation>
LocationCache::CreateLocation(const Response &resp){
  auto resp_msg = static_pointer_cast<ScanResponse>(resp.response());
  auto &results = resp_msg->results().Get(0);
  auto &cells = results.cell();
  LOG(ERROR) << "resp_msg = " << resp_msg->DebugString();
  auto ri = folly::to<RegionInfo>(cells.Get(0).value());
  auto sn = folly::to<ServerName>(cells.Get(1).value());

  LOG(ERROR) << "RegionInfo = " << ri.DebugString();
  LOG(ERROR) << "ServerName = " << sn.DebugString();
  auto wrapped = make_shared<RemoveServiceFilter>(cp_.get(sn), sn, this->cp_);
  return std::make_shared<RegionLocation>(std::move(ri), std::move(sn), wrapped);
}
