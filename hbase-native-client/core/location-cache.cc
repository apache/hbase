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

#include "if/ZooKeeper.pb.h"
#include "serde/zk-deserializer.h"

using namespace std;
using namespace folly;
using namespace hbase::pb;
using namespace hbase;

// TODO(eclark): make this configurable on client creation
static const char META_ZNODE_NAME[] = "/hbase/meta-region-server";

LocationCache::LocationCache(string quorum_spec,
                             shared_ptr<folly::Executor> executor)
    : quorum_spec_(quorum_spec), executor_(executor), meta_promise_(nullptr) {
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
  if (derser.parse(buf.get(), &mrs) == false) {
    LOG(ERROR) << "Unable to decode";
  }
  return mrs.server();
}
