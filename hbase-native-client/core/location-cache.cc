#include "location-cache.h"

#include <folly/Logging.h>

#include "if/ZooKeeper.pb.h"

using namespace std;
using namespace folly;
using namespace hbase::pb;

namespace hbase {

// TODO(elliott): make this configurable on client creation
const static string META_LOCATION = "/hbase/meta-region-server";

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
  char contents[4096];
  int len = sizeof(contents);
  // TODO(elliott): handle disconnects/reconntion as needed.
  int zk_result =
      zoo_get(this->zk_, META_LOCATION.c_str(), 0, contents, &len, nullptr);

  if (zk_result != ZOK) {
    LOG(ERROR) << "Error getting meta location.";
    throw runtime_error("Error getting meta location");
  }

  MetaRegionServer mrs;
  mrs.ParseFromArray(contents, len);
  return mrs.server();
}
}
