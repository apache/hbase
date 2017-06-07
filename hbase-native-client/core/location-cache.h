#pragma once

#include <memory>
#include <mutex>

#include <zookeeper/zookeeper.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include <folly/Executor.h>
#include "if/HBase.pb.h"

namespace hbase {
class LocationCache {
public:
  explicit LocationCache(std::string quorum_spec,
                         std::shared_ptr<folly::Executor> executor);
  ~LocationCache();
  // Meta Related Methods.
  // These are only public until testing is complete
  folly::Future<hbase::pb::ServerName> LocateMeta();
  void InvalidateMeta();

private:
  void RefreshMetaLocation();
  hbase::pb::ServerName ReadMetaLocation();

  std::string quorum_spec_;
  std::shared_ptr<folly::Executor> executor_;
  std::unique_ptr<folly::SharedPromise<hbase::pb::ServerName>> meta_promise_;
  std::mutex meta_lock_;

  zhandle_t *zk_;
};
} // hbase
