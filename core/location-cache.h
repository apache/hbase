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

#include <zookeeper/zookeeper.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <folly/Executor.h>

#include <memory>
#include <mutex>
#include <string>

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
}  // namespace hbase
