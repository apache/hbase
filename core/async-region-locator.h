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

#include <folly/futures/Future.h>
#include <memory>
#include <string>

#include "core/region-location.h"
#include "if/Client.pb.h"
#include "serde/region-info.h"
#include "serde/server-name.h"
#include "serde/table-name.h"

namespace hbase {

class AsyncRegionLocator {
 public:
  AsyncRegionLocator() {}
  virtual ~AsyncRegionLocator() = default;

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
      const int64_t locate_ns = 0) = 0;
  /**
   * Update cached region location, possibly using the information from exception.
   */
  virtual void UpdateCachedLocation(const RegionLocation &loc, const std::exception &error) = 0;
};

}  // namespace hbase
