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

#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include "core/action.h"
#include "core/region-location.h"
#include "core/region-request.h"

namespace hbase {

class ServerRequest {
 public:
  // Concurrent
  using ActionsByRegion = std::map<std::string, std::shared_ptr<RegionRequest>>;

  explicit ServerRequest(std::shared_ptr<RegionLocation> region_location) {
    auto region_name = region_location->region_name();
    auto region_request = std::make_shared<RegionRequest>(region_location);
    actions_by_region_[region_name] = region_request;
  }
  ~ServerRequest() {}

  void AddActionsByRegion(std::shared_ptr<RegionLocation> region_location,
                          std::shared_ptr<Action> action) {
    auto region_name = region_location->region_name();
    auto itr = actions_by_region_.at(region_name);
    itr->AddAction(action);
  }

  const ActionsByRegion &actions_by_region() const { return actions_by_region_; }

 private:
  ActionsByRegion actions_by_region_;
};
} /* namespace hbase */
