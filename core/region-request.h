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

#include <memory>
#include <queue>
#include <vector>
#include "core/action.h"
#include "core/region-location.h"

namespace hbase {

class RegionRequest {
 public:
  // Concurrent
  using ActionList = std::vector<std::shared_ptr<Action>>;
  explicit RegionRequest(const std::shared_ptr<hbase::RegionLocation> &region_loc)
      : region_loc_(region_loc) {}
  ~RegionRequest() {}
  void AddAction(std::shared_ptr<Action> action) { actions_.push_back(action); }
  std::shared_ptr<hbase::RegionLocation> region_location() const { return region_loc_; }
  const ActionList &actions() const { return actions_; }

 private:
  std::shared_ptr<hbase::RegionLocation> region_loc_;
  ActionList actions_;
};

} /* namespace hbase */
