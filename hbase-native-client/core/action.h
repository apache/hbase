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
#include "core/row.h"

namespace hbase {
class Action {
 public:
  Action(std::shared_ptr<hbase::Row> action, int32_t original_index)
      : action_(action), original_index_(original_index) {}
  ~Action() {}

  int32_t original_index() const { return original_index_; }

  std::shared_ptr<hbase::Row> action() const { return action_; }

 private:
  std::shared_ptr<hbase::Row> action_;
  int32_t original_index_;
  int64_t nonce_ = -1;
  int32_t replica_id_ = -1;
};

} /* namespace hbase */
