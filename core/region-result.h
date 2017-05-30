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
#include <string>
#include <tuple>
#include "core/result.h"
#include "if/Client.pb.h"

namespace hbase {

using ResultOrExceptionTuple =
    std::tuple<std::shared_ptr<hbase::Result>, std::shared_ptr<std::exception>>;

class RegionResult {
 public:
  RegionResult();
  void AddResultOrException(int32_t index, std::shared_ptr<hbase::Result> result,
                            std::shared_ptr<std::exception> exc);

  void set_stat(std::shared_ptr<pb::RegionLoadStats> stat);

  int ResultOrExceptionSize() const;

  std::shared_ptr<ResultOrExceptionTuple> ResultOrException(int32_t index) const;

  const std::shared_ptr<pb::RegionLoadStats>& stat() const;

  ~RegionResult();

 private:
  std::map<int, ResultOrExceptionTuple> result_or_excption_;
  std::shared_ptr<pb::RegionLoadStats> stat_;
};
} /* namespace hbase */
