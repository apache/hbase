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

#include <core/region-result.h>
#include <folly/ExceptionWrapper.h>
#include <exception>
#include <map>
#include <memory>
#include <string>

#include "core/result.h"
#include "if/Client.pb.h"

namespace hbase {

class MultiResponse {
 public:
  MultiResponse();
  /**
   * @brief Returns Number of pairs in this container
   */
  int Size() const;

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName
   * @param originalIndex the original index of the Action (request).
   * @param resOrEx the result or error; will be empty for successful Put and Delete actions.
   */
  void AddRegionResult(const std::string& region_name, int32_t original_index,
                       std::shared_ptr<Result> result,
                       std::shared_ptr<folly::exception_wrapper> exc);

  void AddRegionException(const std::string& region_name,
                          std::shared_ptr<folly::exception_wrapper> exception);

  /**
   * @return the exception for the region, if any. Null otherwise.
   */
  std::shared_ptr<folly::exception_wrapper> RegionException(const std::string& region_name) const;

  const std::map<std::string, std::shared_ptr<folly::exception_wrapper>>& RegionExceptions() const;

  void AddStatistic(const std::string& region_name, std::shared_ptr<pb::RegionLoadStats> stat);

  const std::map<std::string, std::shared_ptr<RegionResult>>& RegionResults() const;

  ~MultiResponse();

 private:
  // map of regionName to map of Results by the original index for that Result
  std::map<std::string, std::shared_ptr<RegionResult>> results_;
  /**
   * The server can send us a failure for the region itself, instead of individual failure.
   * It's a part of the protobuf definition.
   */
  std::map<std::string, std::shared_ptr<folly::exception_wrapper>> exceptions_;
};

} /* namespace hbase */
