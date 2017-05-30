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

#include "core/multi-response.h"
#include "core/region-result.h"

using hbase::pb::RegionLoadStats;

namespace hbase {

MultiResponse::MultiResponse() {}

int MultiResponse::Size() const {
  int size = 0;
  for (const auto& result : results_) {
    size += result.second->ResultOrExceptionSize();
  }
  return size;
}

void MultiResponse::AddRegionResult(const std::string& region_name, int32_t original_index,
                                    std::shared_ptr<Result> result,
                                    std::shared_ptr<std::exception> exc) {
  bool region_found = false;
  for (auto itr = results_.begin(); itr != results_.end(); ++itr) {
    if (itr->first == region_name) {
      region_found = true;
      itr->second->AddResultOrException(original_index, result, exc);
      break;
    }
  }
  if (!region_found) {
    auto region_result = std::make_shared<RegionResult>();
    region_result->AddResultOrException(original_index, result, exc);
    results_[region_name] = region_result;
  }
}

void MultiResponse::AddRegionException(const std::string& region_name,
                                       std::shared_ptr<std::exception> exception) {
  exceptions_[region_name] = exception;
}

std::shared_ptr<std::exception> MultiResponse::RegionException(
    const std::string& region_name) const {
  auto find = exceptions_.at(region_name);
  return find;
}

const std::map<std::string, std::shared_ptr<std::exception> >& MultiResponse::RegionExceptions()
    const {
  return exceptions_;
}

void MultiResponse::AddStatistic(const std::string& region_name,
                                 std::shared_ptr<RegionLoadStats> stat) {
  results_[region_name]->set_stat(stat);
}

const std::map<std::string, std::shared_ptr<RegionResult> >& MultiResponse::RegionResults() const {
  return results_;
}

MultiResponse::~MultiResponse() {}

} /* namespace hbase */
