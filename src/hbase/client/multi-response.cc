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

#include "hbase/client/multi-response.h"
#include <glog/logging.h>
#include "hbase/client/region-result.h"

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
                                    std::shared_ptr<folly::exception_wrapper> exc) {
  auto itr = results_.find(region_name);
  if (itr == results_.end()) {
    auto region_result = std::make_shared<RegionResult>();
    region_result->AddResultOrException(original_index, result, exc);
    results_[region_name] = region_result;
  } else {
    itr->second->AddResultOrException(original_index, result, exc);
  }
}

void MultiResponse::AddRegionException(const std::string& region_name,
                                       std::shared_ptr<folly::exception_wrapper> exception) {
  VLOG(8) << "Store Region Exception:- " << exception->what() << "; Region[" << region_name << "];";
  bool region_found = false;
  auto itr = exceptions_.find(region_name);
  if (itr == exceptions_.end()) {
    auto region_result = std::make_shared<folly::exception_wrapper>();
    exceptions_[region_name] = exception;
  } else {
    itr->second = exception;
  }
}

std::shared_ptr<folly::exception_wrapper> MultiResponse::RegionException(
    const std::string& region_name) const {
  auto find = exceptions_.at(region_name);
  return find;
}

const std::map<std::string, std::shared_ptr<folly::exception_wrapper> >&
MultiResponse::RegionExceptions() const {
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
