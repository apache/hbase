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

#include <folly/Logging.h>
#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "hbase/client/result.h"
#include "hbase/if/Client.pb.h"
#include "hbase/if/HBase.pb.h"

namespace hbase {

class ScanResultCache {
  // In Java, there are 3 different implementations for this. We are not doing partial results,
  // or scan batching in native code for now, so this version is simpler and
  // only deals with giving back complete rows as Result. It is more or less implementation
  // of CompleteScanResultCache.java

 public:
  /**
   * Add the given results to cache and get valid results back.
   * @param results the results of a scan next. Must not be null.
   * @param is_hearthbeat indicate whether the results is gotten from a heartbeat response.
   * @return valid results, never null.
   */
  std::vector<std::shared_ptr<Result>> AddAndGet(
      const std::vector<std::shared_ptr<Result>> &results, bool is_hearthbeat);

  void Clear();

  int64_t num_complete_rows() const { return num_complete_rows_; }

 private:
  /**
     * Forms a single result from the partial results in the partialResults list. This method is
     * useful for reconstructing partial results on the client side.
     * @param partial_results list of partial results
     * @return The complete result that is formed by combining all of the partial results together
     */
  static std::shared_ptr<Result> CreateCompleteResult(
      const std::vector<std::shared_ptr<Result>> &partial_results);

  std::shared_ptr<Result> Combine();

  std::vector<std::shared_ptr<Result>> PrependCombined(
      const std::vector<std::shared_ptr<Result>> &results, int length);

  std::vector<std::shared_ptr<Result>> UpdateNumberOfCompleteResultsAndReturn(
      const std::shared_ptr<Result> &result);

  std::vector<std::shared_ptr<Result>> UpdateNumberOfCompleteResultsAndReturn(
      const std::vector<std::shared_ptr<Result>> &results);

 private:
  std::vector<std::shared_ptr<Result>> partial_results_;
  int64_t num_complete_rows_ = 0;
};
}  // namespace hbase
