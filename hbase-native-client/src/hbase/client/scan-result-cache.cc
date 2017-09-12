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

#include "hbase/client/scan-result-cache.h"
#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>

namespace hbase {
/**
 * Add the given results to cache and get valid results back.
 * @param results the results of a scan next. Must not be null.
 * @param is_hearthbeat indicate whether the results is gotten from a heartbeat response.
 * @return valid results, never null.
 */
std::vector<std::shared_ptr<Result>> ScanResultCache::AddAndGet(
    const std::vector<std::shared_ptr<Result>> &results, bool is_hearthbeat) {
  // If no results were returned it indicates that either we have the all the partial results
  // necessary to construct the complete result or the server had to send a heartbeat message
  // to the client to keep the client-server connection alive
  if (results.empty()) {
    // If this response was an empty heartbeat message, then we have not exhausted the region
    // and thus there may be more partials server side that still need to be added to the partial
    // list before we form the complete Result
    if (!partial_results_.empty() && !is_hearthbeat) {
      return UpdateNumberOfCompleteResultsAndReturn(
          std::vector<std::shared_ptr<Result>>{Combine()});
    }
    return std::vector<std::shared_ptr<Result>>{};
  }
  // In every RPC response there should be at most a single partial result. Furthermore, if
  // there is a partial result, it is guaranteed to be in the last position of the array.
  auto last = results[results.size() - 1];
  if (last->Partial()) {
    if (partial_results_.empty()) {
      partial_results_.push_back(last);
      std::vector<std::shared_ptr<Result>> new_results;
      std::copy_n(results.begin(), results.size() - 1, std::back_inserter(new_results));
      return UpdateNumberOfCompleteResultsAndReturn(new_results);
    }
    // We have only one result and it is partial
    if (results.size() == 1) {
      // check if there is a row change
      if (partial_results_.at(0)->Row() == last->Row()) {
        partial_results_.push_back(last);
        return std::vector<std::shared_ptr<Result>>{};
      }
      auto complete_result = Combine();
      partial_results_.push_back(last);
      return UpdateNumberOfCompleteResultsAndReturn(complete_result);
    }
    // We have some complete results
    auto results_to_return = PrependCombined(results, results.size() - 1);
    partial_results_.push_back(last);
    return UpdateNumberOfCompleteResultsAndReturn(results_to_return);
  }
  if (!partial_results_.empty()) {
    return UpdateNumberOfCompleteResultsAndReturn(PrependCombined(results, results.size()));
  }
  return UpdateNumberOfCompleteResultsAndReturn(results);
}

void ScanResultCache::Clear() { partial_results_.clear(); }

std::shared_ptr<Result> ScanResultCache::CreateCompleteResult(
    const std::vector<std::shared_ptr<Result>> &partial_results) {
  if (partial_results.empty()) {
    return std::make_shared<Result>(std::vector<std::shared_ptr<Cell>>{}, false, false, false);
  }
  std::vector<std::shared_ptr<Cell>> cells{};
  bool stale = false;
  std::string prev_row = "";
  std::string current_row = "";
  size_t i = 0;
  for (const auto &r : partial_results) {
    current_row = r->Row();
    if (i != 0 && prev_row != current_row) {
      throw new std::runtime_error(
          "Cannot form complete result. Rows of partial results do not match.");
    }
    // Ensure that all Results except the last one are marked as partials. The last result
    // may not be marked as a partial because Results are only marked as partials when
    // the scan on the server side must be stopped due to reaching the maxResultSize.
    // Visualizing it makes it easier to understand:
    // maxResultSize: 2 cells
    // (-x-) represents cell number x in a row
    // Example: row1: -1- -2- -3- -4- -5- (5 cells total)
    // How row1 will be returned by the server as partial Results:
    // Result1: -1- -2- (2 cells, size limit reached, mark as partial)
    // Result2: -3- -4- (2 cells, size limit reached, mark as partial)
    // Result3: -5- (1 cell, size limit NOT reached, NOT marked as partial)
    if (i != partial_results.size() - 1 && !r->Partial()) {
      throw new std::runtime_error("Cannot form complete result. Result is missing partial flag.");
    }
    prev_row = current_row;
    stale = stale || r->Stale();
    for (const auto &c : r->Cells()) {
      cells.push_back(c);
    }
    i++;
  }

  return std::make_shared<Result>(cells, false, stale, false);
}

std::shared_ptr<Result> ScanResultCache::Combine() {
  auto result = CreateCompleteResult(partial_results_);
  partial_results_.clear();
  return result;
}

std::vector<std::shared_ptr<Result>> ScanResultCache::PrependCombined(
    const std::vector<std::shared_ptr<Result>> &results, int length) {
  if (length == 0) {
    return std::vector<std::shared_ptr<Result>>{Combine()};
  }
  // the last part of a partial result may not be marked as partial so here we need to check if
  // there is a row change.
  size_t start;
  if (partial_results_[0]->Row() == results[0]->Row()) {
    partial_results_.push_back(results[0]);
    start = 1;
    length--;
  } else {
    start = 0;
  }
  std::vector<std::shared_ptr<Result>> prepend_results{};
  prepend_results.push_back(Combine());
  std::copy_n(results.begin() + start, length, std::back_inserter(prepend_results));
  return prepend_results;
}

std::vector<std::shared_ptr<Result>> ScanResultCache::UpdateNumberOfCompleteResultsAndReturn(
    const std::shared_ptr<Result> &result) {
  return UpdateNumberOfCompleteResultsAndReturn(std::vector<std::shared_ptr<Result>>{result});
}

std::vector<std::shared_ptr<Result>> ScanResultCache::UpdateNumberOfCompleteResultsAndReturn(
    const std::vector<std::shared_ptr<Result>> &results) {
  num_complete_rows_ += results.size();
  return results;
}
}  // namespace hbase
