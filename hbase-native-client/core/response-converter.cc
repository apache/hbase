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

#include "core/response-converter.h"

#include <glog/logging.h>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "core/cell.h"
#include "core/multi-response.h"
#include "exceptions/exception.h"

using hbase::pb::GetResponse;
using hbase::pb::MutateResponse;
using hbase::pb::ScanResponse;
using hbase::pb::RegionLoadStats;

namespace hbase {

ResponseConverter::ResponseConverter() {}

ResponseConverter::~ResponseConverter() {}

// impl note: we are returning shared_ptr's instead of unique_ptr's because these
// go inside folly::Future's, making the move semantics extremely tricky.
std::shared_ptr<Result> ResponseConverter::FromGetResponse(const Response& resp) {
  auto get_resp = std::static_pointer_cast<GetResponse>(resp.resp_msg());
  VLOG(3) << "FromGetResponse:" << get_resp->ShortDebugString();
  return ToResult(get_resp->result(), resp.cell_scanner());
}

std::shared_ptr<Result> ResponseConverter::FromMutateResponse(const Response& resp) {
  auto mutate_resp = std::static_pointer_cast<MutateResponse>(resp.resp_msg());
  hbase::pb::Result result = mutate_resp->result();
  return ToResult(mutate_resp->result(), resp.cell_scanner());
}

std::shared_ptr<Result> ResponseConverter::ToResult(
    const hbase::pb::Result& result, const std::unique_ptr<CellScanner>& cell_scanner) {
  std::vector<std::shared_ptr<Cell>> vcells;
  for (auto cell : result.cell()) {
    std::shared_ptr<Cell> pcell =
        std::make_shared<Cell>(cell.row(), cell.family(), cell.qualifier(), cell.timestamp(),
                               cell.value(), static_cast<hbase::CellType>(cell.cell_type()));
    vcells.push_back(pcell);
  }

  // iterate over the cells coming from rpc codec
  if (cell_scanner != nullptr) {
    int cells_read = 0;
    while (cells_read != result.associated_cell_count()) {
      if (cell_scanner->Advance()) {
        vcells.push_back(cell_scanner->Current());
        cells_read += 1;
      } else {
        LOG(ERROR) << "CellScanner::Advance() returned false unexpectedly. Cells Read:- "
                   << cells_read << "; Expected Cell Count:- " << result.associated_cell_count();
        std::runtime_error("CellScanner::Advance() returned false unexpectedly");
      }
    }
  }
  return std::make_shared<Result>(vcells, result.exists(), result.stale(), result.partial());
}

std::vector<std::shared_ptr<Result>> ResponseConverter::FromScanResponse(const Response& resp) {
  auto scan_resp = std::static_pointer_cast<ScanResponse>(resp.resp_msg());
  VLOG(3) << "FromScanResponse:" << scan_resp->ShortDebugString();
  int num_results = resp.cell_scanner() != nullptr ? scan_resp->cells_per_result_size()
                                                   : scan_resp->results_size();

  std::vector<std::shared_ptr<Result>> results{static_cast<size_t>(num_results)};
  for (int i = 0; i < num_results; i++) {
    if (resp.cell_scanner() != nullptr) {
      // Cells are out in cellblocks.  Group them up again as Results.  How many to read at a
      // time will be found in getCellsLength -- length here is how many Cells in the i'th Result
      int num_cells = scan_resp->cells_per_result(i);

      std::vector<std::shared_ptr<Cell>> vcells;
      while (resp.cell_scanner()->Advance()) {
        vcells.push_back(resp.cell_scanner()->Current());
      }
      // TODO: check associated cell count?

      if (vcells.size() != num_cells) {
        std::string msg = "Results sent from server=" + std::to_string(num_results) +
                          ". But only got " + std::to_string(i) +
                          " results completely at client. Resetting the scanner to scan again.";
        LOG(ERROR) << msg;
        throw std::runtime_error(msg);
      }
      // TODO: handle partial results per Result by checking partial_flag_per_result
      results[i] = std::make_shared<Result>(vcells, false, scan_resp->stale(), false);
    } else {
      results[i] = ToResult(scan_resp->results(i), resp.cell_scanner());
    }
  }

  return results;
}

std::unique_ptr<hbase::MultiResponse> ResponseConverter::GetResults(std::shared_ptr<Request> req,
                                                                    const Response& resp) {
  auto multi_req = std::static_pointer_cast<hbase::pb::MultiRequest>(req->req_msg());
  auto multi_resp = std::static_pointer_cast<hbase::pb::MultiResponse>(resp.resp_msg());
  VLOG(3) << "GetResults:" << multi_resp->ShortDebugString();
  int req_region_action_count = multi_req->regionaction_size();
  int res_region_action_count = multi_resp->regionactionresult_size();
  if (req_region_action_count != res_region_action_count) {
    throw std::runtime_error("Request mutation count=" + std::to_string(req_region_action_count) +
                             " does not match response mutation result count=" +
                             std::to_string(res_region_action_count));
  }
  auto multi_response = std::make_unique<hbase::MultiResponse>();
  for (int32_t num = 0; num < res_region_action_count; num++) {
    hbase::pb::RegionAction actions = multi_req->regionaction(num);
    hbase::pb::RegionActionResult action_result = multi_resp->regionactionresult(num);
    hbase::pb::RegionSpecifier rs = actions.region();
    if (rs.has_type() && rs.type() != hbase::pb::RegionSpecifier::REGION_NAME) {
      throw std::runtime_error("We support only encoded types for protobuf multi response.");
    }

    auto region_name = rs.value();
    if (action_result.has_exception()) {
      if (action_result.exception().has_value()) {
        auto exc = std::make_shared<hbase::IOException>(action_result.exception().value());
        VLOG(8) << "Store Region Exception:- " << exc->what();
        multi_response->AddRegionException(region_name, exc);
      }
      continue;
    }

    if (actions.action_size() != action_result.resultorexception_size()) {
      throw std::runtime_error("actions.action_size=" + std::to_string(actions.action_size()) +
                               ", action_result.resultorexception_size=" +
                               std::to_string(action_result.resultorexception_size()) +
                               " for region " + actions.region().value());
    }

    for (hbase::pb::ResultOrException roe : action_result.resultorexception()) {
      std::shared_ptr<Result> result;
      std::shared_ptr<std::exception> exc;
      if (roe.has_exception()) {
        if (roe.exception().has_value()) {
          exc = std::make_shared<hbase::IOException>(roe.exception().value());
          VLOG(8) << "Store ResultOrException:- " << exc->what();
        }
      } else if (roe.has_result()) {
        result = ToResult(roe.result(), resp.cell_scanner());
      } else if (roe.has_service_result()) {
        // TODO Not processing Coprocessor Service Result;
      } else {
        // Sometimes, the response is just "it was processed". Generally, this occurs for things
        // like mutateRows where either we get back 'processed' (or not) and optionally some
        // statistics about the regions we touched.
        std::vector<std::shared_ptr<Cell>> empty_cells;
        result = std::make_shared<Result>(empty_cells, multi_resp->processed() ? true : false,
                                          false, false);
      }
      multi_response->AddRegionResult(region_name, roe.index(), std::move(result), exc);
    }
  }

  if (multi_resp->has_regionstatistics()) {
    hbase::pb::MultiRegionLoadStats stats = multi_resp->regionstatistics();
    for (int i = 0; i < stats.region_size(); i++) {
      multi_response->AddStatistic(stats.region(i).value(),
                                   std::make_shared<RegionLoadStats>(stats.stat(i)));
    }
  }
  return multi_response;
}
} /* namespace hbase */
