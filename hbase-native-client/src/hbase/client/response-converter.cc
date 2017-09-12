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

#include "hbase/client/response-converter.h"
#include <glog/logging.h>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "hbase/client/cell.h"
#include "hbase/client/multi-response.h"
#include "hbase/exceptions/exception.h"

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

bool ResponseConverter::BoolFromMutateResponse(const Response& resp) {
  auto mutate_resp = std::static_pointer_cast<MutateResponse>(resp.resp_msg());
  return mutate_resp->processed();
}

std::shared_ptr<Result> ResponseConverter::ToResult(
    const hbase::pb::Result& result, const std::shared_ptr<CellScanner> cell_scanner) {
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
  return FromScanResponse(scan_resp, resp.cell_scanner());
}

std::vector<std::shared_ptr<Result>> ResponseConverter::FromScanResponse(
    const std::shared_ptr<ScanResponse> scan_resp, std::shared_ptr<CellScanner> cell_scanner) {
  VLOG(3) << "FromScanResponse:" << scan_resp->ShortDebugString()
          << " cell_scanner:" << (cell_scanner != nullptr);
  int num_results =
      cell_scanner != nullptr ? scan_resp->cells_per_result_size() : scan_resp->results_size();

  std::vector<std::shared_ptr<Result>> results{static_cast<size_t>(num_results)};
  for (int i = 0; i < num_results; i++) {
    if (cell_scanner != nullptr) {
      // Cells are out in cellblocks.  Group them up again as Results.  How many to read at a
      // time will be found in getCellsLength -- length here is how many Cells in the i'th Result
      int num_cells = scan_resp->cells_per_result(i);

      std::vector<std::shared_ptr<Cell>> vcells;
      for (int j = 0; j < num_cells; j++) {
        if (!cell_scanner->Advance()) {
          std::string msg = "Results sent from server=" + std::to_string(num_results) +
                            ". But only got " + std::to_string(i) +
                            " results completely at client. Resetting the scanner to scan again.";
          LOG(ERROR) << msg;
          throw std::runtime_error(msg);
        }
        vcells.push_back(cell_scanner->Current());
      }
      // TODO: handle partial results per Result by checking partial_flag_per_result
      results[i] = std::make_shared<Result>(vcells, false, scan_resp->stale(), false);
    } else {
      results[i] = ToResult(scan_resp->results(i), cell_scanner);
    }
  }

  return results;
}

std::unique_ptr<hbase::MultiResponse> ResponseConverter::GetResults(
    std::shared_ptr<Request> req, const Response& resp,
    const ServerRequest::ActionsByRegion& actions_by_region) {
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
      auto ew = ResponseConverter::GetRemoteException(action_result.exception());
      VLOG(8) << "Store Remote Region Exception:- " << ew->what().toStdString() << "; Region["
              << region_name << "];";
      multi_response->AddRegionException(region_name, ew);
      continue;
    }

    if (actions.action_size() != action_result.resultorexception_size()) {
      throw std::runtime_error("actions.action_size=" + std::to_string(actions.action_size()) +
                               ", action_result.resultorexception_size=" +
                               std::to_string(action_result.resultorexception_size()) +
                               " for region " + actions.region().value());
    }

    auto multi_actions = actions_by_region.at(region_name)->actions();
    uint64_t multi_actions_num = 0;
    for (hbase::pb::ResultOrException roe : action_result.resultorexception()) {
      std::shared_ptr<Result> result;
      std::shared_ptr<folly::exception_wrapper> ew;
      if (roe.has_exception()) {
        auto ew = ResponseConverter::GetRemoteException(roe.exception());
        VLOG(8) << "Store Remote Region Exception:- " << ew->what().toStdString() << "; Region["
                << region_name << "];";
        multi_response->AddRegionException(region_name, ew);
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
      // We add the original index of the multi-action so that when populating the response back we
      // do it as per the action index
      multi_response->AddRegionResult(
          region_name, multi_actions[multi_actions_num]->original_index(), std::move(result), ew);
      multi_actions_num++;
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

std::shared_ptr<folly::exception_wrapper> ResponseConverter::GetRemoteException(
    const hbase::pb::NameBytesPair& exc_resp) {
  std::string what;
  std::string exception_class_name = exc_resp.has_name() ? exc_resp.name() : "";
  std::string stack_trace = exc_resp.has_value() ? exc_resp.value() : "";

  what.append(exception_class_name).append(stack_trace);
  auto remote_exception = std::make_unique<RemoteException>(what);
  remote_exception->set_exception_class_name(exception_class_name)
      ->set_stack_trace(stack_trace)
      ->set_hostname("")
      ->set_port(0);

  return std::make_shared<folly::exception_wrapper>(
      folly::make_exception_wrapper<RemoteException>(*remote_exception));
}
} /* namespace hbase */
