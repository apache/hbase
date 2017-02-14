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

#include "core/response_converter.h"

#include <vector>

#include "core/cell.h"

using hbase::pb::GetResponse;
using hbase::pb::ScanResponse;

namespace hbase {

ResponseConverter::ResponseConverter() {}

ResponseConverter::~ResponseConverter() {}

std::unique_ptr<Result> ResponseConverter::FromGetResponse(const Response& resp) {
  auto get_resp = std::static_pointer_cast<GetResponse>(resp.resp_msg());

  return ToResult(get_resp->result(), resp.cell_scanner());
}

std::unique_ptr<Result> ResponseConverter::ToResult(
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
    while (cell_scanner->Advance()) {
      vcells.push_back(cell_scanner->Current());
    }
    // TODO: check associated cell count?
  }
  return std::make_unique<Result>(vcells, result.exists(), result.stale(), result.partial());
}

std::vector<std::unique_ptr<Result>> ResponseConverter::FromScanResponse(const Response& resp) {
  auto scan_resp = std::static_pointer_cast<ScanResponse>(resp.resp_msg());
  VLOG(3) << "FromScanResponse:" << scan_resp->ShortDebugString();
  int num_results = resp.cell_scanner() != nullptr ? scan_resp->cells_per_result_size()
                                                   : scan_resp->results_size();

  std::vector<std::unique_ptr<Result>> results{static_cast<size_t>(num_results)};
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
      results[i] = std::make_unique<Result>(vcells, false, scan_resp->stale(), false);
    } else {
      results[i] = ToResult(scan_resp->results(i), resp.cell_scanner());
    }
  }

  return results;
}
} /* namespace hbase */
