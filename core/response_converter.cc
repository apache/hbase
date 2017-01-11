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
#include "if/Client.pb.h"

using hbase::pb::GetResponse;

namespace hbase {

ResponseConverter::ResponseConverter() {}

ResponseConverter::~ResponseConverter() {}

std::unique_ptr<hbase::Result> ResponseConverter::FromGetResponse(const Response& resp) {
  auto get_resp = std::static_pointer_cast<GetResponse>(resp.resp_msg());

  std::vector<std::shared_ptr<Cell>> vcells;
  for (auto cell : get_resp->result().cell()) {
    std::shared_ptr<Cell> pcell =
        std::make_shared<Cell>(cell.row(), cell.family(), cell.qualifier(), cell.timestamp(),
                               cell.value(), static_cast<hbase::CellType>(cell.cell_type()));
    vcells.push_back(pcell);
  }

  return std::make_unique<hbase::Result>(vcells, get_resp->result().exists(),
                                         get_resp->result().stale(), get_resp->result().partial());
}

} /* namespace hbase */
