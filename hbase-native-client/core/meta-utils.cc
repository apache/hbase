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

#include "core/meta-utils.h"

#include <folly/Conv.h>

#include "connection/request.h"
#include "connection/response.h"
#include "if/Client.pb.h"
#include "serde/table-name.h"

using hbase::pb::TableName;
using hbase::MetaUtil;
using hbase::Request;
using hbase::Response;
using hbase::pb::ScanRequest;
using hbase::pb::ServerName;
using hbase::pb::RegionSpecifier_RegionSpecifierType;

static const std::string META_REGION = "1588230740";

std::string MetaUtil::RegionLookupRowkey(const TableName &tn,
                                           const std::string &row) const {
  return folly::to<std::string>(tn, ",", row, ",", "999999999999999999");
}

std::unique_ptr<Request>
MetaUtil::MetaRequest(const TableName tn, const std::string &row) const {
  auto request = Request::scan();
  auto msg = std::static_pointer_cast<ScanRequest>(request->req_msg());

  msg->set_number_of_rows(1);
  msg->set_close_scanner(true);

  // Set the region this scan goes to
  auto region = msg->mutable_region();
  region->set_value(META_REGION);
  region->set_type(RegionSpecifier_RegionSpecifierType::
                       RegionSpecifier_RegionSpecifierType_ENCODED_REGION_NAME);

  auto scan = msg->mutable_scan();
  // We don't care about before, just now.
  scan->set_max_versions(1);
  // Meta should be cached at all times.
  scan->set_cache_blocks(true);
  // We only want one row right now.
  //
  // TODO(eclark): Figure out if we should get more.
  scan->set_caching(1);
  // Close the scan after we have data.
  scan->set_small(true);
  // We know where to start but not where to end.
  scan->set_reversed(true);
  // Give me everything or nothing.
  scan->set_allow_partial_results(false);

  // Set the columns that we need
  auto info_col = scan->add_column();
  info_col->set_family("info");
  info_col->add_qualifier("server");
  info_col->add_qualifier("regioninfo");

  scan->set_start_row(RegionLookupRowkey(tn, row));
  return request;
}
