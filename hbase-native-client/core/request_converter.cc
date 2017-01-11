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

#include "core/request_converter.h"
#include <utility>
#include "if/Client.pb.h"

using hbase::Request;
using hbase::pb::GetRequest;
using hbase::pb::RegionSpecifier;
using hbase::pb::RegionSpecifier_RegionSpecifierType;
using hbase::pb::ScanRequest;

namespace hbase {

RequestConverter::~RequestConverter() {}

RequestConverter::RequestConverter() {}

void RequestConverter::SetRegion(const std::string &region_name,
                                 RegionSpecifier *region_specifier) {
  region_specifier->set_type(
      RegionSpecifier_RegionSpecifierType::RegionSpecifier_RegionSpecifierType_REGION_NAME);
  region_specifier->set_value(region_name);
}

std::unique_ptr<Request> RequestConverter::ToGetRequest(const Get &get,
                                                        const std::string &region_name) {
  auto pb_req = Request::get();

  auto pb_msg = std::static_pointer_cast<GetRequest>(pb_req->req_msg());
  RequestConverter::SetRegion(region_name, pb_msg->mutable_region());

  auto pb_get = pb_msg->mutable_get();
  pb_get->set_max_versions(get.MaxVersions());
  pb_get->set_cache_blocks(get.CacheBlocks());
  pb_get->set_consistency(get.Consistency());

  if (!get.Timerange().IsAllTime()) {
    hbase::pb::TimeRange *pb_time_range = pb_get->mutable_time_range();
    pb_time_range->set_from(get.Timerange().MinTimeStamp());
    pb_time_range->set_to(get.Timerange().MaxTimeStamp());
  }
  pb_get->set_row(get.Row());
  if (get.HasFamilies()) {
    for (const auto &family : get.Family()) {
      auto column = pb_get->add_column();
      column->set_family(family.first);
      for (const auto &qualifier : family.second) {
        column->add_qualifier(qualifier);
      }
    }
  }

  return pb_req;
}

std::unique_ptr<Request> RequestConverter::ToScanRequest(const Scan &scan,
                                                         const std::string &region_name) {
  auto pb_req = Request::scan();

  auto pb_msg = std::static_pointer_cast<ScanRequest>(pb_req->req_msg());

  RequestConverter::SetRegion(region_name, pb_msg->mutable_region());

  auto pb_scan = pb_msg->mutable_scan();
  pb_scan->set_max_versions(scan.MaxVersions());
  pb_scan->set_cache_blocks(scan.CacheBlocks());
  pb_scan->set_reversed(scan.IsReversed());
  pb_scan->set_small(scan.IsSmall());
  pb_scan->set_caching(scan.Caching());
  pb_scan->set_start_row(scan.StartRow());
  pb_scan->set_stop_row(scan.StopRow());
  pb_scan->set_consistency(scan.Consistency());
  pb_scan->set_max_result_size(scan.MaxResultSize());
  pb_scan->set_allow_partial_results(scan.AllowPartialResults());
  pb_scan->set_load_column_families_on_demand(scan.LoadColumnFamiliesOnDemand());

  if (!scan.Timerange().IsAllTime()) {
    hbase::pb::TimeRange *pb_time_range = pb_scan->mutable_time_range();
    pb_time_range->set_from(scan.Timerange().MinTimeStamp());
    pb_time_range->set_to(scan.Timerange().MaxTimeStamp());
  }

  if (scan.HasFamilies()) {
    for (const auto &family : scan.Family()) {
      auto column = pb_scan->add_column();
      column->set_family(family.first);
      for (const auto &qualifier : family.second) {
        column->add_qualifier(qualifier);
      }
    }
  }

  // TODO We will change this later.
  pb_msg->set_client_handles_partials(false);
  pb_msg->set_client_handles_heartbeats(false);
  pb_msg->set_track_scan_metrics(false);

  return pb_req;
}
} /* namespace hbase */
