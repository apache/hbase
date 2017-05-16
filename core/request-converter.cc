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

#include "core/request-converter.h"

#include <folly/Conv.h>

#include <utility>
#include "if/Client.pb.h"

using hbase::Request;
using hbase::pb::GetRequest;
using hbase::pb::RegionAction;
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
  pb_msg->set_allocated_get((RequestConverter::ToGet(get)).release());
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
    for (const auto &family : scan.FamilyMap()) {
      auto column = pb_scan->add_column();
      column->set_family(family.first);
      for (const auto &qualifier : family.second) {
        column->add_qualifier(qualifier);
      }
    }
  }

  if (scan.filter() != nullptr) {
    pb_scan->set_allocated_filter(Filter::ToProto(*(scan.filter())).release());
  }

  // TODO We will change this later.
  pb_msg->set_client_handles_partials(false);
  pb_msg->set_client_handles_heartbeats(false);
  pb_msg->set_track_scan_metrics(false);

  return pb_req;
}

std::unique_ptr<Request> RequestConverter::ToMultiRequest(
    const ActionsByRegion &actions_by_region) {
  auto pb_req = Request::multi();
  auto pb_msg = std::static_pointer_cast<hbase::pb::MultiRequest>(pb_req->req_msg());

  for (const auto &action_by_region : actions_by_region) {
    auto pb_region_action = pb_msg->add_regionaction();
    RequestConverter::SetRegion(action_by_region.first, pb_region_action->mutable_region());
    int action_num = 0;
    for (const auto &region_action : action_by_region.second->actions()) {
      auto pb_action = pb_region_action->add_action();
      auto pget = region_action->action();
      // We store only hbase::Get in hbase::Action as of now. It will be changed later on.
      CHECK(pget) << "Unexpected. action can't be null";
      auto pb_get = RequestConverter::ToGet(*pget);
      pb_action->set_allocated_get(pb_get.release());
      pb_action->set_index(action_num);
      action_num++;
    }
  }

  VLOG(3) << "Multi Req:-" << pb_req->req_msg()->ShortDebugString();
  return pb_req;
}

std::unique_ptr<hbase::pb::Get> RequestConverter::ToGet(const Get &get) {
  auto pb_get = std::make_unique<hbase::pb::Get>();
  pb_get->set_max_versions(get.MaxVersions());
  pb_get->set_cache_blocks(get.CacheBlocks());
  pb_get->set_consistency(get.Consistency());

  if (!get.Timerange().IsAllTime()) {
    hbase::pb::TimeRange *pb_time_range = pb_get->mutable_time_range();
    pb_time_range->set_from(get.Timerange().MinTimeStamp());
    pb_time_range->set_to(get.Timerange().MaxTimeStamp());
  }
  pb_get->set_row(get.row());
  if (get.HasFamilies()) {
    for (const auto &family : get.FamilyMap()) {
      auto column = pb_get->add_column();
      column->set_family(family.first);
      for (const auto &qualifier : family.second) {
        column->add_qualifier(qualifier);
      }
    }
  }

  if (get.filter() != nullptr) {
    pb_get->set_allocated_filter(Filter::ToProto(*(get.filter())).release());
  }
  return pb_get;
}

std::unique_ptr<MutationProto> RequestConverter::ToMutation(const MutationType type,
                                                            const Mutation &mutation,
                                                            const int64_t nonce) {
  auto pb_mut = std::make_unique<MutationProto>();
  pb_mut->set_row(mutation.row());
  pb_mut->set_mutate_type(type);
  pb_mut->set_durability(mutation.Durability());
  pb_mut->set_timestamp(mutation.TimeStamp());
  // TODO: set attributes from the mutation (key value pairs).

  if (nonce > 0) {
    pb_mut->set_nonce(nonce);
  }

  for (const auto &family : mutation.FamilyMap()) {
    for (const auto &cell : family.second) {
      auto column = pb_mut->add_column_value();
      column->set_family(cell->Family());
      auto qual = column->add_qualifier_value();
      qual->set_qualifier(cell->Qualifier());
      qual->set_timestamp(cell->Timestamp());
      auto cell_type = cell->Type();
      if (type == pb::MutationProto_MutationType_DELETE ||
          (type == pb::MutationProto_MutationType_PUT && IsDelete(cell_type))) {
        qual->set_delete_type(ToDeleteType(cell_type));
      }

      qual->set_value(cell->Value());
    }
  }
  return std::move(pb_mut);
}

DeleteType RequestConverter::ToDeleteType(const CellType type) {
  switch (type) {
    case DELETE:
      return pb::MutationProto_DeleteType_DELETE_ONE_VERSION;
    case DELETE_COLUMN:
      return pb::MutationProto_DeleteType_DELETE_MULTIPLE_VERSIONS;
    case DELETE_FAMILY:
      return pb::MutationProto_DeleteType_DELETE_FAMILY;
    case DELETE_FAMILY_VERSION:
      return pb::MutationProto_DeleteType_DELETE_FAMILY_VERSION;
    default:
      throw std::runtime_error("Unknown delete type: " + folly::to<std::string>(type));
  }
}

bool RequestConverter::IsDelete(const CellType type) {
  return CellType::DELETE <= type && type <= CellType::DELETE_FAMILY;
}

std::unique_ptr<Request> RequestConverter::ToMutateRequest(const Put &put,
                                                           const std::string &region_name) {
  auto pb_req = Request::mutate();
  auto pb_msg = std::static_pointer_cast<hbase::pb::MutateRequest>(pb_req->req_msg());
  RequestConverter::SetRegion(region_name, pb_msg->mutable_region());

  pb_msg->set_allocated_mutation(
      ToMutation(MutationType::MutationProto_MutationType_PUT, put, -1).release());

  VLOG(3) << "Req is " << pb_req->req_msg()->ShortDebugString();
  return pb_req;
}
} /* namespace hbase */
