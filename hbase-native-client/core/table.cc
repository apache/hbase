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

#include "core/table.h"

#include <folly/futures/Future.h>
#include <chrono>
#include <limits>
#include <utility>
#include <vector>

#include "core/request_converter.h"
#include "core/response_converter.h"
#include "if/Client.pb.h"
#include "security/user.h"
#include "serde/server-name.h"

using folly::Future;
using hbase::pb::TableName;
using hbase::security::User;
using std::chrono::milliseconds;

namespace hbase {

Table::Table(const TableName &table_name,
             const std::shared_ptr<hbase::LocationCache> &location_cache,
             const std::shared_ptr<hbase::RpcClient> &rpc_client,
             const std::shared_ptr<hbase::Configuration> &conf)
    : table_name_(std::make_shared<TableName>(table_name)),
      location_cache_(location_cache),
      rpc_client_(rpc_client),
      conf_(conf) {
  client_retries_ = (conf_) ? conf_->GetInt("hbase.client.retries", client_retries_) : 5;
}

Table::~Table() {}

std::unique_ptr<hbase::Result> Table::Get(const hbase::Get &get) {
  auto loc = location_cache_->LocateFromMeta(*table_name_, get.Row()).get(milliseconds(1000));
  auto req = hbase::RequestConverter::ToGetRequest(get, loc->region_name());
  auto user = User::defaultUser();  // TODO: make User::current() similar to UserUtil

  Future<Response> f =
      rpc_client_->AsyncCall(loc->server_name().host_name(), loc->server_name().port(),
                             std::move(req), user, "ClientService");
  auto resp = f.get();

  return hbase::ResponseConverter::FromGetResponse(resp);
}

void Table::Close() {
  if (is_closed_) return;

  if (rpc_client_.get()) rpc_client_->Close();
  is_closed_ = true;
}

} /* namespace hbase */
