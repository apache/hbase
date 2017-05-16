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

#include <chrono>
#include <limits>
#include <utility>
#include <vector>

#include "core/async-connection.h"
#include "core/request-converter.h"
#include "core/response-converter.h"
#include "if/Client.pb.h"
#include "security/user.h"
#include "serde/server-name.h"
#include "utils/time-util.h"

using hbase::pb::TableName;
using hbase::security::User;
using std::chrono::milliseconds;

namespace hbase {

Table::Table(const TableName &table_name, std::shared_ptr<AsyncConnection> async_connection)
    : table_name_(std::make_shared<TableName>(table_name)),
      async_connection_(async_connection),
      conf_(async_connection->conf()) {
  async_table_ = std::make_unique<RawAsyncTable>(table_name_, async_connection);
}

Table::~Table() {}

std::shared_ptr<hbase::Result> Table::Get(const hbase::Get &get) {
  auto context = async_table_->Get(get);
  return context.get(operation_timeout());
}

void Table::Put(const hbase::Put &put) {
  auto future = async_table_->Put(put);
  future.get(operation_timeout());
}

milliseconds Table::operation_timeout() const {
  return TimeUtil::ToMillis(async_connection_->connection_conf()->operation_timeout());
}

void Table::Close() { async_table_->Close(); }

std::shared_ptr<RegionLocation> Table::GetRegionLocation(const std::string &row) {
  return async_connection_->region_locator()->LocateRegion(*table_name_, row).get();
}

std::vector<std::shared_ptr<hbase::Result>> Table::Get(const std::vector<hbase::Get> &gets) {
  auto tresults = async_table_->Get(gets).get(operation_timeout());
  std::vector<std::shared_ptr<hbase::Result>> results{};
  uint32_t num = 0;
  for (auto tresult : tresults) {
    if (tresult.hasValue()) {
      results.push_back(tresult.value());
    } else if (tresult.hasException()) {
      LOG(ERROR) << "Caught exception:- " << tresult.exception().getCopied()->what() << " for "
                 << gets[num++].row();
      throw tresult.exception().getCopied();
    }
  }
  return results;
}

} /* namespace hbase */
