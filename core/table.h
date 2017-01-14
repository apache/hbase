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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "connection/rpc-client.h"
#include "core/client.h"
#include "core/configuration.h"
#include "core/get.h"
#include "core/location-cache.h"
#include "core/result.h"
#include "serde/table-name.h"

using hbase::pb::TableName;

namespace hbase {
class Client;

class Table {
 public:
  /**
   * Constructors
   */
  Table(const TableName &table_name, const std::shared_ptr<hbase::LocationCache> &location_cache,
        const std::shared_ptr<hbase::RpcClient> &rpc_client,
        const std::shared_ptr<hbase::Configuration> &conf);
  ~Table();

  /**
   * @brief - Returns a Result object for the constructed Get.
   * @param - get Get object to perform HBase Get operation.
   */
  std::unique_ptr<hbase::Result> Get(const hbase::Get &get);

  /**
   * @brief - Close the client connection.
   */
  void Close();

 private:
  std::shared_ptr<TableName> table_name_;
  std::shared_ptr<hbase::LocationCache> location_cache_;
  std::shared_ptr<hbase::RpcClient> rpc_client_;
  std::shared_ptr<hbase::Configuration> conf_;
  bool is_closed_ = false;
  // default 5 retries. over-ridden in constructor.
  int client_retries_ = 5;
};
} /* namespace hbase */
