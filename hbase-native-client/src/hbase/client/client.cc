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

#include "hbase/client/client.h"

#include <glog/logging.h>
#include <chrono>
#include <exception>
#include <memory>
#include <utility>

using hbase::pb::TableName;

namespace hbase {

Client::Client() {
  HBaseConfigurationLoader loader;
  auto conf = loader.LoadDefaultResources();
  if (!conf) {
    LOG(ERROR) << "Unable to create default Configuration object. Either hbase-default.xml or "
                  "hbase-site.xml is absent in the search path or problems in XML parsing";
    throw std::runtime_error("Configuration object not present.");
  }
  Init(conf.value());
}

Client::Client(const Configuration &conf) { Init(conf); }

void Client::Init(const Configuration &conf) {
  auto conf_ = std::make_shared<Configuration>(conf);
  async_connection_ = AsyncConnectionImpl::Create(conf_);
}

std::unique_ptr<Table> Client::Table(const TableName &table_name) {
  return std::make_unique<hbase::Table>(table_name, async_connection_);
}

void Client::Close() { async_connection_->Close(); }
}  // namespace hbase
