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

#include "core/client.h"

#include <glog/logging.h>
#include <exception>
#include <utility>

namespace hbase {

Client::Client() {
  HBaseConfigurationLoader loader;
  auto conf = loader.LoadDefaultResources();
  if (!conf) {
    LOG(ERROR) << "Unable to create default Configuration object. Either hbase-default.xml or "
                  "hbase-site.xml is absent in the search path or problems in XML parsing";
    throw std::runtime_error("Configuration object not present.");
  }
  conf_ = std::make_shared<hbase::Configuration>(conf.value());
  auto zk_quorum = conf_->Get(kHBaseZookeeperQuorum_, kDefHBaseZookeeperQuorum_);
  location_cache_ = std::make_shared<hbase::LocationCache>(zk_quorum, cpu_executor_, io_executor_);
}

Client::Client(const hbase::Configuration &conf) {
  conf_ = std::make_shared<hbase::Configuration>(conf);
  auto zk_quorum = conf_->Get(kHBaseZookeeperQuorum_, kDefHBaseZookeeperQuorum_);
  location_cache_ = std::make_shared<hbase::LocationCache>(zk_quorum, cpu_executor_, io_executor_);
}

// We can't have the threads continue running after everything is done
// that leads to an error.
Client::~Client() {
  cpu_executor_->stop();
  io_executor_->stop();
  if (rpc_client_.get()) rpc_client_->Close();
}

std::unique_ptr<hbase::Table> Client::Table(const TableName &table_name) {
  return std::make_unique<hbase::Table>(table_name, location_cache_, rpc_client_, conf_);
}

void Client::Close() {
  if (is_closed_) return;

  cpu_executor_->stop();
  io_executor_->stop();
  if (rpc_client_.get()) rpc_client_->Close();
  is_closed_ = true;
}

}  // namespace hbase
