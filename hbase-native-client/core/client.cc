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

#include <unistd.h>
#include <string>

using namespace folly;
using namespace std;
using namespace hbase::pb;

namespace hbase {

Client::Client(std::string zk_quorum)
    : cpu_executor_(std::make_shared<wangle::CPUThreadPoolExecutor>(4)),
      io_executor_(std::make_shared<wangle::IOThreadPoolExecutor>(
          sysconf(_SC_NPROCESSORS_ONLN))),
      location_cache_(zk_quorum, cpu_executor_, io_executor_) {}

// We can't have the threads continue running after everything is done
// that leads to an error.
Client::~Client() {
  cpu_executor_->stop();
  io_executor_->stop();
}

}  // namespace hbase
