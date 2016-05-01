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

#include "connection/connection-factory.h"

#include <folly/futures/Future.h>
#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/channel/OutputBufferingHandler.h>
#include <wangle/service/ClientDispatcher.h>
#include <wangle/service/CloseOnReleaseFilter.h>
#include <wangle/service/ExpiringFilter.h>

#include <string>

#include "connection/client-dispatcher.h"
#include "connection/pipeline.h"
#include "connection/request.h"
#include "connection/response.h"
#include "connection/service.h"

using namespace folly;
using namespace hbase;
using namespace wangle;

ConnectionFactory::ConnectionFactory() : bootstrap_() {
  bootstrap_.group(std::make_shared<wangle::IOThreadPoolExecutor>(1));
  bootstrap_.pipelineFactory(std::make_shared<RpcPipelineFactory>());
}

std::shared_ptr<HBaseService>
ConnectionFactory::make_connection(const std::string &host, int port) {
  // Connect to a given server
  // Then when connected create a ClientDispactcher.
  auto pipeline = bootstrap_.connect(SocketAddress(host, port, true)).get();
  auto dispatcher = std::make_shared<ClientDispatcher>();
  dispatcher->setPipeline(pipeline);
  auto service = std::make_shared<
      CloseOnReleaseFilter<std::unique_ptr<Request>, Response>>(dispatcher);
  return service;
}
