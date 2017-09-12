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

#include <folly/Conv.h>
#include <glog/logging.h>
#include <wangle/channel/Handler.h>

#include <chrono>

#include <folly/ExceptionWrapper.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocketException.h>

#include "hbase/connection/client-dispatcher.h"
#include "hbase/connection/connection-factory.h"
#include "hbase/connection/pipeline.h"
#include "hbase/connection/sasl-handler.h"
#include "hbase/connection/service.h"
#include "hbase/exceptions/exception.h"

using std::chrono::milliseconds;
using std::chrono::nanoseconds;

namespace hbase {

ConnectionFactory::ConnectionFactory(std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor,
                                     std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor,
                                     std::shared_ptr<Codec> codec,
                                     std::shared_ptr<Configuration> conf,
                                     nanoseconds connect_timeout)
    : connect_timeout_(connect_timeout),
      io_executor_(io_executor),
      cpu_executor_(cpu_executor),
      conf_(conf),
      pipeline_factory_(std::make_shared<RpcPipelineFactory>(codec, conf)) {}

std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>> ConnectionFactory::MakeBootstrap() {
  auto client = std::make_shared<wangle::ClientBootstrap<SerializePipeline>>();
  client->group(io_executor_);
  client->pipelineFactory(pipeline_factory_);

  // TODO: Opened https://github.com/facebook/wangle/issues/85 in wangle so that we can set socket
  //  options like TCP_NODELAY, SO_KEEPALIVE, CONNECT_TIMEOUT_MILLIS, etc.

  return client;
}

std::shared_ptr<HBaseService> ConnectionFactory::Connect(
    std::shared_ptr<RpcConnection> rpc_connection,
    std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>> client_bootstrap,
    const std::string &hostname, uint16_t port) {
  // connection should happen from an IO thread
  try {
    auto future = via(io_executor_.get()).then([=]() {
      VLOG(1) << "Connecting to server: " << hostname << ":" << port;
      return client_bootstrap->connect(folly::SocketAddress(hostname, port, true),
                                       std::chrono::duration_cast<milliseconds>(connect_timeout_));
    });

    // See about using shared promise for this.
    auto pipeline = future.get();

    VLOG(1) << "Connected to server: " << hostname << ":" << port;
    auto dispatcher =
        std::make_shared<ClientDispatcher>(hostname + ":" + folly::to<std::string>(port));
    dispatcher->setPipeline(pipeline);
    return dispatcher;
  } catch (const folly::AsyncSocketException &e) {
    throw ConnectionException(folly::exception_wrapper{e});
  }
}
}  // namespace hbase
