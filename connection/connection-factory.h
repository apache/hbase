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

#include <wangle/concurrent/CPUThreadPoolExecutor.h>
#include <wangle/concurrent/IOThreadPoolExecutor.h>
#include <wangle/service/Service.h>

#include <chrono>
#include <memory>
#include <string>

#include "connection/pipeline.h"
#include "connection/request.h"
#include "connection/response.h"
#include "connection/service.h"
#include "security/user.h"

namespace hbase {

class RpcConnection;

/**
 * Class to create a ClientBootstrap and turn it into a connected
 * pipeline.
 */
class ConnectionFactory {
 public:
  /**
   * Constructor.
   * There should only be one ConnectionFactory per client.
   */
  ConnectionFactory(std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor,
                    std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor,
                    std::shared_ptr<Codec> codec, std::shared_ptr<Configuration> conf,
                    std::chrono::nanoseconds connect_timeout = std::chrono::nanoseconds(0));

  /** Default Destructor */
  virtual ~ConnectionFactory() = default;

  /**
   * Create a BootStrap from which a connection can be made.
   */
  virtual std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>> MakeBootstrap();

  /**
   * Connect a ClientBootstrap to a server and return the pipeline.
   *
   * This is mostly visible so that mocks can override socket connections.
   */
  virtual std::shared_ptr<HBaseService> Connect(
      std::shared_ptr<RpcConnection> rpc_connection,
      std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>> client_bootstrap,
      const std::string &hostname, uint16_t port);

  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor() { return io_executor_; }

  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor() { return cpu_executor_; }

 private:
  std::chrono::nanoseconds connect_timeout_;
  std::shared_ptr<Configuration> conf_;
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_executor_;
  std::shared_ptr<wangle::CPUThreadPoolExecutor> cpu_executor_;
  std::shared_ptr<RpcPipelineFactory> pipeline_factory_;
};
}  // namespace hbase
