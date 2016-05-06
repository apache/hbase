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

#include <wangle/service/Service.h>

#include <string>

#include "connection/pipeline.h"
#include "connection/request.h"
#include "connection/response.h"
#include "connection/service.h"

namespace hbase {

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
  ConnectionFactory(std::shared_ptr<wangle::IOThreadPoolExecutor> io_pool);
  /** Default Desctructor */
  virtual ~ConnectionFactory() = default;

  /**
   * Create a BootStrap from which a connection can be made.
   */
  virtual std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>>
  MakeBootstrap();

  /**
   * Connect a ClientBootstrap to a server and return the pipeline.
   *
   * This is mostly visible so that mocks can override socket connections.
   */
  virtual std::shared_ptr<HBaseService>
  Connect(std::shared_ptr<wangle::ClientBootstrap<SerializePipeline>> client,
          const std::string &hostname, int port);

private:
  std::shared_ptr<wangle::IOThreadPoolExecutor> io_pool_;
  std::shared_ptr<RpcPipelineFactory> pipeline_factory_;
};
} // namespace hbase
