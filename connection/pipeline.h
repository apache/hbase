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

#include <folly/io/IOBufQueue.h>
#include <wangle/service/Service.h>

#include "connection/request.h"
#include "connection/response.h"
#include "utils/user-util.h"

namespace hbase {

/** Pipeline to turn IOBuf into requests */
using SerializePipeline =
    wangle::Pipeline<folly::IOBufQueue &, std::unique_ptr<Request>>;

/**
 * Factory to create new pipelines for HBase RPC's.
 */
class RpcPipelineFactory : public wangle::PipelineFactory<SerializePipeline> {
public:
  /**
   * Constructor. This will create user util.
   */
  RpcPipelineFactory();

  /**
   * Create a new pipeline.
   * The pipeline will be:
   *
   * - Async Socke Handler
   * - Event Base Handler
   * - Length Field Based Frame Decoder
   * - Client Handler
   */
  SerializePipeline::Ptr
  newPipeline(std::shared_ptr<folly::AsyncTransportWrapper> sock) override;

private:
  UserUtil user_util_;
};
} // namespace hbase
