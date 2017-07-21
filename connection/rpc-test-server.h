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
#include <wangle/service/ExecutorFilter.h>
#include <wangle/service/Service.h>

#include "connection/request.h"
#include "connection/response.h"

using namespace hbase;
using namespace folly;
using namespace wangle;

namespace hbase {
using RpcTestServerSerializePipeline = wangle::Pipeline<IOBufQueue&, std::unique_ptr<Response>>;

class RpcTestService : public Service<std::unique_ptr<Request>, std::unique_ptr<Response>> {
 public:
  RpcTestService() {}
  virtual ~RpcTestService() = default;
  Future<std::unique_ptr<Response>> operator()(std::unique_ptr<Request> request) override;
};

class RpcTestServerPipelineFactory : public PipelineFactory<RpcTestServerSerializePipeline> {
 public:
  RpcTestServerSerializePipeline::Ptr newPipeline(
      std::shared_ptr<AsyncTransportWrapper> sock) override;

 private:
  ExecutorFilter<std::unique_ptr<Request>, std::unique_ptr<Response>> service_{
      std::make_shared<CPUThreadPoolExecutor>(1), std::make_shared<RpcTestService>()};
};
}  // end of namespace hbase
