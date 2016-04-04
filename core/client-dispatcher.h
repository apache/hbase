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

#include <wangle/service/ClientDispatcher.h>

#include "core/pipeline.h"
#include "core/request.h"
#include "core/response.h"

namespace hbase {
class ClientDispatcher
    : public wangle::ClientDispatcherBase<SerializePipeline, Request,
                                          Response> {
public:
  void read(Context *ctx, Response in) override;
  folly::Future<Response> operator()(Request arg) override;
  folly::Future<folly::Unit> close(Context *ctx) override;
  folly::Future<folly::Unit> close() override;

private:
  std::unordered_map<int32_t, folly::Promise<Response>> requests_;
  uint32_t current_call_id_ = 1;
};
}  // namespace hbase
