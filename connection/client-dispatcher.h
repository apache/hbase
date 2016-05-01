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

#include "connection/pipeline.h"
#include "connection/request.h"
#include "connection/response.h"

namespace hbase {
class ClientDispatcher
    : public wangle::ClientDispatcherBase<SerializePipeline,
                                          std::unique_ptr<Request>, Response> {
public:
  ClientDispatcher();
  void read(Context *ctx, Response in) override;
  folly::Future<Response> operator()(std::unique_ptr<Request> arg) override;
  folly::Future<folly::Unit> close(Context *ctx) override;
  folly::Future<folly::Unit> close() override;

private:
  std::unordered_map<uint32_t, folly::Promise<Response>> requests_;
  // Start at some number way above what could
  // be there for un-initialized call id counters.
  //
  // This makes it easier to make sure that the're are
  // no access to un-initialized variables.
  //
  // uint32_t has a max of 4Billion so 10 more or less is
  // not a big deal.
  uint32_t current_call_id_;
};
} // namespace hbase
