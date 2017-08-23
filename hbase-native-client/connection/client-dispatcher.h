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

#include <folly/Logging.h>
#include <wangle/service/ClientDispatcher.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "connection/pipeline.h"
#include "connection/request.h"
#include "connection/response.h"
#include "utils/concurrent-map.h"

namespace hbase {

/**
 * Dispatcher that assigns a call_id and then routes the response back to the
 * future.
 */
class ClientDispatcher
    : public wangle::ClientDispatcherBase<SerializePipeline, std::unique_ptr<Request>,
                                          std::unique_ptr<Response>> {
 public:
  /** Create a new ClientDispatcher */
  explicit ClientDispatcher(const std::string &server);
  /** Read a response off the pipeline. */
  void read(Context *ctx, std::unique_ptr<Response> in) override;
  void readException(Context *ctx, folly::exception_wrapper e) override;
  void readEOF(Context *ctx) override;
  /** Take a request as a call and send it down the pipeline. */
  folly::Future<std::unique_ptr<Response>> operator()(std::unique_ptr<Request> arg) override;
  /** Close the dispatcher and the associated pipeline. */
  folly::Future<folly::Unit> close(Context *ctx) override;
  /** Close the dispatcher and the associated pipeline. */
  folly::Future<folly::Unit> close() override;

 private:
  void CloseAndCleanUpCalls();

 private:
  std::recursive_mutex mutex_;
  concurrent_map<uint32_t, folly::Promise<std::unique_ptr<Response>>> requests_;
  // Start at some number way above what could
  // be there for un-initialized call id counters.
  //
  // This makes it easier to make sure that the're are
  // no access to un-initialized variables.
  //
  // uint32_t has a max of 4Billion so 10 more or less is
  // not a big deal.
  std::atomic<uint32_t> current_call_id_;
  std::string server_;
  bool is_closed_;
};
}  // namespace hbase
