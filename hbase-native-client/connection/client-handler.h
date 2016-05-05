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

#include <folly/AtomicHashMap.h>
#include <wangle/channel/Handler.h>

#include <atomic>
#include <mutex>
#include <string>

#include "serde/rpc.h"

// Forward decs.
namespace hbase {
class Request;
class Response;
class HeaderInfo;
}
namespace google {
namespace protobuf {
class Message;
}
}

namespace hbase {

class ClientHandler : public wangle::Handler<std::unique_ptr<folly::IOBuf>,
                                             Response, std::unique_ptr<Request>,
                                             std::unique_ptr<folly::IOBuf>> {
public:
  ClientHandler(std::string user_name);
  void read(Context *ctx, std::unique_ptr<folly::IOBuf> msg) override;
  folly::Future<folly::Unit> write(Context *ctx,
                                   std::unique_ptr<Request> r) override;

private:
  std::unique_ptr<HeaderInfo> header_info_;
  std::string user_name_;
  RpcSerde serde_;

  // in flight requests
  std::unique_ptr<folly::AtomicHashMap<
      uint32_t, std::shared_ptr<google::protobuf::Message>>>
      resp_msgs_;
};

/**
 * Class to contain the info about if the connection header and preamble has
 * been sent.
 *
 * We use a serperate class here so that ClientHandler is relocatable.
 */
class HeaderInfo {
public:
  HeaderInfo() : need_(true), mutex_() {}
  std::atomic<bool> need_;
  std::mutex mutex_;
};
} // namespace hbase
