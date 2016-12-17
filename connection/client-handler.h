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
#include <memory>
#include <mutex>
#include <string>
#include <utility>

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

/**
 * wangle::Handler implementation to convert hbase::Request to IOBuf and
 * convert IOBuf to hbase::Response.
 *
 * This class deals with sending the connection header and preamble
 * on first request.
 */
class ClientHandler : public wangle::Handler<std::unique_ptr<folly::IOBuf>,
                                             Response, std::unique_ptr<Request>,
                                             std::unique_ptr<folly::IOBuf>> {
 public:
  /**
   * Create the handler
   * @param user_name the user name of the user running this process.
   */
  explicit ClientHandler(std::string user_name);

  /**
   * Get bytes from the wire.
   * This should be the full message as the length field decoder should be
   * in the pipeline before this.
   */
  void read(Context *ctx, std::unique_ptr<folly::IOBuf> msg) override;

  /**
   * Write the data down the wire.
   */
  folly::Future<folly::Unit> write(Context *ctx,
                                   std::unique_ptr<Request> r) override;

 private:
  std::unique_ptr<std::once_flag> once_flag_;
  std::string user_name_;
  RpcSerde serde_;

  // in flight requests
  std::unique_ptr<folly::AtomicHashMap<
      uint32_t, std::shared_ptr<google::protobuf::Message>>>
      resp_msgs_;
};
}  // namespace hbase
