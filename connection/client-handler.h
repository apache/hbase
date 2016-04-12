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

#include <wangle/channel/Handler.h>

#include <string>

#include "serde/client-serializer.h"
#include "serde/client-deserializer.h"

// Forward decs.
namespace hbase {
class Request;
class Response;
}

namespace hbase {
class ClientHandler
    : public wangle::Handler<std::unique_ptr<folly::IOBuf>, Response, Request,
                             std::unique_ptr<folly::IOBuf>> {
public:
  ClientHandler(std::string user_name);
  void read(Context *ctx, std::unique_ptr<folly::IOBuf> msg) override;
  folly::Future<folly::Unit> write(Context *ctx, Request r) override;

private:
  bool need_send_header_ = true;
  std::string user_name_;
  ClientSerializer ser_;
  ClientDeserializer deser_;
};
} // namespace hbase
