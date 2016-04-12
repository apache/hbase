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

#include <folly/io/IOBuf.h>
#include <string>
#include <cstdint>

// Forward
namespace google {
namespace protobuf {
class Message;
}
}
namespace hbase {
class Request;
}

namespace hbase {
class ClientSerializer {
public:
  ClientSerializer();
  std::unique_ptr<folly::IOBuf> preamble();
  std::unique_ptr<folly::IOBuf> header(const std::string &user);
  std::unique_ptr<folly::IOBuf> request(const uint32_t call_id,
                                        const std::string &method,
                                        const google::protobuf::Message *msg);
  std::unique_ptr<folly::IOBuf>
  serialize_delimited(const google::protobuf::Message &msg);

  std::unique_ptr<folly::IOBuf>
  serialize_message(const google::protobuf::Message &msg);

  std::unique_ptr<folly::IOBuf>
  prepend_length(std::unique_ptr<folly::IOBuf> msg);

  uint8_t auth_type_;
};
} // namespace hbase
