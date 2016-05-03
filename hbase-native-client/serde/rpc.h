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

#include <memory>
#include <string>

// Forward
namespace folly {
class IOBuf;
}
namespace google {
namespace protobuf {
class Message;
}
}

namespace hbase {
class RpcSerde {
public:
  RpcSerde();
  virtual ~RpcSerde();
  int ParseDelimited(const folly::IOBuf *buf, google::protobuf::Message *msg);
  std::unique_ptr<folly::IOBuf> Preamble();
  std::unique_ptr<folly::IOBuf> Header(const std::string &user);
  std::unique_ptr<folly::IOBuf> Request(const uint32_t call_id,
                                        const std::string &method,
                                        const google::protobuf::Message *msg);
  std::unique_ptr<folly::IOBuf>
  SerializeDelimited(const google::protobuf::Message &msg);

  std::unique_ptr<folly::IOBuf>
  SerializeMessage(const google::protobuf::Message &msg);

  std::unique_ptr<folly::IOBuf>
  PrependLength(std::unique_ptr<folly::IOBuf> msg);

private:
  /* data */
  uint8_t auth_type_;
};
}
