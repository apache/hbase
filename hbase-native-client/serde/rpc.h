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

/**
 * @brief Class for serializing a deserializing rpc formatted data.
 *
 * RpcSerde is the one stop shop for reading/writing data to HBase daemons.
 * It should throw exceptions if anything goes wrong.
 */
class RpcSerde {
 public:
  /**
   * Constructor assumes the default auth type.
   */
  RpcSerde();

  /**
   * Destructor. This is provided just for testing purposes.
   */
  virtual ~RpcSerde() = default;

  /**
   * Pase a message in the delimited format.
   *
   * A message in delimited format consists of the following:
   *
   * - a protobuf var int32.
   * - A protobuf object serialized.
   */
  int ParseDelimited(const folly::IOBuf *buf, google::protobuf::Message *msg);

  /**
   * Create a new connection preamble in a new IOBuf.
   */
  std::unique_ptr<folly::IOBuf> Preamble();

  /**
   * Create the header protobuf object and serialize it to a new IOBuf.
   * Header is in the following format:
   *
   * - Big endian length
   * - ConnectionHeader object serialized out.
   */
  std::unique_ptr<folly::IOBuf> Header(const std::string &user);

  /**
   * Serialize a request message into a protobuf.
   * Request consists of:
   *
   * - Big endian length
   * - RequestHeader object
   * - The passed in Message object
   */
  std::unique_ptr<folly::IOBuf> Request(const uint32_t call_id, const std::string &method,
                                        const google::protobuf::Message *msg);

  /**
   * Serialize a message in the delimited format.
   * Delimited format consists of the following:
   *
   * - A protobuf var int32
   * - The message object seriailized after that.
   */
  std::unique_ptr<folly::IOBuf> SerializeDelimited(const google::protobuf::Message &msg);

  /**
   * Serilalize a message. This does not add any length prepend.
   */
  std::unique_ptr<folly::IOBuf> SerializeMessage(const google::protobuf::Message &msg);

  /**
   * Prepend a length IOBuf to the given IOBuf chain.
   * This involves no copies or moves of the passed in data.
   */
  std::unique_ptr<folly::IOBuf> PrependLength(std::unique_ptr<folly::IOBuf> msg);

 private:
  /* data */
  uint8_t auth_type_;
};
}  // namespace hbase
