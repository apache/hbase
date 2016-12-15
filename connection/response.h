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

#include <cstdint>

// Forward
namespace google {
namespace protobuf {
class Message;
}
}

namespace hbase {

/**
 * @brief Class representing a rpc response
 *
 * This is the class sent to a service.
 */
class Response {
 public:
  /**
   * Constructor.
   * Initinalizes the call id to 0. 0 should never be a valid call id.
   */
  Response() : call_id_(0), resp_msg_(nullptr) {}

  /** Get the call_id */
  uint32_t call_id() { return call_id_; }

  /** Set the call_id */
  void set_call_id(uint32_t call_id) { call_id_ = call_id; }

  /**
   * Get the response message.
   * The caller is reponsible for knowing the type. In practice the call id is
   * used to figure out the type.
   */
  std::shared_ptr<google::protobuf::Message> resp_msg() const {
    return resp_msg_;
  }

  /** Set the response message. */
  void set_resp_msg(std::shared_ptr<google::protobuf::Message> response) {
    resp_msg_ = std::move(response);
  }

 private:
  uint32_t call_id_;
  std::shared_ptr<google::protobuf::Message> resp_msg_;
};
}  // namespace hbase
